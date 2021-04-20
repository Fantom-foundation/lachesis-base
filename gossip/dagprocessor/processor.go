package dagprocessor

import (
	"errors"
	"runtime"
	"sync"

	"github.com/Fantom-foundation/lachesis-base/eventcheck"
	"github.com/Fantom-foundation/lachesis-base/eventcheck/queuedcheck"
	"github.com/Fantom-foundation/lachesis-base/gossip/dagordering"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/utils/datasemaphore"
	"github.com/Fantom-foundation/lachesis-base/utils/workers"
)

var (
	ErrBusy = errors.New("failed to acquire events semaphore")
)

// Processor is responsible for processing incoming events
type Processor struct {
	cfg Config

	quit chan struct{}
	wg   sync.WaitGroup

	callback Callback

	orderedInserter    *workers.Workers
	unorderedInserters *workers.Workers

	buffer *dagordering.EventsBuffer

	eventsSemaphore *datasemaphore.DataSemaphore
}

type EventCallback struct {
	Process         func(e dag.Event) error
	Released        func(e dag.Event, peer string, err error)
	Get             func(hash.Event) dag.Event
	Exists          func(hash.Event) bool
	OnlyInterested  func(ids hash.Events) hash.Events
	CheckParents    func(e dag.Event, parents dag.Events) error
	CheckParentless func(tasks []queuedcheck.EventTask, checked func(res []queuedcheck.EventTask))
}

type Callback struct {
	Event EventCallback
	// PeerMisbehaviour is a callback type for dropping a peer detected as malicious.
	PeerMisbehaviour func(peer string, err error) bool
	HighestLamport   func() idx.Lamport
}

// New creates an event processor
func New(eventsSemaphore *datasemaphore.DataSemaphore, cfg Config, callback Callback) *Processor {
	if cfg.MaxUnorderedInsertions == 0 {
		cfg.MaxUnorderedInsertions = runtime.NumCPU()
	}

	f := &Processor{
		cfg:             cfg,
		quit:            make(chan struct{}),
		eventsSemaphore: eventsSemaphore,
	}
	released := callback.Event.Released
	callback.Event.Released = func(e dag.Event, peer string, err error) {
		f.eventsSemaphore.Release(dag.Metric{1, uint64(e.Size())})
		if released != nil {
			released(e, peer, err)
		}
	}
	f.callback = callback
	f.buffer = dagordering.New(cfg.EventsBufferLimit, dagordering.Callback{
		Process:  callback.Event.Process,
		Released: callback.Event.Released,
		Get:      callback.Event.Get,
		Exists:   callback.Event.Exists,
		Check:    callback.Event.CheckParents,
	})
	f.orderedInserter = workers.New(&f.wg, f.quit, cfg.MaxTasks())
	f.unorderedInserters = workers.New(&f.wg, f.quit, cfg.MaxTasks())
	return f
}

// Start boots up the events processor.
func (f *Processor) Start() {
	f.orderedInserter.Start(1)
	f.unorderedInserters.Start(f.cfg.MaxUnorderedInsertions)
}

// Stop interrupts the processor, canceling all the pending operations.
// Stop waits until all the internal goroutines have finished.
func (f *Processor) Stop() {
	close(f.quit)
	f.eventsSemaphore.Terminate()
	f.wg.Wait()
	f.buffer.Clear()
}

// Overloaded returns true if too much events are being processed or requested
func (f *Processor) Overloaded() bool {
	return f.unorderedInserters.TasksCount() > f.cfg.MaxTasks()*3/4 ||
		f.orderedInserter.TasksCount() > f.cfg.MaxTasks()*3/4
}

type indexedTask struct {
	queuedcheck.EventTask
	pos idx.Event
}

func (f *Processor) Enqueue(peer string, events dag.Events, ordered bool, notifyAnnounces func(hash.Events), done func()) error {
	eventTasks := make([]queuedcheck.EventTask, 0, len(events))
	for i, e := range events {
		eventTasks = append(eventTasks, &indexedTask{
			EventTask: queuedcheck.NewTask(e),
			pos:       idx.Event(i),
		})
	}

	if !f.eventsSemaphore.Acquire(events.Metric(), f.cfg.EventsSemaphoreTimeout) {
		return ErrBusy
	}

	inserter := f.unorderedInserters
	if ordered {
		inserter = f.orderedInserter
	}

	return inserter.Enqueue(func() {
		if done != nil {
			defer done()
		}
		checkedC := make(chan *indexedTask, len(eventTasks))
		f.callback.Event.CheckParentless(eventTasks, func(checked []queuedcheck.EventTask) {
			for _, e := range checked {
				checkedC <- e.(*indexedTask)
			}
		})

		var orderedResults []*indexedTask
		if ordered {
			orderedResults = make([]*indexedTask, len(eventTasks))
		}
		var processed int
		var toRequest hash.Events
		for processed < len(eventTasks) {
			select {
			case res := <-checkedC:
				if ordered {
					orderedResults[res.pos] = res

					for i := processed; processed < len(orderedResults) && orderedResults[i] != nil; i++ {
						toRequest = append(toRequest, f.process(peer, orderedResults[i].Event(), orderedResults[i].Result())...)
						processed++
					}
				} else {
					toRequest = append(toRequest, f.process(peer, res.Event(), res.Result())...)
					processed++
				}

			case <-f.quit:
				return
			}
		}

		// request unknown event parents
		if notifyAnnounces != nil && len(toRequest) != 0 {
			notifyAnnounces(toRequest)
		}
	})
}

func (f *Processor) process(peer string, event dag.Event, resErr error) (toRequest hash.Events) {
	// release event if failed validation
	if resErr != nil {
		f.callback.PeerMisbehaviour(peer, resErr)
		f.callback.Event.Released(event, peer, resErr)
		return hash.Events{}
	}
	// release event if it's too far in future
	highestLamport := f.callback.HighestLamport()
	maxLamportDiff := 1 + idx.Lamport(f.cfg.EventsBufferLimit.Num)
	if event.Lamport() > highestLamport+maxLamportDiff {
		f.callback.Event.Released(event, peer, eventcheck.ErrSpilledEvent)
		return hash.Events{}
	}
	// push event to the ordering buffer
	complete := f.buffer.PushEvent(event, peer)
	if !complete && event.Lamport() <= highestLamport+maxLamportDiff/10 {
		return event.Parents()
	}
	return hash.Events{}
}

func (f *Processor) IsBuffered(id hash.Event) bool {
	return f.buffer.IsBuffered(id)
}

// Clear
func (f *Processor) Clear() {
	f.buffer.Clear()
}

func (f *Processor) TotalBuffered() dag.Metric {
	return f.buffer.Total()
}

func (f *Processor) TasksCount() int {
	return f.orderedInserter.TasksCount() + f.unorderedInserters.TasksCount()
}
