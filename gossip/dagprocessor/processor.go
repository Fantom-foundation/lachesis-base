package dagprocessor

import (
	"errors"
	"runtime"
	"sync"

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
	CheckParentless func(inEvents dag.Events, checked func(ee dag.Events, errs []error))
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
	f.Clear()
	f.wg.Wait()
	f.buffer.Clear()
}

// Overloaded returns true if too much events are being processed or requested
func (f *Processor) Overloaded() bool {
	return f.unorderedInserters.TasksCount() > f.cfg.MaxTasks()*3/4 ||
		f.orderedInserter.TasksCount() > f.cfg.MaxTasks()*3/4
}

type eventErrPair struct {
	event dag.Event
	err   error
}

func (f *Processor) Enqueue(peer string, events dag.Events, ordered bool, notifyAnnounces func(hash.Events), done func()) error {
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
		checkedC := make(chan eventErrPair, len(events))
		f.callback.Event.CheckParentless(events, func(checked dag.Events, errs []error) {
			for i, e := range checked {
				checkedC <- eventErrPair{e, errs[i]}
			}
		})

		var orderedResults []eventErrPair
		var eventPos map[hash.Event]int
		if ordered {
			orderedResults = make([]eventErrPair, len(events))
			eventPos = make(map[hash.Event]int, len(events))
			for i, e := range events {
				eventPos[e.ID()] = i
			}
		}
		var processed int
		var toRequest hash.Events
		for processed < len(events) {
			select {
			case res := <-checkedC:
				if ordered {
					orderedResults[eventPos[res.event.ID()]] = res

					for i := processed; processed < len(orderedResults) && orderedResults[i].event != nil; i++ {
						toRequest = append(toRequest, f.process(peer, orderedResults[i])...)
						processed++
					}
				} else {
					toRequest = append(toRequest, f.process(peer, res)...)
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

func (f *Processor) process(peer string, res eventErrPair) (toRequest hash.Events) {
	// release event if failed validation
	if res.err != nil {
		f.callback.PeerMisbehaviour(peer, res.err)
		f.callback.Event.Released(res.event, peer, res.err)
		return hash.Events{}
	}
	// release event if it's too far in future
	highestLamport := f.callback.HighestLamport()
	maxLamportDiff := 1 + idx.Lamport(f.cfg.EventsBufferLimit.Num)
	if res.event.Lamport() > highestLamport+maxLamportDiff {
		f.callback.Event.Released(res.event, peer, res.err)
		return hash.Events{}
	}
	// push event to the ordering buffer
	complete := f.buffer.PushEvent(res.event, peer)
	if !complete && res.event.Lamport() <= highestLamport+maxLamportDiff/10 {
		return res.event.Parents()
	}
	return hash.Events{}
}

func (f *Processor) IsBuffered(id hash.Event) bool {
	return f.buffer.IsBuffered(id)
}

// Clear
func (f *Processor) Clear() {
	f.buffer.Clear()
	f.unorderedInserters.Drain()
	f.orderedInserter.Drain()
}

func (f *Processor) TotalBuffered() dag.Metric {
	return f.buffer.Total()
}

func (f *Processor) TasksCount() int {
	return f.orderedInserter.TasksCount() + f.unorderedInserters.TasksCount()
}
