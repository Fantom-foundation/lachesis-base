package dagprocessor

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
	"github.com/Fantom-foundation/lachesis-base/utils/datasemaphore"
)

func TestProcessor(t *testing.T) {
	for try := 0; try < 500; try++ {
		testProcessor(t)
	}
}

var maxGroupSize = dag.Metric{
	Num:  50,
	Size: 50 * 50,
}

func shuffleEventsIntoChunks(inEvents dag.Events) []dag.Events {
	if len(inEvents) == 0 {
		return nil
	}
	var chunks []dag.Events
	var lastChunk dag.Events
	var lastChunkSize dag.Metric
	for _, rnd := range rand.Perm(len(inEvents)) {
		e := inEvents[rnd]
		if rand.Intn(10) == 0 || lastChunkSize.Num+1 >= maxGroupSize.Num || lastChunkSize.Size+uint64(e.Size()) >= maxGroupSize.Size {
			chunks = append(chunks, lastChunk)
			lastChunk = dag.Events{}
		}
		lastChunk = append(lastChunk, e)
		lastChunkSize.Num++
		lastChunkSize.Size += uint64(e.Size())
	}
	chunks = append(chunks, lastChunk)
	return chunks
}

func testProcessor(t *testing.T) {
	nodes := tdag.GenNodes(5)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 10, 3, nil, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	limit := dag.Metric{
		Num:  idx.Event(len(ordered)),
		Size: uint64(ordered.Metric().Size),
	}
	semaphore := datasemaphore.New(limit, func(received dag.Metric, processing dag.Metric, releasing dag.Metric) {
		t.Fatal("events semaphore inconsistency")
	})
	config := DefaultConfig(cachescale.Identity)
	config.EventsBufferLimit = limit

	checked := 0

	highestLamport := idx.Lamport(0)
	processed := make(map[hash.Event]dag.Event)
	mu := sync.RWMutex{}
	processor := New(semaphore, config, Callback{
		Event: EventCallback{
			Process: func(e dag.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if _, ok := processed[e.ID()]; ok {
					t.Fatalf("%s already processed", e.String())
					return nil
				}
				for _, p := range e.Parents() {
					if _, ok := processed[p]; !ok {
						t.Fatalf("got %s before parent %s", e.String(), p.String())
						return nil
					}
				}
				if highestLamport < e.Lamport() {
					highestLamport = e.Lamport()
				}
				processed[e.ID()] = e
				return nil
			},

			Released: func(e dag.Event, peer string, err error) {
				if err != nil {
					t.Fatalf("%s unexpectedly dropped with '%s'", e.String(), err)
				}
			},

			Exists: func(e hash.Event) bool {
				mu.RLock()
				defer mu.RUnlock()
				return processed[e] != nil
			},

			Get: func(id hash.Event) dag.Event {
				mu.RLock()
				defer mu.RUnlock()
				return processed[id]
			},

			CheckParents: func(e dag.Event, parents dag.Events) error {
				mu.RLock()
				defer mu.RUnlock()
				checked++
				if e.Frame() != idx.Frame(e.Seq()) {
					return errors.New("malformed event frame")
				}
				return nil
			},
			CheckParentless: func(e dag.Event, checked func(err error)) {
				checked(nil)
			},
		},
		HighestLamport: func() idx.Lamport {
			return highestLamport
		},
	})
	// shuffle events
	chunks := shuffleEventsIntoChunks(ordered)

	// process events
	processor.Start()
	wg := sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Add(1)
		err := processor.Enqueue("", chunk, rand.Intn(2) == 0, func(events hash.Events) {}, func() {
			wg.Done()
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	processor.Stop()

	// everything is processed
	for _, e := range ordered {
		if _, ok := processed[e.ID()]; !ok {
			t.Fatal("event wasn't processed")
		}
	}
	if checked != len(processed) {
		t.Fatal("not all the events were checked")
	}
}

func TestProcessorReleasing(t *testing.T) {
	for try := int64(0); try < 100; try++ {
		testProcessorReleasing(t, 200, try)
	}
}

func testProcessorReleasing(t *testing.T, maxEvents int, try int64) {
	nodes := tdag.GenNodes(5)

	var ordered dag.Events
	_ = tdag.ForEachRandEvent(nodes, 10, 3, rand.New(rand.NewSource(try)), tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetFrame(idx.Frame(e.Seq()))
			return nil
		},
	})

	limit := dag.Metric{
		Num:  idx.Event(rand.Intn(maxEvents)),
		Size: uint64(rand.Intn(maxEvents * 100)),
	}
	limitPlus1group := dag.Metric{
		Num:  limit.Num + maxGroupSize.Num,
		Size: limit.Size + maxGroupSize.Size,
	}
	semaphore := datasemaphore.New(limitPlus1group, func(received dag.Metric, processing dag.Metric, releasing dag.Metric) {
		t.Fatal("events semaphore inconsistency")
	})
	config := DefaultConfig(cachescale.Identity)
	config.EventsBufferLimit = limit

	released := uint32(0)

	highestLamport := idx.Lamport(0)
	processed := make(map[hash.Event]dag.Event)
	mu := sync.RWMutex{}
	processor := New(semaphore, config, Callback{
		Event: EventCallback{
			Process: func(e dag.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if _, ok := processed[e.ID()]; ok {
					t.Fatalf("%s already processed", e.String())
					return nil
				}
				for _, p := range e.Parents() {
					if _, ok := processed[p]; !ok {
						t.Fatalf("got %s before parent %s", e.String(), p.String())
						return nil
					}
				}
				if rand.Intn(10) == 0 {
					return errors.New("testing error")
				}
				if rand.Intn(10) == 0 {
					time.Sleep(time.Microsecond * 100)
				}
				if highestLamport < e.Lamport() {
					highestLamport = e.Lamport()
				}
				processed[e.ID()] = e
				return nil
			},

			Released: func(e dag.Event, peer string, err error) {
				mu.Lock()
				defer mu.Unlock()
				atomic.AddUint32(&released, 1)
			},

			Exists: func(e hash.Event) bool {
				mu.RLock()
				defer mu.RUnlock()
				return processed[e] != nil
			},

			Get: func(id hash.Event) dag.Event {
				mu.RLock()
				defer mu.RUnlock()
				return processed[id]
			},

			CheckParents: func(e dag.Event, parents dag.Events) error {
				if rand.Intn(10) == 0 {
					return errors.New("testing error")
				}
				if rand.Intn(10) == 0 {
					time.Sleep(time.Microsecond * 100)
				}
				return nil
			},
			CheckParentless: func(e dag.Event, checked func(err error)) {
				var err error
				if rand.Intn(10) == 0 {
					err = errors.New("testing error")
				}
				if rand.Intn(10) == 0 {
					time.Sleep(time.Microsecond * 100)
				}
				checked(err)
			},
		},
		HighestLamport: func() idx.Lamport {
			return highestLamport
		},
	})
	// duplicate some events
	ordered = append(ordered, ordered[:rand.Intn(len(ordered))]...)
	// shuffle events
	chunks := shuffleEventsIntoChunks(ordered)

	// process events
	processor.Start()
	wg := sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Add(1)
		err := processor.Enqueue("", chunk, rand.Intn(2) == 0, func(events hash.Events) {}, func() {
			wg.Done()
		})
		if err != nil {
			t.Fatal(err)
		}
		if rand.Intn(10) == 0 {
			processor.Clear()
		}
	}
	wg.Wait()
	processor.Clear()
	if processor.eventsSemaphore.Processing().Num != 0 {
		t.Fatal("not all the events were released", processor.eventsSemaphore.Processing().Num)
	}
	processor.Stop()

	// everything is released
	if uint32(len(ordered)) != released {
		t.Fatal("not all the events were released", len(ordered), released)
	}
}
