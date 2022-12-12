package abft

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

// EventStore is a abft event storage for test purpose.
// It implements EventSource interface.
type EventStore struct {
	db    map[hash.Event]dag.Event
	index map[idx.Epoch][]hash.Event
}

// NewEventStore creates store over memory map.
func NewEventStore() *EventStore {
	return &EventStore{
		db:    map[hash.Event]dag.Event{},
		index: map[idx.Epoch][]hash.Event{},
	}
}

// Close leaves underlying database.
func (s *EventStore) Close() {
	s.db = nil
}

// SetEvent stores event.
func (s *EventStore) SetEvent(e dag.Event) {
	s.db[e.ID()] = e
	epochEvents, ok := s.index[e.Epoch()]
	if !ok {
		epochEvents = []hash.Event{}
	}
	epochEvents = append(epochEvents, e.ID())
	s.index[e.Epoch()] = epochEvents
}

// GetEvent returns stored event.
func (s *EventStore) GetEvent(h hash.Event) dag.Event {
	return s.db[h]
}

// HasEvent returns true if event exists.
func (s *EventStore) HasEvent(h hash.Event) bool {
	_, ok := s.db[h]
	return ok
}

func (s *EventStore) ForEachEpochEvent(epoch idx.Epoch, onEvent func(event dag.Event) bool) {
	epochEvents, ok := s.index[epoch]
	if !ok {
		return
	}
	for _, h := range epochEvents {
		onEvent(s.db[h])
	}
}

/*
 * Tests:
 */

func TestEventStore(t *testing.T) {

	t.Run("NotExisting", func(t *testing.T) {
		store := NewEventStore()
		defer store.Close()

		assertar := assert.New(t)

		h := hash.FakeEvent()
		e1 := store.GetEvent(h)
		assertar.Nil(e1)
	})

	t.Run("Events", func(t *testing.T) {
		store := NewEventStore()
		defer store.Close()

		assertar := assert.New(t)

		nodes := tdag.GenNodes(5)
		tdag.ForEachRandEvent(nodes, int(TestMaxEpochEvents)-1, 4, nil, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				store.SetEvent(e)
				e1 := store.GetEvent(e.ID())

				if !assertar.Equal(e, e1) {
					t.Fatal(e.String() + " != " + e1.String())
				}
			},
		})
	})

	t.Run("Iterator", func(t *testing.T) {
		store := NewEventStore()
		defer store.Close()

		assertar := assert.New(t)

		nodes := tdag.GenNodes(5)
		ordered := []hash.Event{}
		tdag.ForEachRandEvent(nodes, int(TestMaxEpochEvents)-1, 4, nil, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				store.SetEvent(e)
				ordered = append(ordered, e.ID())
			},
		})

		i := 0
		store.ForEachEpochEvent(idx.Epoch(0), func(e dag.Event) bool {
			assertar.Equal(e.ID(), ordered[i])
			i++
			return true
		})

	})

}
