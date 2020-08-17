package abft

import (
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	lru "github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/dag/tdag"
	"github.com/Fantom-foundation/go-lachesis/kvdb"
)

// EventStore is a abft event storage for test purpose.
// It implements EventSource interface.
type EventStore struct {
	db map[hash.Event]dag.Event
}

// NewEventStore creates store over memory map.
func NewEventStore() *EventStore {
	return &EventStore{
		db: map[hash.Event]dag.Event{},
	}
}

// Close leaves underlying database.
func (s *EventStore) Close() {
	s.db = nil
}

// SetEvent stores event.
func (s *EventStore) SetEvent(e dag.Event) {
	s.db[e.ID()] = e
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

/*
 * Tests:
 */

func TestEventStore(t *testing.T) {
	store := NewEventStore()

	t.Run("NotExisting", func(t *testing.T) {
		assertar := assert.New(t)

		h := hash.FakeEvent()
		e1 := store.GetEvent(h)
		assertar.Nil(e1)
	})

	t.Run("Events", func(t *testing.T) {
		assertar := assert.New(t)

		nodes := tdag.GenNodes(5)
		tdag.ForEachRandEvent(nodes, int(TestMaxEpochBlocks)-1, 4, nil, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				store.SetEvent(e)
				e1 := store.GetEvent(e.ID())

				if !assertar.Equal(e, e1) {
					t.Fatal(e.String() + " != " + e1.String())
				}
			},
		})
	})

	store.Close()
}

/*
 * Utils:
 */

func (s *EventStore) set(table kvdb.Store, key []byte, val interface{}) {
	buf, err := rlp.EncodeToBytes(val)
	if err != nil {
		panic(err)
	}

	if err := table.Put(key, buf); err != nil {
		panic(err)
	}
}

func (s *EventStore) get(table kvdb.Store, key []byte, to interface{}) interface{} {
	buf, err := table.Get(key)
	if err != nil {
		panic(err)
	}
	if buf == nil {
		return nil
	}

	err = rlp.DecodeBytes(buf, to)
	if err != nil {
		panic(err)
	}
	return to
}

func (s *EventStore) has(table kvdb.Store, key []byte) bool {
	res, err := table.Has(key)
	if err != nil {
		panic(err)
	}
	return res
}

func (s *EventStore) makeCache(size int) *lru.Cache {
	if size <= 0 {
		return nil
	}

	cache, err := lru.New(size)
	if err != nil {
		panic(err)
		return nil
	}
	return cache
}
