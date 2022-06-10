package synced

import (
	"sync"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// store wrapper around any kvdb.Store.
type store struct {
	iteratedReader
	underlying kvdb.Store
}

// WrapStore underlying db to make its methods synced with mu.
func WrapStore(parent kvdb.Store, mu *sync.RWMutex) kvdb.Store {
	s := &store{
		iteratedReader: iteratedReader{
			mu:         mu,
			underlying: parent,
		},
		underlying: parent,
	}

	return s
}

// Put puts key-value pair into the cache.
func (s *store) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.underlying.Put(key, value)
}

// Delete removes key-value pair by key.
func (s *store) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.underlying.Delete(key)
}

// Compact flattens the underlying data store for the given key range.
func (s *store) Compact(start []byte, limit []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.underlying.Compact(start, limit)
}

// Close leaves underlying database.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.underlying.Close()
}

// Drop drops database.
func (s *store) Drop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.underlying.Drop()
}

// NewBatch creates new batch.
func (s *store) NewBatch() kvdb.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &syncedBatch{
		mu:         s.mu,
		underlying: s.underlying.NewBatch(),
	}

}

/*
 * Batch
 */

// syncedBatch wraps a batch.
type syncedBatch struct {
	mu         *sync.RWMutex
	underlying kvdb.Batch
}

// Put adds "add key-value pair" operation into batch.
func (b *syncedBatch) Put(key, value []byte) error {
	return b.underlying.Put(key, value)
}

// Delete adds "remove key" operation into batch.
func (b *syncedBatch) Delete(key []byte) error {
	return b.underlying.Delete(key)
}

// Write writes batch into db. Not atomic.
func (b *syncedBatch) Write() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.underlying.Write()
}

// ValueSize returns key-values sizes sum.
func (b *syncedBatch) ValueSize() int {
	return b.underlying.ValueSize()
}

// Reset cleans whole batch.
func (b *syncedBatch) Reset() {
	b.underlying.Reset()
}

// Replay replays the batch contents.
func (b *syncedBatch) Replay(w kvdb.Writer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.underlying.Replay(w)
}
