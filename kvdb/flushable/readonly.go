package flushable

import (
	"sync"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// Readonly kvdb.Store wrapper around any Database.
type Readonly struct {
	mu         *sync.RWMutex
	underlying kvdb.Store
}

// Wrap underlying db.
// Allows the readings only.
func WrapWithReadonly(parent kvdb.Store, mu *sync.RWMutex) *Readonly {
	ro := &Readonly{
		mu:         mu,
		underlying: parent,
	}

	return ro
}

// Has checks if key is in the exists.
func (ro *Readonly) Has(key []byte) (bool, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Has(key)
}

// Get returns key-value pair by key.
func (ro *Readonly) Get(key []byte) ([]byte, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Get(key)
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (ro *Readonly) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	ro.mu.RLock()
	defer ro.mu.RUnlock()
	// NOTE: iterator's methods are not locked with ro.mu
	return ro.underlying.NewIterator(prefix, start)
}

// Stat returns a particular internal stat of the database.
func (ro *Readonly) Stat(property string) (string, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Stat(property)
}

// Compact flattens the underlying data store for the given key range.
func (ro *Readonly) Compact(start []byte, limit []byte) error {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Compact(start, limit)
}

// Put puts key-value pair into the cache.
func (ro *Readonly) Put(key []byte, value []byte) error {
	panic("is not allowed, readonly")
	return ro.underlying.Put(key, value)
}

// NewBatch creates new batch.
func (ro *Readonly) NewBatch() kvdb.Batch {
	panic("is not allowed, readonly")
	return ro.underlying.NewBatch()
}

// Delete removes key-value pair by key.
func (ro *Readonly) Delete(key []byte) error {
	panic("is not allowed, readonly")
	return ro.underlying.Delete(key)
}

// Close leaves underlying database.
func (ro *Readonly) Close() error {
	panic("is not allowed, readonly")
	return ro.underlying.Close()
}
