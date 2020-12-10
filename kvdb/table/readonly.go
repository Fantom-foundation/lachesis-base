package table

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// readonly kvdb.Store panics on any data mutation.
type readonly struct {
	kvdb.ReadonlyStore
}

// Put puts key-value pair into the cache.
func (ro *readonly) Put(key []byte, value []byte) error {
	panic("readonly!")
	return nil
}

// Delete removes key-value pair by key.
func (ro *readonly) Delete(key []byte) error {
	panic("readonly!")
	return nil
}

// Compact flattens the underlying data store for the given key range.
func (ro *readonly) Compact(start []byte, limit []byte) error {
	panic("readonly!")
	return nil
}

// Close leaves underlying database.
func (ro *readonly) Close() error {
	panic("readonly!")
	return nil
}

// NewBatch creates new batch.
func (ro *readonly) NewBatch() kvdb.Batch {
	panic("readonly!")
	return nil
}
