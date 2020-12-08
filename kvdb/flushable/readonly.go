package flushable

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// Readonly kvdb.Store wrapper around any Database.
type Readonly struct {
	underlying kvdb.Store
}

// Wrap underlying db.
// Allows the readings only.
func WrapWithReadonly(parent kvdb.Store) *Readonly {
	ro := &Readonly{
		underlying: parent,
	}

	return ro
}

// Has checks if key is in the exists.
func (ro *Readonly) Has(key []byte) (bool, error) {
	return ro.underlying.Has(key)
}

// Get returns key-value pair by key.
func (ro *Readonly) Get(key []byte) ([]byte, error) {
	return ro.underlying.Get(key)
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

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (ro *Readonly) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	return ro.underlying.NewIterator(prefix, start)
}

// Stat returns a particular internal stat of the database.
func (ro *Readonly) Stat(property string) (string, error) {
	return ro.underlying.Stat(property)
}

// Compact flattens the underlying data store for the given key range.
func (ro *Readonly) Compact(start []byte, limit []byte) error {
	return ro.underlying.Compact(start, limit)
}

// Close leaves underlying database.
func (ro *Readonly) Close() error {
	panic("is not allowed, readonly")
	return ro.underlying.Close()
}
