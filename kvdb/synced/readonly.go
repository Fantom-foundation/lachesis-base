package synced

import (
	"sync"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// readonlyStore wrapper around any kvdb.ReadonlyStore.
type readonlyStore struct {
	mu         *sync.RWMutex
	underlying kvdb.ReadonlyStore
}

// WrapReadonlyStore underlying db to make its methods synced with mu.
func WrapReadonlyStore(parent kvdb.ReadonlyStore, mu *sync.RWMutex) kvdb.ReadonlyStore {
	ro := &readonlyStore{
		mu:         mu,
		underlying: parent,
	}

	return ro
}

// Has checks if key is in the exists.
func (ro *readonlyStore) Has(key []byte) (bool, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Has(key)
}

// Get returns key-value pair by key.
func (ro *readonlyStore) Get(key []byte) ([]byte, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Get(key)
}

// Stat returns a particular internal stat of the database.
func (ro *readonlyStore) Stat(property string) (string, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Stat(property)
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (ro *readonlyStore) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return &readonlyIterator{
		mu:       ro.mu,
		parentIt: ro.underlying.NewIterator(prefix, start),
	}
}

/*
 * Iterator
 */

type readonlyIterator struct {
	mu       *sync.RWMutex
	parentIt kvdb.Iterator
}

// Next scans key-value pair by key in lexicographic order. Looks in cache first,
// then - in DB.
func (it *readonlyIterator) Next() bool {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.parentIt.Next()
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *readonlyIterator) Error() error {
	return it.parentIt.Error()
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *readonlyIterator) Key() []byte {
	return it.parentIt.Key()
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *readonlyIterator) Value() []byte {
	return it.parentIt.Value()
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *readonlyIterator) Release() {
	it.parentIt.Release()
}
