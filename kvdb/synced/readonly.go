package synced

import (
	"sync"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// iteratedReader wrapper around any kvdb.IteratedReader.
type iteratedReader struct {
	mu         *sync.RWMutex
	underlying kvdb.IteratedReader
}

// WrapIteratedReader underlying db to make its methods synced with mu.
func WrapIteratedReader(parent kvdb.IteratedReader, mu *sync.RWMutex) kvdb.IteratedReader {
	return &iteratedReader{
		mu:         mu,
		underlying: parent,
	}
}

// WrapSnapshot underlying db to make its methods synced with mu.
func WrapSnapshot(parent kvdb.Snapshot, mu *sync.RWMutex) kvdb.Snapshot {
	return &readonlySnapshot{
		iteratedReader: iteratedReader{
			mu:         mu,
			underlying: parent,
		},
		snap: parent,
	}
}

// Has checks if key is in the exists.
func (ro *iteratedReader) Has(key []byte) (bool, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Has(key)
}

// Get returns key-value pair by key.
func (ro *iteratedReader) Get(key []byte) ([]byte, error) {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return ro.underlying.Get(key)
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (ro *iteratedReader) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	ro.mu.RLock()
	defer ro.mu.RUnlock()

	return &readonlyIterator{
		mu:       ro.mu,
		parentIt: ro.underlying.NewIterator(prefix, start),
	}
}

// Stat returns a particular internal stat of the database.
func (s *store) Stat(property string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.underlying.Stat(property)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (s *store) GetSnapshot() (kvdb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, err := s.underlying.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &readonlySnapshot{
		iteratedReader: iteratedReader{
			mu:         s.mu,
			underlying: snap,
		},
		snap: snap,
	}, nil
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

type readonlySnapshot struct {
	iteratedReader
	snap kvdb.Snapshot
}

func (s *readonlySnapshot) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snap.Release()
}
