package nokeyiserr

import (
	"errors"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

var (
	errNotFound = errors.New("not found")
)

type Wrapper struct {
	kvdb.Store
}

type Snapshot struct {
	kvdb.Snapshot
}

// Wrap creates new Wrapper
func Wrap(db kvdb.Store) *Wrapper {
	return &Wrapper{db}
}

// Get returns error if key isn't found
func (w *Wrapper) Get(key []byte) ([]byte, error) {
	val, err := w.Store.Get(key)
	if val == nil && err == nil {
		return nil, errNotFound
	}
	return val, err
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (w *Wrapper) GetSnapshot() (kvdb.Snapshot, error) {
	snap, err := w.Store.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{snap}, nil
}

// Get returns error if key isn't found
func (w *Snapshot) Get(key []byte) ([]byte, error) {
	val, err := w.Snapshot.Get(key)
	if val == nil && err == nil {
		return nil, errNotFound
	}
	return val, err
}
