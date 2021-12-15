package skiperrors

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// wrapper is a kvdb.Store wrapper around any kvdb.Store.
// It ignores some errors of underlying store.
// NOTE: ignoring is not implemented at Iterator, Batch, .
type wrapper struct {
	readWrapper
	underlying kvdb.Store
}

type readWrapper struct {
	reader kvdb.IteratedReader

	errs []error
}

// Wrap returns a wrapped kvdb.Store.
func Wrap(db kvdb.Store, errs ...error) kvdb.Store {
	return &wrapper{
		readWrapper: readWrapper{
			reader: db,
			errs:   errs,
		},
		underlying: db,
	}
}

func (f *readWrapper) skip(got error) bool {
	if got == nil {
		return false
	}

	for _, exp := range f.errs {
		if got == exp || got.Error() == exp.Error() {
			return true
		}
	}

	return false
}

/*
 * implementation:
 */

// Has retrieves if a key is present in the key-value data store.
func (f *readWrapper) Has(key []byte) (bool, error) {
	has, err := f.reader.Has(key)
	if f.skip(err) {
		return false, nil
	}
	return has, err
}

// Get retrieves the given key if it's present in the key-value data store.
func (f *readWrapper) Get(key []byte) ([]byte, error) {
	b, err := f.reader.Get(key)
	if f.skip(err) {
		return nil, nil
	}
	return b, err
}

// Put inserts the given value into the key-value data store.
func (f *wrapper) Put(key []byte, value []byte) error {
	err := f.underlying.Put(key, value)
	if f.skip(err) {
		return nil
	}
	return err
}

// Delete removes the key from the key-value data store.
func (f *wrapper) Delete(key []byte) error {
	err := f.underlying.Delete(key)
	if f.skip(err) {
		return nil
	}
	return err
}

// NewBatch creates a write-only database that buffers changes to its host db
// until a final write is called.
func (f *wrapper) NewBatch() kvdb.Batch {
	return f.underlying.NewBatch()
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (f *readWrapper) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	return f.reader.NewIterator(prefix, start)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (f *wrapper) GetSnapshot() (kvdb.Snapshot, error) {
	snap, err := f.underlying.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		readWrapper{
			reader: snap,
			errs:   f.errs,
		},
		snap,
	}, nil
}

// Stat returns a particular internal stat of the database.
func (f *wrapper) Stat(property string) (string, error) {
	stat, err := f.underlying.Stat(property)
	if f.skip(err) {
		return "", nil
	}
	return stat, err
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (f *wrapper) Compact(start []byte, limit []byte) error {
	err := f.underlying.Compact(start, limit)
	if f.skip(err) {
		return nil
	}
	return err
}

// Close closes database.
func (f *wrapper) Close() error {
	err := f.underlying.Close()
	if f.skip(err) {
		return nil
	}
	return err
}

// Snapshot is a DB snapshot.
type Snapshot struct {
	readWrapper
	snap kvdb.Snapshot
}

func (s *Snapshot) Release() {
	s.snap.Release()
}
