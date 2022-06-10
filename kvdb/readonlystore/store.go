package readonlystore

import "github.com/Fantom-foundation/lachesis-base/kvdb"

type Store struct {
	kvdb.Store
}

func Wrap(s kvdb.Store) *Store {
	return &Store{s}
}

// Put inserts the given value into the key-value data store.
func (s *Store) Put(key []byte, value []byte) error {
	return kvdb.ErrUnsupportedOp
}

// Delete removes the key from the key-value data store.
func (s *Store) Delete(key []byte) error {
	return kvdb.ErrUnsupportedOp
}

type Batch struct {
	kvdb.Batch
}

func (s *Store) NewBatch() kvdb.Batch {
	return &Batch{s.Store.NewBatch()}
}

// Put inserts the given value into the key-value data store.
func (s *Batch) Put(key []byte, value []byte) error {
	return kvdb.ErrUnsupportedOp
}

// Delete removes the key from the key-value data store.
func (s *Batch) Delete(key []byte) error {
	return kvdb.ErrUnsupportedOp
}
