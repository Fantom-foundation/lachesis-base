package nokeyiserr

import (
	"errors"
	"github.com/Fantom-foundation/go-lachesis/kvdb"
)

var (
	errNotFound = errors.New("not found")
)

type Wrapper struct {
	kvdb.Store
}

// Wrap creates new Wrapper
func Wrap(db kvdb.Store) *Wrapper {
	return &Wrapper{db}
}

// get returns error if key isn't found
func (w *Wrapper) Get(key []byte) ([]byte, error) {
	val, err := w.Store.Get(key)
	if val == nil && err == nil {
		return nil, errNotFound
	}
	return val, err
}
