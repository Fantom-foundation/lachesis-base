package flashable

import (
	"errors"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/common"
)

var (
	errClosed         = errors.New("flashable - database closed")
	errNotImplemented = errors.New("flashable - not implemented")
	errInconsistent   = errors.New("flashable - inconsistent db")
)

// Flashable is a fast, append only, Flushable intended for the vecengine.
// It does not implement all of the Flushable interface, just what is needed by
// the vecengine.
type Flashable struct {
	modified       map[string][]byte
	underlying     backedMap
	sizeEstimation int
	onDrop         func()
}

func Wrap(parent kvdb.Store, sizeLimit int) *Flashable {
	if parent == nil {
		panic("nil parent")
	}
	return WrapWithDrop(parent, sizeLimit, parent.Drop)
}

func WrapWithDrop(parent kvdb.Store, sizeLimit int, drop func()) *Flashable {
	if parent == nil {
		panic("nil parent")
	}
	return &Flashable{
		modified:   make(map[string][]byte),
		underlying: *newBackedMap(parent, sizeLimit, sizeLimit/2),
		onDrop:     drop,
	}
}

func (w *Flashable) clearModified() {
	for k := range w.modified {
		delete(w.modified, k)
	}
	w.sizeEstimation = 0
}

func (w *Flashable) Has(key []byte) (bool, error) {
	if w.modified == nil {
		return false, errClosed
	}
	_, ok := w.modified[string(key)]
	if ok {
		return true, nil
	}
	return w.underlying.has(key)
}

func (w *Flashable) Get(key []byte) ([]byte, error) {
	if w.modified == nil {
		return nil, errClosed
	}
	if val, ok := w.modified[string(key)]; ok {
		return common.CopyBytes(val), nil
	}
	return w.underlying.get(key)
}

func (w *Flashable) Put(key []byte, value []byte) error {
	if value == nil || key == nil {
		return errors.New("flashable: key or value is nil")
	}
	w.modified[string(key)] = common.CopyBytes(value)
	w.sizeEstimation += len(key) + len(value)
	return nil
}

func (w *Flashable) NotFlushedPairs() int {
	return len(w.modified)
}

func (w *Flashable) NotFlushedSizeEst() int {
	return w.sizeEstimation
}

func (w *Flashable) Flush() error {
	if w.modified == nil {
		return errClosed
	}

	w.underlying.unloadIfNecessary()

	for key, val := range w.modified {
		w.underlying.add(key, val)
	}

	w.clearModified()

	return nil
}

func (w *Flashable) DropNotFlushed() {
	w.clearModified()
}

func (w *Flashable) DropAll() {
	w.DropNotFlushed()
	w.underlying.dropAll()
}

func (w *Flashable) Close() error {
	if w.modified == nil {
		return errClosed
	}
	w.DropNotFlushed()
	w.modified = nil
	return w.underlying.close()
}

func (w *Flashable) Drop() {
	if w.modified != nil {
		panic("close db first")
	}
	if w.onDrop != nil {
		w.onDrop()
	}
}

/* Some methods are not implement and panic when called */

func (w *Flashable) Delete(key []byte) error {
	panic(errNotImplemented)
}

func (w *Flashable) GetSnapshot() (kvdb.Snapshot, error) {
	panic(errNotImplemented)
}

func (w *Flashable) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	panic(errNotImplemented)
}

func (w *Flashable) Stat(property string) (string, error) {
	panic(errNotImplemented)
}

func (w *Flashable) Compact(start []byte, limit []byte) error {
	panic(errNotImplemented)
}

func (w *Flashable) NewBatch() kvdb.Batch {
	panic(errNotImplemented)
}
