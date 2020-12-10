package table

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// Readonly table wraper of the underling DB, so all the table's data is stored with a prefix in underling DB.
type Readonly struct {
	prefix     []byte
	underlying kvdb.ReadonlyStore
}

func NewReadonly(db kvdb.ReadonlyStore, prefix []byte) *Readonly {
	return &Readonly{
		underlying: db,
		prefix:     prefix,
	}
}

func (t *Readonly) NewReadonlyTable(prefix []byte) *Readonly {
	return NewReadonly(t, prefix)
}

func (t *Readonly) Has(key []byte) (bool, error) {
	return t.underlying.Has(prefixed(key, t.prefix))
}

func (t *Readonly) Get(key []byte) ([]byte, error) {
	return t.underlying.Get(prefixed(key, t.prefix))
}

func (t *Readonly) Stat(property string) (string, error) {
	return t.underlying.Stat(property)
}

func (t *Readonly) NewIterator(itPrefix []byte, start []byte) kvdb.Iterator {
	return &iterator{t.underlying.NewIterator(prefixed(itPrefix, t.prefix), start), t.prefix}
}

/*
 * Iterator
 */

type iterator struct {
	it     kvdb.Iterator
	prefix []byte
}

func (it *iterator) Next() bool {
	return it.it.Next()
}

func (it *iterator) Error() error {
	return it.it.Error()
}

func (it *iterator) Key() []byte {
	return noPrefix(it.it.Key(), it.prefix)
}

func (it *iterator) Value() []byte {
	return it.it.Value()
}

func (it *iterator) Release() {
	it.it.Release()
	*it = iterator{}
}
