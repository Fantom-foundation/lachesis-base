package table

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type IteratedReader struct {
	prefix     []byte
	underlying kvdb.IteratedReader
}

// Readonly table wraper of the underling DB, so all the table's data is stored with a prefix in underling DB.
type Readonly struct {
	IteratedReader
	underlying kvdb.ReadonlyStore
}

func NewReadonly(db kvdb.ReadonlyStore, prefix []byte) *Readonly {
	return &Readonly{
		IteratedReader: IteratedReader{
			prefix:     prefix,
			underlying: db,
		},
		underlying: db,
	}
}

func (t *Readonly) NewReadonlyTable(prefix []byte) *Readonly {
	return NewReadonly(t, prefix)
}

func (t *IteratedReader) Has(key []byte) (bool, error) {
	return t.underlying.Has(prefixed(key, t.prefix))
}

func (t *IteratedReader) Get(key []byte) ([]byte, error) {
	return t.underlying.Get(prefixed(key, t.prefix))
}

func (t *IteratedReader) NewIterator(itPrefix []byte, start []byte) kvdb.Iterator {
	return &iterator{t.underlying.NewIterator(prefixed(itPrefix, t.prefix), start), t.prefix}
}

func (t *Readonly) GetSnapshot() (kvdb.Snapshot, error) {
	snap, err := t.underlying.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &snapshot{
		IteratedReader: IteratedReader{
			prefix:     t.prefix,
			underlying: snap,
		},
		snap: snap,
	}, nil
}

func (t *Readonly) Stat(property string) (string, error) {
	return t.underlying.Stat(property)
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

type snapshot struct {
	IteratedReader
	snap kvdb.Snapshot
}

func (s *snapshot) Release() {
	s.snap.Release()
}
