package flushable

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/devnulldb"
)

// LazyFlushable is a Flushable with delayed DB producer
type LazyFlushable struct {
	*Flushable
	producer func() (kvdb.Store, error)
}

var (
	devnull = devnulldb.New()
)

// NewLazy makes flushable with real db producer.
// Real db won't be produced until first .Flush() is called.
// All the writes into the cache won't be written in parent until .Flush() is called.
func NewLazy(producer func() (kvdb.Store, error), drop func()) *LazyFlushable {
	if producer == nil {
		panic("nil producer")
	}

	w := &LazyFlushable{
		Flushable: WrapWithDrop(devnull, drop),
		producer:  producer,
	}
	return w
}

// InitUnderlyingDb is UnderlyingDb getter. Makes underlying in lazy case.
func (w *LazyFlushable) InitUnderlyingDb() (kvdb.Store, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.initUnderlyingDb()
}

func (w *LazyFlushable) initUnderlyingDb() (kvdb.Store, error) {
	var err error
	if w.underlying == devnull && w.producer != nil {
		w.underlying, err = w.producer()
		if err != nil {
			return nil, err
		}
		w.flushableReader.underlying = w.underlying
		w.producer = nil // need once
	}

	return w.underlying, nil
}

// Flush current cache into parent DB.
// Real db won't be produced until first .Flush() is called.
func (w *LazyFlushable) Flush() (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.underlying, err = w.initUnderlyingDb()
	if err != nil {
		return err
	}
	w.flushableReader.underlying = w.underlying

	return w.flush()
}
