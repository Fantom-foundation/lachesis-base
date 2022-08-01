package flaggedproducer

import (
	"sync"
	"sync/atomic"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
)

type Producer struct {
	backend    kvdb.IterableDBProducer
	mu         sync.Mutex
	dbs        map[string]*flaggedStore
	flushIDKey []byte
}

func Wrap(backend kvdb.IterableDBProducer, flushIDKey []byte) *Producer {
	return &Producer{
		backend:    backend,
		dbs:        make(map[string]*flaggedStore),
		flushIDKey: flushIDKey,
	}
}

func (f *Producer) OpenDB(name string) (kvdb.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// open existing DB
	openedDB := f.dbs[name]
	if openedDB != nil {
		return openedDB, nil
	}
	// create new DB
	db, err := f.backend.OpenDB(name)
	if err != nil {
		return nil, err
	}
	flagged := &flaggedStore{
		Store: db,
		DropFn: func() {
			f.mu.Lock()
			delete(f.dbs, name)
			f.mu.Unlock()
			_ = db.Close()
			db.Drop()
		},
		Dirty:      0,
		flushIDKey: f.flushIDKey,
	}
	f.dbs[name] = flagged
	return flagged, nil
}

func (f *Producer) Names() []string {
	return f.backend.Names()
}

func (f *Producer) NotFlushedSizeEst() int {
	return 0
}

func (f *Producer) Flush(id []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, db := range f.dbs {
		err := flushable.MarkFlushID(db, f.flushIDKey, flushable.CleanPrefix, id)
		if err != nil {
			return err
		}
		atomic.StoreUint32(&db.Dirty, 0)
	}
	return nil
}

func (f *Producer) Initialize(dbNames []string, flushID []byte) ([]byte, error) {
	dbs := map[string]kvdb.Store{}
	for _, name := range dbNames {
		db, err := f.OpenDB(name)
		if err != nil {
			return flushID, err
		}
		dbs[name] = db
	}
	return flushable.CheckDBsSynced(dbs, f.flushIDKey, flushID)
}

func (f *Producer) Close() error {
	for _, db := range f.dbs {
		err := db.Store.Close()
		if err != nil {
			return err
		}
	}
	f.dbs = nil
	return nil
}
