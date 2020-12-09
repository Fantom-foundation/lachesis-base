package flushable

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/status-im/keycard-go/hexutils"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/synced"
)

var _ kvdb.FlushableDBProducer = (*SyncedPool)(nil)

const (
	DirtyPrefix = 0xde
	CleanPrefix = 0x00
)

type wrappers struct {
	*LazyFlushable
	kvdb.ReadonlyStore
}

type SyncedPool struct {
	producer kvdb.DBProducer

	wrappers    map[string]wrappers
	queuedDrops map[string]struct{}

	flushIDKey []byte

	sync.Mutex
	flushing sync.RWMutex
}

func NewSyncedPool(producer kvdb.DBProducer, flushIDKey []byte) *SyncedPool {
	if producer == nil {
		panic("nil producer")
	}

	p := &SyncedPool{
		producer:    producer,
		wrappers:    make(map[string]wrappers),
		queuedDrops: make(map[string]struct{}),
		flushIDKey:  flushIDKey,
	}

	return p
}

func (p *SyncedPool) Initialize(dbNames []string) error {
	for _, name := range dbNames {
		wrapper := p.getDb(name)
		_, err := wrapper.InitUnderlyingDb()
		if err != nil {
			return err
		}
	}
	return p.checkDBsSynced()
}

func (p *SyncedPool) callbacks(name string) (
	onOpen func() (kvdb.DropableStore, error),
	onDrop func(),
) {
	onOpen = func() (kvdb.DropableStore, error) {
		return p.producer.OpenDB(name)
	}

	onDrop = func() {
		p.dropDb(name)
	}

	return
}

func (p *SyncedPool) dropDb(name string) {
	p.Lock()
	defer p.Unlock()

	p.queuedDrops[name] = struct{}{}
}

func (p *SyncedPool) OpenDB(name string) (kvdb.DropableStore, error) {
	p.Lock()
	defer p.Unlock()

	return p.getDb(name), nil
}

func (p *SyncedPool) GetUnderlying(name string) (kvdb.ReadonlyStore, error) {
	p.Lock()
	defer p.Unlock()

	wrapper := p.wrappers[name]
	if wrapper.ReadonlyStore != nil {
		return wrapper.ReadonlyStore, nil
	}

	wrapper.LazyFlushable = p.getDb(name)
	db, err := wrapper.LazyFlushable.initUnderlyingDb()
	if err != nil {
		return nil, err
	}
	wrapper.ReadonlyStore = synced.WrapReadonlyStore(db, &p.flushing)
	p.wrappers[name] = wrapper

	return wrapper.ReadonlyStore, nil
}

func (p *SyncedPool) getDb(name string) *LazyFlushable {
	wrapper := p.wrappers[name]
	if wrapper.LazyFlushable != nil {
		return wrapper.LazyFlushable
	}

	open, drop := p.callbacks(name)
	wrapper.LazyFlushable = NewLazy(open, drop)
	p.wrappers[name] = wrapper

	return wrapper.LazyFlushable
}

func (p *SyncedPool) Flush(id []byte) error {
	p.Lock()
	defer p.Unlock()

	p.flushing.Lock()
	defer p.flushing.Unlock()

	return p.flush(id)
}

func (p *SyncedPool) flush(id []byte) error {
	// drop old DBs
	for name := range p.queuedDrops {
		w := p.wrappers[name]
		delete(p.wrappers, name)
		if w.LazyFlushable == nil {
			continue
		}
		db := w.LazyFlushable.underlying
		if db == nil {
			continue
		}
		// db.Close() is called inside wrapper.Close()
		db.(kvdb.DropableStore).Drop()
	}
	p.queuedDrops = make(map[string]struct{})

	// write dirty flags
	for _, w := range p.wrappers {
		db, err := w.InitUnderlyingDb()
		if err != nil {
			return err
		}

		prev, err := db.Get(p.flushIDKey)
		if err != nil {
			return err
		}
		if prev == nil {
			prev = []byte("initial")
		}

		marker := bytes.NewBuffer(nil)
		marker.Write([]byte{DirtyPrefix})
		marker.Write(prev)
		marker.Write(id)
		err = db.Put(p.flushIDKey, marker.Bytes())
		if err != nil {
			return err
		}
	}

	// flush data
	for _, wrapper := range p.wrappers {
		err := wrapper.Flush()
		if err != nil {
			return err
		}
	}

	// write clean flags
	for _, w := range p.wrappers {
		db, err := w.InitUnderlyingDb()
		if err != nil {
			return err
		}
		err = db.Put(p.flushIDKey, append([]byte{CleanPrefix}, id...))
		if err != nil {
			return err
		}
	}

	return nil
}

// NotFlushedSizeEst returns a total size of not flushed key pairs
func (p *SyncedPool) NotFlushedSizeEst() int {
	p.Lock()
	defer p.Unlock()

	totalNotFlushed := 0
	for _, db := range p.wrappers {
		totalNotFlushed += db.NotFlushedSizeEst()
	}
	return totalNotFlushed
}

// checkDBsSynced on startup, after all dbs are registered.
func (p *SyncedPool) checkDBsSynced() error {
	p.Lock()
	defer p.Unlock()

	var (
		prevID *[]byte
		descrs []string
		list   = func() string {
			return strings.Join(descrs, ",\n")
		}
	)
	for name, w := range p.wrappers {
		db, err := w.InitUnderlyingDb()
		if err != nil {
			return err
		}

		mark, err := db.Get(p.flushIDKey)
		if err != nil {
			return err
		}
		descrs = append(descrs, fmt.Sprintf("%s: %s", name, hexutils.BytesToHex(mark)))

		if bytes.HasPrefix(mark, []byte{DirtyPrefix}) {
			return fmt.Errorf("dirty state: %s", list())
		}
		if prevID == nil {
			prevID = &mark
		}
		if !bytes.Equal(mark, *prevID) {
			return fmt.Errorf("not synced: %s", list())
		}
	}
	return nil
}

func (p *SyncedPool) Close() error {
	for _, w := range p.wrappers {
		err := w.Close()
		if err != nil {
			return err
		}
	}
	*p = SyncedPool{}
	return nil
}
