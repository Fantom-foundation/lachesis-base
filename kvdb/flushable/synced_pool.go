package flushable

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/status-im/keycard-go/hexutils"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/readonlystore"
	"github.com/Fantom-foundation/lachesis-base/kvdb/synced"
)

var _ kvdb.FlushableDBProducer = (*SyncedPool)(nil)

const (
	DirtyPrefix = 0xde
	CleanPrefix = 0x00
)

type wrappers struct {
	Flushable     *closeDropWrapped
	ReadonlyStore kvdb.Store
}

type SyncedPool struct {
	producer kvdb.DBProducer

	wrappers      map[string]wrappers
	queuedDrops   map[string]struct{}
	queuedDropsMu sync.Mutex

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

func (p *SyncedPool) Initialize(dbNames []string, flushID []byte) ([]byte, error) {
	for _, name := range dbNames {
		wrapper := p.getDB(name)
		_, err := wrapper.InitUnderlyingDb()
		if err != nil {
			return flushID, err
		}
	}
	return p.checkDBsSynced(flushID)
}

func (p *SyncedPool) callbacks(name string) (
	onOpen func() (kvdb.Store, error),
	onClose func() error,
	onDrop func(),
) {
	onOpen = func() (kvdb.Store, error) {
		return p.producer.OpenDB(name)
	}

	onClose = func() error {
		return nil
	}

	onDrop = func() {
		p.enqueueDropDb(name)
	}

	return
}

func (p *SyncedPool) enqueueDropDb(name string) {
	p.queuedDropsMu.Lock()
	defer p.queuedDropsMu.Unlock()

	p.queuedDrops[name] = struct{}{}
}

func (p *SyncedPool) popQueuedDrops() []string {
	p.queuedDropsMu.Lock()
	defer p.queuedDropsMu.Unlock()

	res := make([]string, 0, len(p.queuedDrops))
	for name := range p.queuedDrops {
		res = append(res, name)
	}
	p.queuedDrops = make(map[string]struct{})
	return res
}

func (p *SyncedPool) OpenDB(name string) (kvdb.Store, error) {
	p.Lock()
	defer p.Unlock()

	return p.getDB(name), nil
}

func (p *SyncedPool) GetUnderlying(name string) (kvdb.Store, error) {
	p.Lock()
	defer p.Unlock()

	wrapper := p.wrappers[name]
	if wrapper.ReadonlyStore != nil {
		return wrapper.ReadonlyStore, nil
	}

	wrapper.Flushable = p.getDB(name)
	db, err := wrapper.Flushable.initUnderlyingDb()
	if err != nil {
		return nil, err
	}
	wrapper.ReadonlyStore = readonlystore.Wrap(synced.WrapStore(db, &p.flushing))
	p.wrappers[name] = wrapper

	return wrapper.ReadonlyStore, nil
}

func (p *SyncedPool) getDB(name string) *closeDropWrapped {
	wrapper := p.wrappers[name]
	if wrapper.Flushable != nil {
		return wrapper.Flushable
	}

	open, close, drop := p.callbacks(name)
	wrapper.Flushable = &closeDropWrapped{
		LazyFlushable: NewLazy(open, drop),
		close:         close,
		drop:          drop,
	}
	wrapper.Flushable.close = close
	p.wrappers[name] = wrapper

	return wrapper.Flushable
}

func (p *SyncedPool) Flush(id []byte) error {
	p.Lock()
	defer p.Unlock()

	p.flushing.Lock()
	defer p.flushing.Unlock()

	return p.flush(id)
}

func (p *SyncedPool) flush(id []byte) error {
	queuedDropsList := p.popQueuedDrops()
	// close and drop DBs
	for _, name := range queuedDropsList {
		w := p.wrappers[name]
		delete(p.wrappers, name)
		if w.Flushable == nil {
			continue
		}
		err := w.Flushable.RealClose()
		if err != nil {
			return err
		}
		db := w.Flushable.underlying
		if db == nil {
			continue
		}
		db.Drop()
	}

	// write dirty flags
	for _, w := range p.wrappers {
		db, err := w.Flushable.InitUnderlyingDb()
		if err != nil {
			return err
		}

		err = MarkFlushID(db, p.flushIDKey, DirtyPrefix, id)
		if err != nil {
			return err
		}
	}

	// flush data
	for _, wrapper := range p.wrappers {
		err := wrapper.Flushable.Flush()
		if err != nil {
			return err
		}
	}

	// write clean flags
	for _, w := range p.wrappers {
		db, err := w.Flushable.InitUnderlyingDb()
		if err != nil {
			return err
		}
		err = MarkFlushID(db, p.flushIDKey, CleanPrefix, id)
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
		totalNotFlushed += db.Flushable.NotFlushedSizeEst()
	}
	return totalNotFlushed
}

// checkDBsSynced on startup, after all dbs are registered.
func (p *SyncedPool) checkDBsSynced(flushID []byte) ([]byte, error) {
	p.Lock()
	defer p.Unlock()

	dbs := map[string]kvdb.Store{}
	for name, w := range p.wrappers {
		db, err := w.Flushable.InitUnderlyingDb()
		if err != nil {
			return flushID, err
		}
		dbs[name] = db
	}
	return CheckDBsSynced(dbs, p.flushIDKey, flushID)
}

func CheckDBsSynced(dbs map[string]kvdb.Store, flushIDKey, flushID []byte) ([]byte, error) {
	var (
		descrs []string
		list   = func() string {
			return strings.Join(descrs, ", ")
		}
		nonInit bool
	)
	for name, db := range dbs {
		mark, err := db.Get(flushIDKey)
		if err != nil {
			return flushID, err
		}
		if mark == nil {
			nonInit = true
			continue
		}
		descrs = append(descrs, fmt.Sprintf("%s: %s", name, hexutils.BytesToHex(mark)))

		if bytes.HasPrefix(mark, []byte{DirtyPrefix}) {
			return flushID, fmt.Errorf("dirty state: %s", list())
		}
		if flushID == nil {
			flushID = mark
		}
		if !bytes.Equal(mark, flushID) {
			return flushID, fmt.Errorf("not synced: %s != %s", hexutils.BytesToHex(flushID), list())
		}
	}
	if flushID != nil && nonInit {
		return flushID, fmt.Errorf("non-initialized DB state")
	}
	return flushID, nil
}

func (p *SyncedPool) Names() []string {
	p.Lock()
	defer p.Unlock()
	names := make([]string, 0, len(p.wrappers))
	for name := range p.wrappers {
		names = append(names, name)
	}
	return names
}

func (p *SyncedPool) Close() error {
	for _, w := range p.wrappers {
		err := w.Flushable.RealClose()
		if err != nil {
			return err
		}
	}
	*p = SyncedPool{}
	return nil
}

func MarkFlushID(db kvdb.Store, key []byte, prefix byte, flushID []byte) error {
	return db.Put(key, append([]byte{prefix}, flushID...))
}
