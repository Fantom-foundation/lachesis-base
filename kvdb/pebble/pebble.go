package pebble

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/utils/piecefunc"
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	filename   string     // filename for reporting
	underlying *pebble.DB // Pebble instance

	quitLock sync.Mutex // Mutex protecting the quit channel access

	onClose func() error
	onDrop  func()
}

// adjustCache scales down cache to match "real" RAM usage by process
var adjustCache = piecefunc.NewFunc([]piecefunc.Dot{
	{
		X: 0,
		Y: 16 * opt.KiB,
	},
	{
		X: 46 * opt.MiB,
		Y: 100 * opt.KiB,
	},
	{
		X: 60 * opt.MiB,
		Y: 1 * opt.MiB,
	},
	{
		X: 190 * opt.MiB,
		Y: 10 * opt.MiB,
	},
	{
		X: 300 * opt.MiB,
		Y: 18 * opt.MiB,
	},
	{
		X: 450 * opt.MiB,
		Y: 40 * opt.MiB,
	},
	{
		X: 600 * opt.MiB,
		Y: 100 * opt.MiB,
	},
	{
		X: 750 * opt.MiB,
		Y: 130 * opt.MiB,
	},
	{
		X: 1200 * opt.MiB,
		Y: 300 * opt.MiB,
	},
	{
		X: 3300 * opt.MiB,
		Y: 1000 * opt.MiB,
	},
	{
		X: 6400000 * opt.MiB,
		Y: 2000000 * opt.MiB,
	},
})

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(path string, cache int, handles int, close func() error, drop func()) (*Database, error) {
	cache = int(adjustCache(uint64(cache)))
	db, err := pebble.Open(path, &pebble.Options{
		Cache:                    pebble.NewCache(int64(cache * 2 / 3)), // default 8 MB
		MemTableSize:             cache / 3,                             // default 4 MB
		MaxOpenFiles:             handles,                               // default 1000
		WALBytesPerSync:          0,                                     // default 0 (matches RocksDB = no background syncing)
		MaxConcurrentCompactions: 3,                                     // default 1, important for big imports performance
	})

	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	pdb := Database{
		filename:   path,
		underlying: db,
		onClose:    close,
		onDrop:     drop,
	}
	return &pdb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()
	println("pebble close")

	if db.underlying == nil {
		panic("already closed")
	}

	pdb := db.underlying
	db.underlying = nil

	if db.onClose != nil {
		if err := db.onClose(); err != nil {
			return err
		}
		db.onClose = nil
	}
	if err := pdb.Close(); err != nil {
		return err
	}
	return nil
}

// Drop whole database.
func (db *Database) Drop() {
	if db.underlying != nil {
		panic("Close database first!")
	}
	if db.onDrop != nil {
		db.onDrop()
	}
}

// AsyncFlush asynchronously flushes the in-memory buffer to the disk.
func (db *Database) AsyncFlush() error {
	_, err := db.underlying.AsyncFlush()
	return err
}

// SyncFlush synchronously flushes the in-memory buffer to the disk.
func (db *Database) SyncFlush() error {
	return db.underlying.Flush()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	_, closer, err := db.underlying.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = closer.Close()
	return true, err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	value, closer, err := db.underlying.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	clonedValue := append([]byte{}, value...)
	err = closer.Close()
	return clonedValue, err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.underlying.Set(key, value, pebble.NoSync)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.underlying.Delete(key, pebble.NoSync)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() kvdb.Batch {
	return &batch{
		db: db.underlying,
		b:  db.underlying.NewBatch(),
	}
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	x := iterator{db.underlying.NewIter(bytesPrefixRange(prefix, start)), false, false}
	return &x
}

type iterator struct {
	*pebble.Iterator
	isStarted bool
	isClosed  bool
}

func (it *iterator) Next() bool {
	if it.isStarted {
		return it.Iterator.Next()
	} else {
		// pebble needs First() instead of the first Next()
		it.isStarted = true
		return it.Iterator.First()
	}
}

func (it *iterator) Release() {
	if it.isClosed {
		return
	}
	_ = it.Iterator.Close() // must not be called multiple times
	it.isClosed = true
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *pebble.IterOptions {
	if prefix == nil && start == nil {
		return nil
	}
	var r pebble.IterOptions
	if prefix != nil {
		r = bytesPrefix(prefix)
	} else {
		r.LowerBound = []byte{}
	}
	r.LowerBound = append(r.LowerBound, start...)
	return &r
}

// bytesPrefix is copied from leveldb util
func bytesPrefix(prefix []byte) pebble.IterOptions {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: limit,
	}
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	if property == "async_flush" {
		return "", db.AsyncFlush()
	}
	if property == "sync_flush" {
		return "", db.SyncFlush()
	}
	// only "leveldb." prefix is accessible using debug.chaindbProperty
	if property == "leveldb.iostats" {
		total := db.underlying.Metrics().Total()
		return fmt.Sprintf("Read(MB):%.5f Write(MB):%.5f",
			float64(total.BytesRead)/1048576.0, // 1024*1024
			float64(total.BytesFlushed+total.BytesCompacted)/1048576.0), nil
	}
	if property == "leveldb.metrics" {
		return db.underlying.Metrics().String(), nil
	}
	return "", fmt.Errorf("pebble stat property %s does not exists", property)
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	if limit == nil {
		limit = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	return db.underlying.Compact(start, limit, true)
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.filename
}

// GetSnapshot returns the latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (db *Database) GetSnapshot() (kvdb.Snapshot, error) {
	return &snapshot{
		db:   db.underlying,
		snap: db.underlying.NewSnapshot(),
	}, nil
}

type snapshot struct {
	db   *pebble.DB
	snap *pebble.Snapshot
}

// Has retrieves if a key is present in the key-value store.
func (s *snapshot) Has(key []byte) (bool, error) {
	_, closer, err := s.snap.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = closer.Close()
	return true, err
}

// Get retrieves the given key if it's present in the key-value store.
func (s *snapshot) Get(key []byte) ([]byte, error) {
	value, closer, err := s.snap.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	clonedValue := append([]byte{}, value...)
	err = closer.Close()
	return clonedValue, err
}

func (s *snapshot) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	x := iterator{s.snap.NewIter(bytesPrefixRange(prefix, start)), false, false}
	return &x
}

func (s *snapshot) Release() {
	_ = s.snap.Close()
}

// batch is a write-only pebble batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db   *pebble.DB
	b    *pebble.Batch
	size int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	err := b.b.Set(key, value, pebble.NoSync)
	b.size += len(value)
	return err
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	err := b.b.Delete(key, pebble.NoSync)
	b.size++
	return err
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	return b.db.Apply(b.b, pebble.NoSync)
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b.Reset()
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w kvdb.Writer) (err error) {
	for iter := b.b.Reader(); len(iter) > 0; {
		kind, key, value, ok := iter.Next()
		if !ok {
			break
		}
		switch kind {
		case pebble.InternalKeyKindSet:
			err = w.Put(key, value)
		case pebble.InternalKeyKindDelete:
			err = w.Delete(key)
		}
		if err != nil {
			break
		}
	}
	return
}
