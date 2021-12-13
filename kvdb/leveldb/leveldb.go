// +build !js

// Package leveldb implements the key-value database layer based on LevelDB.
package leveldb

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

const (
	// minCache is the minimum amount of memory in bytes to allocate to leveldb
	// read and write caching, split half and half.
	minCache = opt.KiB

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	filename   string      // filename for reporting
	underlying *leveldb.DB // LevelDB instance

	quitLock sync.Mutex // Mutex protecting the quit channel access

	onClose func() error
	onDrop  func()
}

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(path string, cache int, handles int, close func() error, drop func()) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}

	// Open the db and recover any potential corruptions
	db, err := leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: handles,
		BlockCacheCapacity:     cache / 2,
		WriteBuffer:            cache / 4, // Two of these are used internally
		Filter:                 filter.NewBloomFilter(10),
	})
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	ldb := &Database{
		filename:   path,
		underlying: db,
	}

	ldb.onClose = close
	ldb.onDrop = drop

	return ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.underlying == nil {
		panic("already closed")
	}

	ldb := db.underlying
	db.underlying = nil

	if db.onClose != nil {
		if err := db.onClose(); err != nil {
			return err
		}
		db.onClose = nil
	}
	if err := ldb.Close(); err != nil {
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

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	dat, err := db.underlying.Has(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return false, nil
	}
	return dat, err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	dat, err := db.underlying.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return nil, nil
	}
	return dat, err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.underlying.Put(key, value, nil)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.underlying.Delete(key, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() kvdb.Batch {
	return &batch{
		db: db.underlying,
		b:  new(leveldb.Batch),
	}
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	return db.underlying.NewIterator(bytesPrefixRange(prefix, start), nil)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (db *Database) GetSnapshot() (kvdb.Snapshot, error) {
	snap, err := db.underlying.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{snap}, nil
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return db.underlying.GetProperty(property)
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	return db.underlying.CompactRange(util.Range{Start: start, Limit: limit})
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.filename
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db   *leveldb.DB
	b    *leveldb.Batch
	size int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.b.Put(key, value)
	b.size += len(value)
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.b.Delete(key)
	b.size++
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	return b.db.Write(b.b, nil)
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b.Reset()
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w kvdb.Writer) error {
	return b.b.Replay(&replayer{writer: w})
}

// replayer is a small wrapper to implement the correct replay methods.
type replayer struct {
	writer  kvdb.Writer
	failure error
}

// Put inserts the given value into the key-value data store.
func (r *replayer) Put(key, value []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the key-value data store.
func (r *replayer) Delete(key []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Delete(key)
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}

// Snapshot is a DB snapshot.
type Snapshot struct {
	snap *leveldb.Snapshot
}

func (s *Snapshot) String() string {
	return s.snap.String()
}

// Get retrieves the given key if it's present in the key-value store.
func (s *Snapshot) Get(key []byte) (value []byte, err error) {
	dat, err := s.snap.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return nil, nil
	}
	return dat, err
}

// Has retrieves if a key is present in the key-value store.
func (s *Snapshot) Has(key []byte) (ret bool, err error) {
	dat, err := s.snap.Has(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return false, nil
	}
	return dat, err
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (s *Snapshot) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	return s.snap.NewIterator(bytesPrefixRange(prefix, start), nil)
}

// Release releases the snapshot. This will not release any returned
// iterators, the iterators would still be valid until released or the
// underlying DB is closed.
//
// Other methods should not be called after the snapshot has been released.
func (s *Snapshot) Release() {
	s.snap.Release()
}
