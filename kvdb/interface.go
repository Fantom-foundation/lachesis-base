package kvdb

import (
	"errors"
	"io"

	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	ErrUnsupportedOp = errors.New("operation is unsupported")
)

// IdealBatchSize defines the size of the data batches should ideally add in one
// write.
const IdealBatchSize = 100 * 1024

// Batch is a write-only database that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type Batch interface {
	Writer

	// ValueSize retrieves the amount of data queued up for writing.
	ValueSize() int

	// Write flushes any accumulated data to disk.
	Write() error

	// Reset resets the batch for reuse.
	Reset()

	// Replay replays the batch contents.
	Replay(w Writer) error
}

// Iterator iterates over a database's key/value pairs in ascending key order.
type Iterator interface {
	ethdb.Iterator
}

// Writer wraps the Put method of a backing data store.
type Writer interface {
	ethdb.KeyValueWriter
}

// Reader wraps the Has and get method of a backing data store.
type Reader interface {
	ethdb.KeyValueReader
}

// IteratedReader wraps the Iteratee, Reader methods of a backing data store.
type IteratedReader interface {
	Reader
	Iteratee
}

type Snapshot interface {
	IteratedReader
	Release()
}

// Snapshoter wraps the GetSnapshot methods of a backing data store.
type Snapshoter interface {
	GetSnapshot() (Snapshot, error)
}

// Batcher wraps the NewBatch method of a backing data store.
type Batcher interface {
	// NewBatch creates a write-only database that buffers changes to its host db
	// until a final write is called.
	NewBatch() Batch
}

// Iteratee wraps the NewIterator methods of a backing data store.
type Iteratee interface {
	// NewIterator creates a binary-alphabetical iterator over a subset
	// of database content with a particular key prefix, starting at a particular
	// initial key (or after, if it does not exist).
	NewIterator(prefix []byte, start []byte) Iterator
}

// Store contains all the methods required to allow handling different
// key-value data stores backing the high level database.
type Store interface {
	IteratedReader
	Snapshoter
	ethdb.Stater
	Writer
	Batcher
	ethdb.Compacter
	io.Closer
	Droper
}

// Droper is able to delete the DB.
type Droper interface {
	Drop()
}

// FlushableKVStore contains all the method for flushable databases,
// i.e. databases which write changes on disk only on flush.
type FlushableKVStore interface {
	Store

	NotFlushedPairs() int
	NotFlushedSizeEst() int
	Flush() error
	DropNotFlushed()
	DeleteAll()
}

// DBProducer represents real db producer.
type DBProducer interface {
	// OpenDB or create db with name.
	OpenDB(name string) (Store, error)
}

type Iterable interface {
	// Names of existing databases.
	Names() []string
}

type IterableDBProducer interface {
	DBProducer
	Iterable
}

type FlushableDBProducer interface {
	DBProducer
	NotFlushedSizeEst() int
	Flush(id []byte) error
}

type ScopedFlushableProducer interface {
	FlushableDBProducer
	Initialize(dbNames []string, flushID []byte) ([]byte, error)
	Close() error
}

type FullDBProducer interface {
	ScopedFlushableProducer
	Iterable
}
