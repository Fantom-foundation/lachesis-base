package kvdb

import (
	"io"

	"github.com/ethereum/go-ethereum/ethdb"
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

// Batcher wraps the NewBatch method of a backing data store.
type Batcher interface {
	// NewBatch creates a write-only database that buffers changes to its host db
	// until a final write is called.
	NewBatch() Batch
}

// Iteratee wraps the NewIterator methods of a backing data store.
type Iteratee interface {
	// NewIterator creates a binary-alphabetical iterator over the entire keyspace
	// contained within the key-value database.
	NewIterator() Iterator

	// NewIteratorWithStart creates a binary-alphabetical iterator over a subset of
	// database content starting at a particular initial key (or after, if it does
	// not exist).
	NewIteratorWithStart(start []byte) Iterator

	// NewIteratorWithPrefix creates a binary-alphabetical iterator over a subset
	// of database content with a particular key prefix.
	NewIteratorWithPrefix(prefix []byte) Iterator
}

// Store contains all the methods required to allow handling different
// key-value data stores backing the high level database.
type Store interface {
	Reader
	Writer
	Batcher
	Iteratee
	ethdb.Stater
	ethdb.Compacter
	io.Closer
}

// Droper is able to delete the DB.
type Droper interface {
	Drop()
}

// DropableStore is Droper + Store
type DropableStore interface {
	Store
	Droper
}

// FlushableKVStore contains all the method for flushable databases,
// i.e. databases which write changes on disk only on flush.
type FlushableKVStore interface {
	DropableStore

	NotFlushedPairs() int
	NotFlushedSizeEst() int
	Flush() error
	DropNotFlushed()
}

// DbProducer represents real db producer.
type DbProducer interface {
	// Names of existing databases.
	Names() []string
	// OpenDb or create db with name.
	OpenDb(name string) DropableStore
}
