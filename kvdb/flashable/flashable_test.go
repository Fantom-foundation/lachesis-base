package flashable

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// TestFlashableNoBackup tests normal operation of Flashable, before and after
// flush, while the size remains under the limit.
func TestFlashableNoBackup(t *testing.T) {
	// we set the limit at 1000 bytes and insert 240 bytes [10 * (8 + 8 + 8)] so
	// the underlying cache should not be unloaded to leveldb

	backupDB, _ := tempLevelDB()
	flashable := Wrap(backupDB, 1000)

	putOp := func(key []byte, value []byte) {
		err := flashable.Put(key, value)
		if err != nil {
			t.Error(err)
		}
	}

	getOp := func(key []byte, val []byte) {
		v, err := flashable.Get(key)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(v, val) {
			t.Errorf("retrieved value does not match expected value")
		}
	}

	totalItems := 10
	keySize := 8
	valSize := 8
	expectedNotFlushedSize := totalItems * (keySize + valSize)

	loopOp(putOp, totalItems)

	assert.Equal(t, totalItems, flashable.NotFlushedPairs())
	assert.Equal(t, expectedNotFlushedSize, flashable.NotFlushedSizeEst())
	assert.Equal(t, 0, flashable.underlying.cacheSizeEstimation)

	loopOp(getOp, totalItems)

	err := flashable.Flush()
	assert.NoError(t, err)

	expectedUnderlyingCacheSize := totalItems * (2*keySize + valSize)

	assert.Equal(t, 0, flashable.NotFlushedPairs())
	assert.Equal(t, 0, flashable.NotFlushedSizeEst())
	assert.Equal(t, expectedUnderlyingCacheSize, flashable.underlying.cacheSizeEstimation)

	loopOp(getOp, totalItems)
}

// TestFlashableBackup tests that the native map is unloaded to persistent
// storage when size exceeds the limit, respecting the eviction threshold.
func TestFlashableBackup(t *testing.T) {
	// we set the limit at 144 bytes and insert 240 bytes [10 * (8 + 8 + 8)]
	// the eviction threshold is 72 bytes
	//
	// unfolding:
	//
	// - the sizeLimit is first hit after inserting 6 items (6*24 = 144).
	//		* the first 3 items (3*24=72) are unloaded from the map and copied to level db
	//   => | cache = 3 | cacheSizeEstimation = 72 | levelDB = 3
	//
	// - after inserting 3 more items, the cache limit is hit again.
	// 		* the next 3 items are unloaded from the map and copied to level db
	// 	 => | cache = 3 | cacheSizeEstimation = 72 | levelDB = 6
	//
	// - after inserting the last item, the size of cache is still under the limit
	//   => | cache = 4 | cacheSizeEstimation = 96 | levelDB = 6

	backupDB, _ := tempLevelDB()
	flashable := Wrap(backupDB, 144)

	putOp := func(key []byte, value []byte) {
		if err := flashable.Put(key, value); err != nil {
			t.Error(err)
		}
		if err := flashable.Flush(); err != nil {
			t.Error(err)
		}
	}

	totalItems := 10
	expectedUnderlyingCacheCount := 4
	expectedUnderlyingCacheSize := expectedUnderlyingCacheCount * 24

	loopOp(putOp, totalItems)

	assert.Equal(t, 0, flashable.NotFlushedPairs())
	assert.Equal(t, 0, flashable.NotFlushedSizeEst())
	assert.Equal(t, expectedUnderlyingCacheCount, len(flashable.underlying.cache))
	assert.Equal(t, expectedUnderlyingCacheCount, len(flashable.underlying.cacheIndex))
	assert.Equal(t, expectedUnderlyingCacheSize, flashable.underlying.cacheSizeEstimation)

	getOp := func(key []byte, val []byte) {
		v, err := flashable.Get(key)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(v, val) {
			t.Errorf("retrieved value does not match expected value")
		}
	}

	// check that we can retrieve items from the backup store
	loopOp(getOp, totalItems)
}

// TestFlashableUpdateValue tests that updating a value (as opposed to inserting
// a new value) does not increase the size of the cache.
func TestFlashableUpdateValue(t *testing.T) {
	backupDB, _ := tempLevelDB()
	flashable := Wrap(backupDB, 1000)

	key0 := bigendian.Uint64ToBytes(uint64(0))
	bigVal := make([]byte, 70)
	for i := 0; i < 70; i++ {
		bigVal[i] = 0xff
	}

	for i := 0; i < 2; i++ {
		if err := flashable.Put(key0, bigVal); err != nil {
			t.Error(err)
		}
		if err := flashable.Flush(); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, 0, flashable.NotFlushedPairs())
	assert.Equal(t, 0, flashable.NotFlushedSizeEst())
	assert.Equal(t, 1, len(flashable.underlying.cache))
	assert.Equal(t, 1, len(flashable.underlying.cacheIndex))
	assert.Equal(t, 86, flashable.underlying.cacheSizeEstimation)

	key1 := bigendian.Uint64ToBytes(uint64(1))
	for i := 0; i < 2; i++ {
		if err := flashable.Put(key1, bigVal); err != nil {
			t.Error(err)
		}
	}
	if err := flashable.Flush(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, 0, flashable.NotFlushedPairs())
	assert.Equal(t, 0, flashable.NotFlushedSizeEst())
	assert.Equal(t, 2, len(flashable.underlying.cache))
	assert.Equal(t, 2, len(flashable.underlying.cacheIndex))
	assert.Equal(t, 172, flashable.underlying.cacheSizeEstimation)

}

func loopOp(operation func(key []byte, val []byte), iterations int) {
	for op := 0; op < iterations; op++ {
		step := op & 0xff
		key := bigendian.Uint64ToBytes(uint64(step << 48))
		val := bigendian.Uint64ToBytes(uint64(step))
		operation(key, val)
	}
}

func tempLevelDB() (kvdb.Store, error) {
	cache16mb := func(string) (int, int) {
		return 16 * opt.MiB, 64
	}
	dir, err := ioutil.TempDir("", "bench")
	if err != nil {
		panic(fmt.Sprintf("can't create temporary directory %s: %v", dir, err))
	}
	disk := leveldb.NewProducer(dir, cache16mb)
	ldb, _ := disk.OpenDB("0")
	return ldb, nil
}
