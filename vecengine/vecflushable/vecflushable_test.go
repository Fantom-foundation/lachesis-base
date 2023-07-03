package vecflushable

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/devnulldb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
)

// TestVecflushableNoBackup tests normal operation of vecflushable, before and after
// flush, while the size remains under the limit.
func TestVecflushableNoBackup(t *testing.T) {
	// we set the limit at 100000 bytes so
	// the underlying cache should not be unloaded to leveldb

	backupDB, _ := tempLevelDB()
	vecflushable := Wrap(backupDB, 100000)

	putOp := func(key []byte, value []byte) {
		err := vecflushable.Put(key, value)
		if err != nil {
			t.Error(err)
		}
	}

	getOp := func(key []byte, val []byte) {
		v, err := vecflushable.Get(key)
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
	expectedNotFlushedSize := totalItems * mapMemEst(keySize, valSize)

	loopOp(putOp, totalItems)

	assert.Equal(t, totalItems, vecflushable.NotFlushedPairs())
	assert.Equal(t, expectedNotFlushedSize, vecflushable.NotFlushedSizeEst())
	assert.Equal(t, 0, vecflushable.underlying.memSize)

	loopOp(getOp, totalItems)

	err := vecflushable.Flush()
	assert.NoError(t, err)

	expectedUnderlyingCacheSize := totalItems * mapMemEst(keySize, valSize)

	assert.Equal(t, 0, vecflushable.NotFlushedPairs())
	assert.Equal(t, 0, vecflushable.NotFlushedSizeEst())
	assert.Equal(t, expectedUnderlyingCacheSize, vecflushable.underlying.memSize)

	loopOp(getOp, totalItems)
}

// TestVecflushableBackup tests that the native map is unloaded to persistent
// storage when size exceeds the limit, respecting the eviction threshold.
func TestVecflushableBackup(t *testing.T) {
	// we set the limit at 144 bytes and insert 1160 bytes [10 * (100 + 8 + 8)]
	// the eviction threshold is 72 bytes
	//
	// unfolding:
	//
	// - the sizeLimit is first hit after inserting 6 items (6*116 = 696).
	//		* the first 3 items (3*16=48) are unloaded from the map and copied to level db
	//   => | cache = 3 | memSize = 348 | levelDB = 3
	//
	// - after inserting 3 more items, the cache limit is hit again.
	// 		* the next 3 items are unloaded from the map and copied to level db
	// 	 => | cache = 3 | memSize = 72 | levelDB = 6
	//
	// - after inserting the last item, the size of cache is still under the limit
	//   => | cache = 4 | memSize = 96 | levelDB = 6

	backupDB, _ := tempLevelDB()
	vecflushable := wrap(backupDB, 696-1, 48)

	putOp := func(key []byte, value []byte) {
		if err := vecflushable.Put(key, value); err != nil {
			t.Error(err)
		}
		if err := vecflushable.Flush(); err != nil {
			t.Error(err)
		}
	}

	totalItems := 10
	expectedUnderlyingCacheCount := 4
	expectedUnderlyingCacheSize := expectedUnderlyingCacheCount * 116

	loopOp(putOp, totalItems)

	assert.Equal(t, 0, vecflushable.NotFlushedPairs())
	assert.Equal(t, 0, vecflushable.NotFlushedSizeEst())
	assert.Equal(t, expectedUnderlyingCacheCount, len(vecflushable.underlying.cache))
	assert.Equal(t, expectedUnderlyingCacheSize, vecflushable.underlying.memSize)

	getOp := func(key []byte, val []byte) {
		v, err := vecflushable.Get(key)
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

// TestVecflushableUpdateValue tests that updating a value (as opposed to inserting
// a new value) does not increase the size of the cache.
func TestVecflushableUpdateValue(t *testing.T) {
	backupDB, _ := tempLevelDB()
	vecflushable := Wrap(backupDB, 1000)

	key0 := bigendian.Uint64ToBytes(uint64(0))
	bigVal := make([]byte, 70)
	for i := 0; i < 70; i++ {
		bigVal[i] = 0xff
	}

	for i := 0; i < 2; i++ {
		if err := vecflushable.Put(key0, bigVal); err != nil {
			t.Error(err)
		}
		if err := vecflushable.Flush(); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, 0, vecflushable.NotFlushedPairs())
	assert.Equal(t, 0, vecflushable.NotFlushedSizeEst())
	assert.Equal(t, 1, len(vecflushable.underlying.cache))
	assert.Equal(t, 178, vecflushable.underlying.memSize)

	key1 := bigendian.Uint64ToBytes(uint64(1))
	for i := 0; i < 2; i++ {
		if err := vecflushable.Put(key1, bigVal); err != nil {
			t.Error(err)
		}
	}
	if err := vecflushable.Flush(); err != nil {
		t.Error(err)
	}

	assert.Equal(t, 0, vecflushable.NotFlushedPairs())
	assert.Equal(t, 0, vecflushable.NotFlushedSizeEst())
	assert.Equal(t, 2, len(vecflushable.underlying.cache))
	assert.Equal(t, 356, vecflushable.underlying.memSize)
}

func TestSizeBenchmark(t *testing.T) {
	return // remove to benchmark
	for _, numItems := range []int{10, 100, 1000, 10000, 100000, 1000000, 10000000} {
		t.Run(strconv.Itoa(numItems), func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				vecflushable := Wrap(devnulldb.New(), 1_000_000_000)
				loopOp(
					func(key []byte, value []byte) {
						err := vecflushable.Put(key, value)
						if err != nil {
							t.Error(err)
						}
						vecflushable.Flush()
					},
					numItems,
				)
				runtime.KeepAlive(vecflushable) // prevent GC
			})
			s := res.MemBytes / uint64(numItems)
			fmt.Printf("items: %d, avg bytes/item: %v\n", numItems, s)
		})
	}
}

func BenchmarkPutAndFlush(b *testing.B) {
	b.ReportAllocs()
	vecflushable := Wrap(devnulldb.New(), 1_000_000_000)
	for op := 0; op < b.N; op++ {
		step := op & 0xff
		key := bigendian.Uint64ToBytes(uint64(step << 48))
		val := bigendian.Uint64ToBytes(uint64(step))
		err := vecflushable.Put(key, val)
		if err != nil {
			b.Error(err)
		}
		vecflushable.Flush()
	}
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
