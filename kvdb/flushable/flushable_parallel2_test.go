package flushable

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

const testPairsNum uint64 = 20

func TestFlushableParallelC(t *testing.T) {
	for x := uint64(0); x < (testPairsNum + 2); x++ {
		testFlushableParallelFail(t, x)
	}
}

func testFlushableParallelFail(t *testing.T, x uint64) {
	require := require.New(t)
	testDuration := 2 * time.Second

	disk := tmpDir()

	// open raw databases
	ldb, err := disk.OpenDB("1")
	require.NoError(err)
	defer ldb.Drop()
	defer ldb.Close()

	flushableDb := Wrap(ldb)
	tableImmutable := table.New(flushableDb, []byte("2"))

	// fill data
	for i := uint64(0); i < testPairsNum; i++ {
		_ = tableImmutable.Put(bigendian.Uint64ToBytes(i), bigendian.Uint64ToBytes(i))
		if i == testPairsNum/2 { // a half of data is flushed, other half isn't
			_ = flushableDb.Flush()
		}
	}

	stop := make(chan struct{})
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	work := sync.WaitGroup{}
	work.Add(1)
	go func() {
		defer work.Done()
		for !stopped() {
			// iterate over tableImmutable and check its content
			if x == 0 {
				_ = flushableDb.Flush()
			}
			it := tableImmutable.NewIterator(nil, nil)
			defer it.Release()
			if x == 1 {
				_ = flushableDb.Flush()
			}
			i := uint64(0)
			for ; it.Next(); i++ {
				require.NoError(it.Error(), i)
				require.Equal(bigendian.Uint64ToBytes(i), it.Key(), i)
				require.Equal(bigendian.Uint64ToBytes(i), it.Value(), i)
				if x == i+2 {
					_ = flushableDb.Flush()
				}
			}
			require.Equal(testPairsNum, i, ">> %d", x) // !here
		}
	}()

	time.Sleep(testDuration)
	close(stop)
	work.Wait()
}
