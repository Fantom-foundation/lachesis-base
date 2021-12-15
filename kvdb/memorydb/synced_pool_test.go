package memorydb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

func TestSyncedPoolUnderlying(t *testing.T) {
	require := require.New(t)
	const (
		N       = 1000
		dbname1 = "db1"
		dbname2 = "db2"
		tbname  = "table"
	)

	dbs := NewProducer("")
	pool := flushable.NewSyncedPool(dbs, []byte("flushID"))

	db1, err := pool.GetUnderlying(dbname1)
	require.NoError(err)
	r1 := table.NewReadonly(db1, []byte(tbname))

	fdb1, err := pool.OpenDB(dbname1)
	require.NoError(err)
	w1 := table.New(fdb1, []byte(tbname))

	fdb2, err := pool.OpenDB(dbname2)
	require.NoError(err)
	w2 := table.New(fdb2, []byte(tbname))

	db2, err := pool.GetUnderlying(dbname2)
	require.NoError(err)
	r2 := table.NewReadonly(db2, []byte(tbname))

	pushData := func(n uint32, w kvdb.Store) {
		const size uint32 = 10
		for i := size; i > 0; i-- {
			key := bigendian.Uint32ToBytes(i + size*n)
			w.Put(key, key)
		}
	}

	checkConsistency := func() {
		it := r1.NewIterator(nil, nil)
		defer it.Release()
		var prev uint32 = 0
		for it.Next() {
			key1 := it.Key()
			i := bigendian.BytesToUint32(key1)
			require.Equal(prev+1, i)
			prev = i

			key2, err := r2.Get(key1)
			require.NoError(err)
			require.Equal(key1, key2)
		}
	}

	pushData(0, w1)
	checkConsistency()

	pushData(0, w2)
	pool.Flush(nil)
	checkConsistency()

	pushData(1, w1)
	pushData(1, w2)
	checkConsistency()
	pool.Flush(nil)
	checkConsistency()
}
