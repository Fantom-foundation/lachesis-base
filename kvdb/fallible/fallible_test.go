package fallible

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
)

func TestFallible(t *testing.T) {
	require := require.New(t)

	var (
		key  = []byte("test-key")
		key2 = []byte("test-key-2")
		val  = []byte("test-value")
		db   kvdb.Store
		err  error
	)

	mem := memorydb.New()
	w := Wrap(mem)
	db = w

	var v []byte
	v, err = db.Get(key)
	require.Nil(v)
	require.NoError(err)

	require.Panics(func() {
		_ = db.Put(key, val)
	})

	w.SetWriteCount(1)

	err = db.Put(key, val)
	require.NoError(err)

	require.Panics(func() {
		_ = db.Put(key, val)
	})

	err = db.Delete(key)
	require.Nil(err)

	count := w.GetWriteCount()
	require.Equal(-1, count)

	require.Panics(func() {
		db.Close()
	})

	require.Panics(func() {
		db.Drop()
	})

	w.SetWriteCount(2)
	count = w.GetWriteCount()
	require.Equal(2, count)

	err = db.Put(key, val)
	require.NoError(err)

	err = db.Put(key2, val)
	require.NoError(err)

	iterator := db.NewIterator([]byte("test"), nil)

	iterator.Next()
	require.Equal(key, iterator.Key())

	iterator.Next()
	require.Equal(key2, iterator.Key())
}
