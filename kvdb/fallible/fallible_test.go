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
	assert.Nil(t, err)

	count := w.GetWriteCount()
	assert.Equal(t, -1, count)

	assertar.Panics(func() {
		db.Close()
	})

	assertar.Panics(func() {
		db.Drop()
	})

	w.SetWriteCount(2)
	count = w.GetWriteCount()
	assert.Equal(t, 2, count)

	err = db.Put(key, val)
	assertar.NoError(err)

	err = db.Put(key2, val)
	assertar.NoError(err)

	iterator := db.NewIterator([]byte("test"), nil)

	iterator.Next()
	assert.Equal(t, key, iterator.Key())

	iterator.Next()
	assert.Equal(t, key2, iterator.Key())

}
