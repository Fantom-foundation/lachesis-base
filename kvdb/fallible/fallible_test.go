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
		key = []byte("test-key")
		val = []byte("test-value")
		db  kvdb.Store
		err error
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
}
