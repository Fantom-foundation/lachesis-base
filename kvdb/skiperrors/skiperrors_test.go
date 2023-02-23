package skiperrors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

func TestWrapper(t *testing.T) {
	assertar := assert.New(t)

	var (
		key1 = []byte("test-key-1")
		val1 = []byte("test-value-1")
		key2 = []byte("test-key-2")
		val2 = []byte("test-value-2")

		expected = errors.New("database closed")
	)

	mem := memorydb.New()
	origin := table.New(mem, []byte("t"))
	wrapped := Wrap(origin, expected)

	err := origin.Put(key1, val1)
	assertar.NoError(err)

	res, err := wrapped.Get(key1)
	assertar.NoError(err)
	assertar.Equal(val1, res)

	err = wrapped.Put(key2, val2)
	assertar.NoError(err)

	res, err = origin.Get(key2)
	assertar.NoError(err)
	assertar.Equal(val2, res)

	// Delete key test
	err = wrapped.Delete(key2)
	assertar.NoError(err)

	ok, err := wrapped.Has(key2)
	assertar.NoError(err)
	assertar.False(ok)

	// Get snapshot test
	_, err = wrapped.GetSnapshot()
	assertar.NoError(err)

	// Get stat test
	stat, err := wrapped.Stat("")
	assertar.NoError(err)
	assertar.Equal(stat, "")

	// Compact test

	err = wrapped.Compact(nil, nil)
	assertar.NoError(err)

	mem.Close()

	res, err = wrapped.Get(key1)
	assertar.Nil(res)
	assertar.NoError(err)

	_, err = origin.Get(key1)
	assertar.Error(err)
}
