package circularbuff

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCircularCache(t *testing.T) {
	t.Parallel()
	cache, err := New(-1)
	require.Nil(t, cache)
	require.Error(t, err)

	size := 10
	cache, err = New(size)
	require.NoError(t, err)
	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}

	// adding same key on the same position but no evict because the evict is nil
	require.True(t, cache.Contains(1))
	val, _ := cache.Get(1)
	require.Equal(t, 1, val)
	evicted := cache.Add(1, 11)
	require.False(t, evicted)
	require.True(t, cache.Contains(1))
	val, _ = cache.Get(1)
	require.Equal(t, 11, val)

	// add one more item when the cache is full
	// 11 will replace the next position, a.k.a 2
	require.True(t, cache.Contains(2))
	val, ok := cache.Get(11)
	require.Nil(t, val)
	require.False(t, ok)
	evicted = cache.Add(11, 11)
	require.False(t, evicted)
	require.False(t, cache.Contains(2))

	// Remove
	require.True(t, cache.Contains(3))
	require.True(t, cache.Remove(3))
	require.False(t, cache.Remove(12))
	require.False(t, cache.Contains(3))

	// Purge
	require.True(t, cache.Contains(6))
	cache.Purge()
	require.False(t, cache.Contains(6))

	// Add/Get nil value
	require.False(t, cache.Add(12, nil))
	val, ok = cache.Get(12)
	require.Nil(t, val)
	require.False(t, ok)
}

func TestCircularCacheWithCallback(t *testing.T) {
	t.Parallel()
	size := 10
	cache, err := NewWithEvict(size, func(key interface{}, value interface{}) {
		require.Equal(t, key, 1)
		require.Equal(t, value, 1)
	})
	require.NoError(t, err)
	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}

	// re-add 1, evict callback fundtion will be called
	// and check that evicted item is 1/1
	require.True(t, cache.Contains(1))
	evicted := cache.Add(1, 1)
	require.True(t, evicted)
	require.True(t, cache.Contains(1))

	// Add one more item into the next position, 2
	require.True(t, cache.Contains(2))
	evicted = cache.Add(11, 11)
	require.False(t, evicted)
	require.False(t, cache.Contains(2))
}
