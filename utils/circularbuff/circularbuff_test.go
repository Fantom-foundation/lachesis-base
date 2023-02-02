package circularbuff

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCircularCache(t *testing.T) {
	size := 10
	cache, err := New(size)
	require.NoError(t, err)
	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}
	require.Equal(t, cache.Len(), size)
	back, ok := cache.Peek(1)
	require.True(t, ok)
	two, ok := cache.Peek(2)
	require.True(t, ok)
	front, ok := cache.Peek(10)
	require.True(t, ok)
	_, ok = cache.Peek(11)
	require.False(t, ok)

	require.Equal(t, back, 1)
	require.Equal(t, two, 2)
	require.Equal(t, front, 10)

	// add one more item when the cache is full
	// 11 will replace the oldest item, a.k.a 1
	evicted := cache.Add(11, 11)
	require.True(t, evicted)
	require.Equal(t, cache.Len(), size)

	eleven, ok := cache.Peek(11)
	require.True(t, ok)
	require.Equal(t, eleven, 11)
	_, ok = cache.Peek(1)
	require.False(t, ok)

	// 12 will replace 11
	evicted = cache.Add(12, 12)
	require.True(t, evicted)
	require.Equal(t, cache.Len(), size)

	twelve, ok := cache.Get(12) // 12 is moved to front
	require.True(t, ok)
	require.Equal(t, twelve, 12)
	_, ok = cache.Peek(11)
	require.False(t, ok)

	// 13 will replace the oldest one, a.k.a 2
	evicted = cache.Add(13, 13)
	require.True(t, evicted)
	require.Equal(t, cache.Len(), size)

	_, ok = cache.Peek(2)
	require.False(t, ok)

	cache.Purge()
	require.Equal(t, cache.Len(), 0)
}

func TestCircularCacheWithCallback(t *testing.T) {
	size := 10
	cache, err := NewWithEvict(size, func(key interface{}, value interface{}) {
		require.Equal(t, key, 2)
		require.Equal(t, value, 2)
	})
	require.NoError(t, err)
	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}

	// re-add 1 to move it from back to front
	evicted := cache.Add(1, 1)
	require.False(t, evicted)

	// Add one more item, evict callback fundtion will be called
	// and check that evicted item is 2/2, the oldest item
	evicted = cache.Add(11, 11)
	require.True(t, evicted)
	require.Equal(t, cache.Len(), size)
}
