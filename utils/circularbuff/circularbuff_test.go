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
	require.Equal(t, 0, cache.Len())
	require.False(t, cache.Contains(6))

	cache.Add(12, 12)
	require.Equal(t, 1, cache.Len())

	// Add/Get nil value
	require.False(t, cache.Add(12, nil))
	val, ok = cache.Get(12)
	require.Nil(t, val)
	require.False(t, ok)
}

func TestCircularCacheWithCallback(t *testing.T) {
	t.Parallel()
	size := 10
	evictedKey := 0
	evictedVal := 0
	cache, err := NewWithEvict(size, func(key interface{}, value interface{}) {
		require.Equal(t, evictedKey, key)
		require.Equal(t, evictedVal, value)
	})
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, size, cache.Cap())

	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}
	require.Equal(t, size, cache.Len())

	// re-add 1, evict callback function will be called
	// and check that evicted item is 1/1
	evictedKey = 1
	evictedVal = 1
	require.True(t, cache.Contains(1))
	evicted := cache.Add(1, 1)
	require.Equal(t, size, cache.Len())
	require.True(t, evicted)
	require.True(t, cache.Contains(1))

	// Add one more item into the next position, 2
	evictedKey = 2
	evictedVal = 2
	require.True(t, cache.Contains(2))
	evicted = cache.Add(11, 11)
	require.Equal(t, size, cache.Len())
	require.True(t, evicted)
	require.False(t, cache.Contains(2))

	evictedKey = 5
	evictedVal = 5
	require.True(t, cache.Contains(5))
	require.True(t, cache.Remove(5))
	require.Equal(t, size-1, cache.Len())
	require.False(t, cache.Contains(5))
	require.False(t, cache.Remove(5))

	evictedKey = 9
	evictedVal = 9
	require.True(t, cache.Contains(9))
	require.True(t, cache.Remove(9))
	require.Equal(t, size-2, cache.Len())
	require.False(t, cache.Contains(9))
	require.False(t, cache.Remove(9))

	evictedKey = 3
	evictedVal = 3
	require.True(t, cache.Contains(3))
	evicted = cache.Add(12, 12)
	require.Equal(t, size-2, cache.Len())
	require.True(t, evicted)
	require.False(t, cache.Contains(3))
	require.True(t, cache.Contains(12))
}

func TestCircularCachePurgeWithCallback(t *testing.T) {
	t.Parallel()
	size := 100
	evictedCount := 0
	cache, err := NewWithEvict(size, func(key interface{}, value interface{}) {
		evictedCount++
	})
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, size, cache.Cap())

	// fill up cache
	for i := 1; i <= size; i++ {
		evicted := cache.Add(i, i)
		require.False(t, evicted)
	}

	// purge cache
	cache.Purge()
	require.Equal(t, 0, cache.Len())
	require.Equal(t, size, evictedCount)
}

func run(b *testing.B, cache *Cache) {
	for i := 0; i < b.N; i++ {
		cache.Add(i, i)
	}
	if cache.Contains(b.N / 2) {
		cache.Get(b.N / 2)
	}
	cache.Remove(b.N / 2)
	cache.Purge()
	cache.Add(b.N, b.N)
}

func benchmarkSizeWithCallback(b *testing.B, size int) {
	cache, err := NewWithEvict(size, func(key interface{}, value interface{}) {})
	require.NoError(b, err)
	require.NotNil(b, cache)
	require.Equal(b, 0, cache.Len())
	require.Equal(b, size, cache.Cap())
	run(b, cache)
}

func BenchmarkSizeWithCallback10(b *testing.B) {
	benchmarkSizeWithCallback(b, 10)
}

func BenchmarkSizeWithCallback100(b *testing.B) {
	benchmarkSizeWithCallback(b, 100)
}

func BenchmarkSizeWithCallback1000(b *testing.B) {
	benchmarkSizeWithCallback(b, 1000)
}

func BenchmarkSizeWithCallback10000(b *testing.B) {
	benchmarkSizeWithCallback(b, 10000)
}

func BenchmarkSizeWithCallback100000(b *testing.B) {
	benchmarkSizeWithCallback(b, 100000)
}

func BenchmarkSizeWithCallback1000000(b *testing.B) {
	benchmarkSizeWithCallback(b, 1000000)
}

func benchmarkSize(b *testing.B, size int) {
	cache, err := New(size)
	require.NoError(b, err)
	require.NotNil(b, cache)
	require.Equal(b, 0, cache.Len())
	require.Equal(b, size, cache.Cap())
	run(b, cache)
}

func BenchmarkSize10(b *testing.B) {
	benchmarkSize(b, 10)
}

func BenchmarkSize100(b *testing.B) {
	benchmarkSize(b, 100)
}

func BenchmarkSize1000(b *testing.B) {
	benchmarkSize(b, 1000)
}

func BenchmarkSize10000(b *testing.B) {
	benchmarkSize(b, 10000)
}

func BenchmarkSize100000(b *testing.B) {
	benchmarkSize(b, 100000)
}

func BenchmarkSize1000000(b *testing.B) {
	benchmarkSize(b, 1000000)
}
