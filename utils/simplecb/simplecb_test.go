package simplecb

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
		cache.Add(i, i)
	}

	// adding same key on the same position but no evict because the evict is nil
	require.True(t, cache.Contains(1))
	require.Equal(t, 1, cache.Get(1))
	cache.Add(1, 11)
	require.True(t, cache.Contains(1))
	require.Equal(t, 11, cache.Get(1))

	// add one more item when the cache is full
	// 11 will replace the next position, a.k.a 2
	require.True(t, cache.Contains(2))
	require.Nil(t, cache.Get(11))
	cache.Add(11, 11)
	require.False(t, cache.Contains(2))

	// Remove
	require.True(t, cache.Contains(3))
	cache.Remove(3)
	cache.Remove(12)
	require.False(t, cache.Contains(3))

	// Purge
	require.True(t, cache.Contains(6))
	cache.Purge()
	require.Equal(t, 0, cache.Len())
	require.False(t, cache.Contains(6))

	cache.Add(12, 12)
	require.Equal(t, 1, cache.Len())

	// Add/Get nil value
	cache.Add(12, nil)
	require.Nil(t, cache.Get(12))
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
	if !cache.Contains(b.N) {
		cache.Add(b.N, b.N)
	}
}

func benchmark(b *testing.B, size int) {
	cache, err := New(size)
	require.NoError(b, err)
	require.NotNil(b, cache)
	require.Equal(b, 0, cache.Len())
	require.Equal(b, size, cache.Cap())
	run(b, cache)
}

func BenchmarkCache10(b *testing.B) {
	benchmark(b, 10)
}

func BenchmarkCache100(b *testing.B) {
	benchmark(b, 100)
}

func BenchmarkCache1000(b *testing.B) {
	benchmark(b, 1000)
}

func BenchmarkCache10000(b *testing.B) {
	benchmark(b, 10000)
}

func BenchmarkCache100000(b *testing.B) {
	benchmark(b, 100000)
}

func BenchmarkCache1000000(b *testing.B) {
	benchmark(b, 1000000)
}
