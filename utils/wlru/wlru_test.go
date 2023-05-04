package wlru

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	_, err := New(10, -10)
	assert.Error(t, err)

	cache, err := New(5, 5)
	assert.Nil(t, err)

	cache.Add(1, 1, 1)
	cache.Add(2, 2, 2)
	cache.Add(2, 3, 2)
	assert.Equal(t, 2, cache.Len())
	assert.Equal(t, uint(3), cache.Weight())

	cache.Add(3, 3, 3)
	assert.Equal(t, 2, cache.Len())
	assert.Equal(t, uint(5), cache.Weight())

	w, s := cache.Total()
	assert.Equal(t, uint(5), w)
	assert.Equal(t, 2, s)

	_, ok := cache.Get(1)
	assert.False(t, ok)

	keys := cache.Keys()
	assert.Equal(t, 2, len(keys))

	assert.True(t, cache.Contains(2))
	k, v, ok := cache.GetOldest()
	assert.True(t, ok)
	assert.Equal(t, 2, k)
	assert.Equal(t, 3, v)

	cache.Peek(2)
	k, v, ok = cache.GetOldest()
	assert.True(t, ok)
	assert.Equal(t, 2, k)
	assert.Equal(t, 3, v)

	cache.Get(2)
	k, v, ok = cache.RemoveOldest()
	assert.True(t, ok)
	assert.Equal(t, 3, k)
	assert.Equal(t, 3, v)

	ok, _ = cache.ContainsOrAdd(2, 1, 5)
	assert.True(t, ok)

	_, ok, evicted := cache.PeekOrAdd(4, 1, 5)
	assert.False(t, ok)
	assert.Equal(t, 1, evicted)

	evicted = cache.Resize(2, 2)
	assert.Equal(t, 1, evicted)

	cache.Add(5, 1, 1)
	cache.Add(6, 1, 1)
	assert.Equal(t, 2, cache.Len())
	cache.Remove(5)
	assert.False(t, cache.Contains(5))

	cache.Purge()
	w = cache.Weight()
	assert.Equal(t, uint(0), w)
}
