package wlru

import (
	"sync"

	"github.com/Fantom-foundation/lachesis-base/utils/simplewlru"
)

// Cache is a thread-safe fixed size LRU cache.
type Cache struct {
	lru  *simplewlru.Cache
	lock sync.RWMutex
}

// New creates a weighted LRU of the given size.
func New(maxWeight uint, maxSize int) (*Cache, error) {
	return NewWithEvict(maxWeight, maxSize, nil)
}

// NewWithEvict constructs a fixed weight/size cache with the given eviction
// callback.
func NewWithEvict(maxWeight uint, maxSize int, onEvicted func(key interface{}, value interface{})) (*Cache, error) {
	lru, err := simplewlru.NewWithEvict(maxWeight, maxSize, onEvicted)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		lru: lru,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	c.lock.Lock()
	c.lru.Purge()
	c.lock.Unlock()
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache) Add(key, value interface{}, weight uint) (evicted int) {
	c.lock.Lock()
	evicted = c.lru.Add(key, value, weight)
	c.lock.Unlock()
	return evicted
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key interface{}) (value interface{}, ok bool) {
	c.lock.Lock()
	value, ok = c.lru.Get(key)
	c.lock.Unlock()
	return value, ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *Cache) Contains(key interface{}) bool {
	c.lock.RLock()
	containKey := c.lru.Contains(key)
	c.lock.RUnlock()
	return containKey
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache) Peek(key interface{}) (value interface{}, ok bool) {
	c.lock.RLock()
	value, ok = c.lru.Peek(key)
	c.lock.RUnlock()
	return value, ok
}

// ContainsOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *Cache) ContainsOrAdd(key, value interface{}, weight uint) (ok bool, evicted int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.lru.Contains(key) {
		return true, 0
	}
	evicted = c.lru.Add(key, value, weight)
	return false, evicted
}

// PeekOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *Cache) PeekOrAdd(key, value interface{}, weight uint) (previous interface{}, ok bool, evicted int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	previous, ok = c.lru.Peek(key)
	if ok {
		return previous, true, 0
	}

	evicted = c.lru.Add(key, value, weight)
	return nil, false, evicted
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key interface{}) (present bool) {
	c.lock.Lock()
	present = c.lru.Remove(key)
	c.lock.Unlock()
	return
}

// Resize changes the cache size.
func (c *Cache) Resize(maxWeight uint, maxSize int) (evicted int) {
	c.lock.Lock()
	evicted = c.lru.Resize(maxWeight, maxSize)
	c.lock.Unlock()
	return evicted
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() (key interface{}, value interface{}, ok bool) {
	c.lock.Lock()
	key, value, ok = c.lru.RemoveOldest()
	c.lock.Unlock()
	return
}

// GetOldest returns the oldest entry
func (c *Cache) GetOldest() (key interface{}, value interface{}, ok bool) {
	c.lock.Lock()
	key, value, ok = c.lru.GetOldest()
	c.lock.Unlock()
	return
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache) Keys() []interface{} {
	c.lock.RLock()
	keys := c.lru.Keys()
	c.lock.RUnlock()
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.lock.RLock()
	length := c.lru.Len()
	c.lock.RUnlock()
	return length
}

// Weight returns the total weight of items in the cache.
func (c *Cache) Weight() uint {
	c.lock.RLock()
	w := c.lru.Weight()
	c.lock.RUnlock()
	return w
}

// Total returns the total weight and number of items in the cache.
func (c *Cache) Total() (weight uint, num int) {
	c.lock.RLock()
	weight, num = c.lru.Total()
	c.lock.RUnlock()
	return weight, num
}
