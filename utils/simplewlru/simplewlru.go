package simplewlru

import (
	"container/list"
	"errors"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(key interface{}, value interface{})

// Cache implements a non-thread safe fixed size/weight LRU cache
type Cache struct {
	maxSize   int
	weight    uint
	maxWeight uint
	evictList *list.List
	items     map[interface{}]*list.Element
	onEvict   EvictCallback
}

// entry is used to hold a value in the evictList
type entry struct {
	key    interface{}
	value  interface{}
	weight uint
}

// New creates a weighted LRU of the given size.
func New(maxWeight uint, maxSize int) (*Cache, error) {
	return NewWithEvict(maxWeight, maxSize, nil)
}

// NewWeightedLRU constructs an LRU of the given weight and size
func NewWithEvict(maxWeight uint, maxSize int, onEvict EvictCallback) (*Cache, error) {
	if maxSize < 0 {
		return nil, errors.New("must provide a non-negative size")
	}
	if maxWeight < 0 {
		return nil, errors.New("must provide a non-negative weight")
	}
	c := &Cache{
		maxSize:   maxSize,
		maxWeight: maxWeight,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvict:   onEvict,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	for k, v := range c.items {
		e := v.Value.(*entry)
		c.weight -= e.weight
		if c.onEvict != nil {
			c.onEvict(k, e.value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *Cache) Add(key, value interface{}, weight uint) (evicted int) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		existing := ent.Value.(*entry)
		c.weight -= existing.weight
		c.weight += weight
		existing.value = value
		existing.weight = weight
		return c.normalize()
	}

	// Add new item
	ent := &entry{key, value, weight}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry
	c.weight += weight

	return c.normalize()
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key interface{}) (value interface{}, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return nil, false
		}
		return ent.Value.(*entry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *Cache) Contains(key interface{}) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache) Peek(key interface{}) (value interface{}, ok bool) {
	var ent *list.Element
	if ent, ok = c.items[key]; ok {
		return ent.Value.(*entry).value, true
	}
	return nil, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *Cache) Remove(key interface{}) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() (key interface{}, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// GetOldest returns the oldest entry
func (c *Cache) GetOldest() (key interface{}, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache) Keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	return c.evictList.Len()
}

// Weight returns the total weight of items in the cache.
func (c *Cache) Weight() uint {
	return c.weight
}

// Total returns the total weight and number of items in the cache.
func (c *Cache) Total() (weight uint, num int) {
	return c.Weight(), c.Len()
}

// Resize changes the cache size.
func (c *Cache) Resize(maxWeight uint, maxSize int) (evicted int) {
	c.maxWeight = maxWeight
	c.maxSize = maxSize
	return c.normalize()
}

func (c *Cache) normalize() (evicted int) {
	for c.weight > c.maxWeight || c.Len() > c.maxSize {
		c.removeOldest()
		evicted++
	}
	return evicted
}

// removeOldest removes the oldest item from the cache.
func (c *Cache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *Cache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	c.weight -= kv.weight
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
