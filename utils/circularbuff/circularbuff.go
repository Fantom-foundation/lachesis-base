package circularbuff

import (
	"container/list"
	"errors"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(key interface{}, value interface{})

// Cache implements a non-thread safe fixed size circular cache
// Once max size is reached, every subsequent writing would overwrite oldest element
type Cache struct {
	maxSize   int
	evictList *list.List
	items     map[interface{}]*list.Element
	onEvict   EvictCallback
}

// entry is used to hold a value in the evictList
type entry struct {
	key   interface{}
	value interface{}
}

// New creates a circular cache of the given size.
func New(maxSize int) (*Cache, error) {
	return NewWithEvict(maxSize, nil)
}

// NewWithEvict constructscircular cache of the given size with callback
func NewWithEvict(maxSize int, onEvict EvictCallback) (*Cache, error) {
	if maxSize < 0 {
		return nil, errors.New("must provide a non-negative size")
	}
	c := &Cache{
		maxSize:   maxSize,
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
		if c.onEvict != nil {
			c.onEvict(k, e.value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache) Add(key, value interface{}) (evicted bool) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		existing := ent.Value.(*entry)
		existing.value = value
		return false
	}

	// Add new item
	ent := &entry{key, value}
	if c.Len() >= c.maxSize {
		c.removeOldest()
		entry := c.evictList.PushBack(ent)
		c.items[key] = entry
		return true
	}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry
	return false
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
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
