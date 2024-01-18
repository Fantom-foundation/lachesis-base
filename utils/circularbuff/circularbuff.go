package circularbuff

import (
	"errors"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(key interface{}, value interface{})

type key interface{}
type value interface{}

// Cache implements a non-thread safe fixed size circular cache
// Once max size is reached, every subsequent writing would overwrite oldest element
type Cache struct {
	maxSize int
	next    int
	buf     []key
	items   map[key]value
	onEvict EvictCallback
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
		maxSize: maxSize,
		next:    0,
		buf:     make([]key, maxSize),
		items:   make(map[key]value),
		onEvict: onEvict,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	// evict all items
	if c.onEvict != nil {
		for _, k := range c.buf {
			if k != nil {
				c.onEvict(k, c.items[k])
			}
		}
	}

	// re-initialize
	c.items = make(map[key]value)
	c.buf = make([]key, c.maxSize)
	c.next = 0
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache) Add(key, value interface{}) (evicted bool) {
	// Check for existing item
	evicted = false
	k := c.buf[c.next]
	if k != nil {
		if c.onEvict != nil {
			c.onEvict(k, c.items[k])
			evicted = true
		}
	}

	size := c.Len()
	c.items[key] = value
	if size < c.Len() {
		delete(c.items, k)
	}
	c.buf[c.next] = key
	c.next = (c.next + 1) % c.maxSize
	return evicted
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	if value, ok := c.items[key]; ok {
		if value == nil {
			return nil, false
		}
		return value, ok
	}
	return nil, false
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *Cache) Contains(key interface{}) bool {
	_, ok := c.items[key]
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *Cache) Remove(key interface{}) bool {
	if val, ok := c.items[key]; ok {
		delete(c.items, key)
		for i, k := range c.buf {
			if k == key {
				c.buf[i] = nil
				if c.onEvict != nil {
					c.onEvict(key, val)
				}
				break
			}
		}
		return true
	}
	return false
}

func (c *Cache) Len() int {
	return len(c.items)
}

func (c *Cache) Cap() int {
	return c.maxSize
}
