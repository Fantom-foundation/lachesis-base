package simplecb

import (
	"errors"
)

type key interface{}
type value interface{}

// Cache implements a non-thread safe fixed size circular cache
// Once max size is reached, every subsequent writing would overwrite oldest element
type Cache struct {
	maxSize int
	next    int
	buf     []key
	items   map[key]value
}

// New creates a circular cache of the given size.
func New(maxSize int) (*Cache, error) {
	if maxSize < 0 {
		return nil, errors.New("must provide a non-negative size")
	}
	c := &Cache{
		maxSize: maxSize,
		next:    0,
		buf:     make([]key, maxSize),
		items:   make(map[key]value),
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	// reset cache by re-initializing
	c.items = make(map[key]value)
	c.buf = make([]key, c.maxSize)
	c.next = 0
}

// Add adds a value to the cache
func (c *Cache) Add(key, value interface{}) {
	k := c.buf[c.next]

	size := c.Len()
	c.items[key] = value
	if size < c.Len() {
		delete(c.items, k)
	}
	c.buf[c.next] = key
	c.next = (c.next + 1) % c.maxSize
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key interface{}) interface{} {
	return c.items[key]
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *Cache) Contains(key interface{}) bool {
	_, ok := c.items[key]
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *Cache) Remove(key interface{}) {
	delete(c.items, key)
}

func (c *Cache) Len() int {
	return len(c.items)
}

func (c *Cache) Cap() int {
	return c.maxSize
}
