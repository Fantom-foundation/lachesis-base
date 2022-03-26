package cachedproducer

import (
	"errors"
	"sync"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type cacheState struct {
	opened     map[string]kvdb.Store
	refCounter map[string]int
	notDropped map[string]bool
	mu         sync.Mutex
}

func openDB(p kvdb.DBProducer, c *cacheState, name string) (kvdb.Store, error) {
	{ // protected by mutex
		c.mu.Lock()
		c.notDropped[name] = true
		if store, ok := c.opened[name]; ok {
			c.refCounter[name]++
			c.mu.Unlock()
			return store, nil
		}
		c.mu.Unlock()
	}
	store, err := p.OpenDB(name)
	if err != nil {
		return nil, err
	}
	realClose := store.Close
	realDrop := store.Drop
	store = &StoreWithFn{
		Store: store,
		CloseFn: func() error {
			// call realClose only after every instance was closed
			toClose := false
			{ // protected by mutex
				c.mu.Lock()
				counter := c.refCounter[name]
				if counter <= 0 {
					c.mu.Unlock()
					return errors.New("called Close more times than OpenDB")
				} else if counter == 1 {
					delete(c.refCounter, name)
					delete(c.opened, name)
					toClose = true
				} else {
					counter--
					c.refCounter[name] = counter
				}
				c.mu.Unlock()
			}
			if toClose {
				return realClose()
			}
			return nil
		},
		DropFn: func() {
			// don't allow to call realDrop twice
			toDrop := false
			{ // protected by mutex
				c.mu.Lock()
				toDrop = c.notDropped[name]
				delete(c.notDropped, name)
				c.mu.Unlock()
			}
			if toDrop {
				realDrop()
			}
		},
	}

	{ // protected by mutex
		c.mu.Lock()
		c.opened[name] = store
		c.refCounter[name]++
		c.mu.Unlock()
	}
	return store, nil
}

type AllDBProducer struct {
	kvdb.FullDBProducer
	cacheState
}

func WrapAll(p kvdb.FullDBProducer) *AllDBProducer {
	return &AllDBProducer{
		FullDBProducer: p,
		cacheState: cacheState{
			opened:     make(map[string]kvdb.Store),
			refCounter: make(map[string]int),
			notDropped: make(map[string]bool),
		},
	}
}

func (p *AllDBProducer) OpenDB(name string) (kvdb.Store, error) {
	return openDB(p.FullDBProducer, &p.cacheState, name)
}

type DBProducer struct {
	kvdb.DBProducer
	cacheState
}

func Wrap(p kvdb.DBProducer) *DBProducer {
	return &DBProducer{
		DBProducer: p,
		cacheState: cacheState{
			opened:     make(map[string]kvdb.Store),
			notDropped: make(map[string]bool),
		},
	}
}

func (p *DBProducer) OpenDB(name string) (kvdb.Store, error) {
	return openDB(p.DBProducer, &p.cacheState, name)
}
