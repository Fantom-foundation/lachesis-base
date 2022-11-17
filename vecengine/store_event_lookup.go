package vecengine

import (
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

func (vi *Engine) addEventLookup(id hash.Event) idx.Event {
	lastKey := vi.getLastLookupKey()
	newKey := lastKey + 1
	err := vi.table.EventLookup.Put(newKey.Bytes(), id.Bytes())
	if err != nil {
		vi.crit(err)
	}
	vi.setLastLookupKey(newKey)
	return newKey
}

func (vi *Engine) lookupEvent(key idx.Event) hash.Event {
	b, err := vi.table.EventLookup.Get(key.Bytes())
	if err != nil {
		vi.crit(err)
	}
	return hash.BytesToEvent(b)
}

func (vi *Engine) setLastLookupKey(key idx.Event) {
	k := []byte("ll")
	err := vi.table.EventLookup.Put(k, key.Bytes())
	if err != nil {
		vi.crit(err)
	}
}

func (vi *Engine) getLastLookupKey() idx.Event {
	k := []byte("ll")
	w, err := vi.table.EventLookup.Get(k)
	if err != nil {
		vi.crit(err)
	}
	if w == nil {
		return idx.Event(0)
	}
	return idx.BytesToEvent(w)
}
