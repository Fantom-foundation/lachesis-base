package vecflushable

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

// TestSizeLimit is used as the limit in unit-test of packages that use vecflushable
const TestSizeLimit = 100000

type backedMap struct {
	cache      map[string][]byte
	backup     kvdb.Store
	memSize    int
	maxMemSize int
}

func newBackedMap(backup kvdb.Store, maxMemSize int) *backedMap {
	return &backedMap{
		cache:      make(map[string][]byte),
		backup:     backup,
		maxMemSize: maxMemSize,
	}
}

func (w *backedMap) has(key []byte) (bool, error) {
	if _, ok := w.cache[string(key)]; ok {
		return true, nil
	}
	val, err := w.backup.Get(key)
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (w *backedMap) get(key []byte) ([]byte, error) {
	if val, ok := w.cache[string(key)]; ok {
		return common.CopyBytes(val), nil
	}
	return w.backup.Get(key)
}

func (w *backedMap) close() error {
	w.cache = nil
	return w.backup.Close()
}

func (w *backedMap) add(key string, val []byte) {
	lenBefore := len(w.cache)
	w.cache[key] = val
	// TODO it works correctly only if new key/value have the same size (which is practically true currently)
	if len(w.cache) > lenBefore {
		w.memSize += mapMemEst(len(key), len(val))
	}
}

// mayUnload evicts and flushes one batch of data
func (w *backedMap) mayUnload() error {
	for w.memSize > w.maxMemSize {
		err := w.unload()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *backedMap) unload() error {
	batch := w.backup.NewBatch()
	defer batch.Reset()

	for key, val := range w.cache {
		err := batch.Put([]byte(key), val)
		if err != nil {
			return err
		}

		delete(w.cache, key)
		rmS := mapMemEst(len(key), len(val))
		if rmS <= w.memSize {
			w.memSize -= rmS
		} else {
			w.memSize = 0
		}

		if batch.ValueSize() > kvdb.IdealBatchSize {
			break
		}
	}

	err := batch.Write()
	if err != nil {
		return err
	}

	return nil
}
