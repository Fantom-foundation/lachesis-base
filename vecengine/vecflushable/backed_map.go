package vecflushable

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/batched"
)

// TestSizeLimit is used as the limit in unit-test of packages that use vecflushable
const TestSizeLimit = 100000

type backedMap struct {
	cache               map[string][]byte
	cacheIndex          []string
	backup              kvdb.Store
	sizeLimit           int
	evictionThreshold   int
	cacheSizeEstimation int
}

func newBackedMap(backup kvdb.Store, sizeLimit int, evictionThreshold int) *backedMap {
	return &backedMap{
		cache:             make(map[string][]byte),
		cacheIndex:        []string{},
		backup:            backup,
		sizeLimit:         sizeLimit,
		evictionThreshold: evictionThreshold,
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
	w.cacheIndex = nil
	return w.backup.Close()
}

func (w *backedMap) add(key string, val []byte) {
	lenBefore := len(w.cache)
	w.cache[key] = val
	if len(w.cache) > lenBefore {
		w.cacheIndex = append(w.cacheIndex, key)
		w.cacheSizeEstimation += mapMemEst(len(key), len(val))
	}
}

func (w *backedMap) unloadIfNecessary() error {
	if w.cacheSizeEstimation < w.sizeLimit {
		return nil
	}

	batch := w.backup.NewBatch()
	defer batch.Reset()

	cutoff := 0
	removedEstimation := 0
	for _, key := range w.cacheIndex {
		var err error

		val, ok := w.cache[key]
		if !ok {
			return errInconsistent
		}

		err = batch.Put([]byte(key), val)
		if err != nil {
			return err
		}

		if batch.ValueSize() > kvdb.IdealBatchSize {
			err = batch.Write()
			if err != nil {
				return err
			}
			batch.Reset()
		}

		delete(w.cache, key)
		removedEstimation += mapMemEst(len(key), len(val))
		cutoff++

		if removedEstimation >= w.evictionThreshold {
			break
		}
	}

	err := batch.Write()
	if err != nil {
		return err
	}

	w.cacheIndex = w.cacheIndex[cutoff:]
	w.cacheSizeEstimation -= removedEstimation

	return nil
}

func (w *backedMap) deleteAll() {
	w.cache = make(map[string][]byte)
	w.cacheIndex = []string{}
	w.cacheSizeEstimation = 0

	wrappedDB := batched.Wrap(w.backup)
	it := wrappedDB.NewIterator(nil, nil)
	for it.Next() {
		wrappedDB.Delete(it.Key())
	}
	it.Release()
	wrappedDB.Flush()
}
