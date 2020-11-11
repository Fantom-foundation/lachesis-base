package vecfc

import (
	lru "github.com/hashicorp/golang-lru"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
	"github.com/Fantom-foundation/lachesis-base/vecengine"
)

// IndexCacheConfig - config for cache sizes of Engine
type IndexCacheConfig struct {
	ForklessCause    int `json:"forklessCause"`
	HighestBeforeSeq int `json:"highestBeforeSeq"`
	LowestAfterSeq   int `json:"lowestAfterSeq"`
}

// IndexConfig - Engine config (cache sizes)
type IndexConfig struct {
	Caches IndexCacheConfig `json:"cacheSizes"`
}

// Engine is a data to detect forkless-cause condition, calculate median timestamp, detect forks.
type Index struct {
	*vecengine.Engine

	crit          func(error)
	validators    *pos.Validators
	validatorIdxs map[idx.ValidatorID]idx.Validator

	getEvent func(hash.Event) dag.Event

	vecDb kvdb.Store
	table struct {
		HighestBeforeSeq kvdb.Store `table:"S"`
		LowestAfterSeq   kvdb.Store `table:"s"`
	}

	cache struct {
		HighestBeforeSeq *lru.Cache
		LowestAfterSeq   *lru.Cache
		ForklessCause    *lru.Cache
	}

	cfg IndexConfig
}

// DefaultConfig returns default index config
func DefaultConfig() IndexConfig {
	return IndexConfig{
		Caches: IndexCacheConfig{
			ForklessCause:    2500,
			HighestBeforeSeq: 500,
			LowestAfterSeq:   500,
		},
	}
}

// LiteConfig returns default index config for tests
func LiteConfig() IndexConfig {
	return IndexConfig{
		Caches: IndexCacheConfig{
			ForklessCause:    100,
			HighestBeforeSeq: 20,
			LowestAfterSeq:   20,
		},
	}
}

// NewIndex creates Index instance.
func NewIndex(crit func(error), config IndexConfig) *Index {
	vi := &Index{
		cfg:  config,
		crit: crit,
	}
	vi.Engine = vecengine.NewIndex(crit, vi.GetEngineCallbacks())
	vi.initCaches()

	return vi
}

func NewIndexWithEngine(crit func(error), config IndexConfig, engine *vecengine.Engine) *Index {
	vi := &Index{
		Engine: engine,
		cfg:    config,
		crit:   crit,
	}
	vi.initCaches()

	return vi
}

func (vi *Index) initCaches() {
	vi.cache.ForklessCause, _ = lru.New(vi.cfg.Caches.ForklessCause)
	vi.cache.HighestBeforeSeq, _ = lru.New(vi.cfg.Caches.HighestBeforeSeq)
	vi.cache.LowestAfterSeq, _ = lru.New(vi.cfg.Caches.LowestAfterSeq)
}

// Reset resets buffers.
func (vi *Index) Reset(validators *pos.Validators, db kvdb.Store, getEvent func(hash.Event) dag.Event) {
	vi.Engine.Reset(validators, db, getEvent)
	vi.getEvent = getEvent
	vi.validators = validators
	vi.validatorIdxs = validators.Idxs()
	vi.cache.ForklessCause.Purge()
	vi.onDropNotFlushed()
}

func (vi *Index) GetEngineCallbacks() vecengine.Callbacks {
	return vecengine.Callbacks{
		GetHighestBefore: func(event hash.Event) vecengine.HighestBeforeI {
			return vi.GetHighestBefore(event)
		},
		GetLowestAfter: func(event hash.Event) vecengine.LowestAfterI {
			return vi.GetLowestAfter(event)
		},
		SetHighestBefore: func(event hash.Event, b vecengine.HighestBeforeI) {
			vi.SetHighestBefore(event, b.(*HighestBeforeSeq))
		},
		SetLowestAfter: func(event hash.Event, b vecengine.LowestAfterI) {
			vi.SetLowestAfter(event, b.(*LowestAfterSeq))
		},
		NewHighestBefore: func(size idx.Validator) vecengine.HighestBeforeI {
			return NewHighestBeforeSeq(size)
		},
		NewLowestAfter: func(size idx.Validator) vecengine.LowestAfterI {
			return NewLowestAfterSeq(size)
		},
		OnDbReset:        vi.onDbReset,
		OnDropNotFlushed: vi.onDropNotFlushed,
	}
}

func (vi *Index) onDbReset(db kvdb.Store) {
	vi.vecDb = db
	table.MigrateTables(&vi.table, vi.vecDb)
}

func (vi *Index) onDropNotFlushed() {
	vi.cache.HighestBeforeSeq.Purge()
	vi.cache.LowestAfterSeq.Purge()
}

// GetMergedHighestBefore returns HighestBefore vector clock without branches, where branches are merged into one
func (vi *Index) GetMergedHighestBefore(id hash.Event) *HighestBeforeSeq {
	return vi.Engine.GetMergedHighestBefore(id).(*HighestBeforeSeq)
}
