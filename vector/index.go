package vector

import (
	"errors"
	"fmt"

	"github.com/hashicorp/golang-lru"

	"github.com/Fantom-foundation/go-lachesis/abft/dagidx"
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/kvdb"
	"github.com/Fantom-foundation/go-lachesis/kvdb/flushable"
	"github.com/Fantom-foundation/go-lachesis/kvdb/table"
)

// IndexCacheConfig - config for cache sizes of Index
type IndexCacheConfig struct {
	ForklessCause     int `json:"forklessCause"`
	HighestBeforeSeq  int `json:"highestBeforeSeq"`
	HighestBeforeTime int `json:"highestBeforeTime"`
	LowestAfterSeq    int `json:"lowestAfterSeq"`
}

// IndexConfig - Index config (cache sizes)
type IndexConfig struct {
	Caches IndexCacheConfig `json:"cacheSizes"`
}

// Index is a data to detect forkless-cause condition, calculate median timestamp, detect forks.
type Index struct {
	crit          func(error)
	validators    *pos.Validators
	validatorIdxs map[idx.StakerID]idx.Validator

	bi *branchesInfo

	getEvent func(hash.Event) dag.Event

	vecDb kvdb.FlushableKVStore
	table struct {
		HighestBeforeSeq  kvdb.Store `table:"S"`
		HighestBeforeTime kvdb.Store `table:"T"`
		LowestAfterSeq    kvdb.Store `table:"s"`

		EventBranch  kvdb.Store `table:"b"`
		BranchesInfo kvdb.Store `table:"B"`
	}

	cache struct {
		HighestBeforeSeq  *lru.Cache
		HighestBeforeTime *lru.Cache
		LowestAfterSeq    *lru.Cache
		ForklessCause     *lru.Cache
	}

	cfg IndexConfig
}

var _ dagidx.DagIndexer = (*Index)(nil)

// DefaultConfig returns default index config for tests
func DefaultConfig() IndexConfig {
	return IndexConfig{
		Caches: IndexCacheConfig{
			ForklessCause:     5000,
			HighestBeforeSeq:  1000,
			HighestBeforeTime: 1000,
			LowestAfterSeq:    1000,
		},
	}
}

// LiteConfig returns default index config for tests
func LiteConfig() IndexConfig {
	return IndexConfig{
		Caches: IndexCacheConfig{
			ForklessCause:     100,
			HighestBeforeSeq:  20,
			HighestBeforeTime: 20,
			LowestAfterSeq:    20,
		},
	}
}

// NewIndex creates Index instance.
func NewIndex(config IndexConfig, crit func(error)) *Index {
	vi := &Index{
		cfg:  config,
		crit: crit,
	}
	vi.cache.ForklessCause, _ = lru.New(vi.cfg.Caches.ForklessCause)
	vi.cache.HighestBeforeSeq, _ = lru.New(vi.cfg.Caches.HighestBeforeSeq)
	vi.cache.HighestBeforeTime, _ = lru.New(vi.cfg.Caches.HighestBeforeTime)
	vi.cache.LowestAfterSeq, _ = lru.New(vi.cfg.Caches.LowestAfterSeq)

	return vi
}

// Reset resets buffers.
func (vi *Index) Reset(validators *pos.Validators, db kvdb.Store, getEvent func(hash.Event) dag.Event) {
	// use wrapper to be able to drop failed events by dropping cache
	vi.getEvent = getEvent
	vi.vecDb = flushable.WrapWithDrop(db, func() {})
	vi.validators = validators.Copy()
	vi.validatorIdxs = validators.Idxs()
	vi.DropNotFlushed()
	vi.cache.ForklessCause.Purge()
	vi.dropDependentCaches()

	table.MigrateTables(&vi.table, vi.vecDb)
}

func (vi *Index) dropDependentCaches() {
	vi.cache.HighestBeforeSeq.Purge()
	vi.cache.HighestBeforeTime.Purge()
	vi.cache.LowestAfterSeq.Purge()
}

// Add calculates vector clocks for the event and saves into DB.
func (vi *Index) Add(e dag.Event) error {
	// sanity check
	if vi.getHighestBeforeSeq(e.ID()) != nil {
		return errors.New("event already exists")
	}
	vi.initBranchesInfo()
	_, err := vi.fillEventVectors(e)
	return err
}

// Flush writes vector clocks to persistent store.
func (vi *Index) Flush() {
	if vi.bi != nil {
		vi.setBranchesInfo(vi.bi)
	}
	if err := vi.vecDb.Flush(); err != nil {
		vi.crit(err)
	}
}

// DropNotFlushed not connected clocks. Call it if event has failed.
func (vi *Index) DropNotFlushed() {
	vi.bi = nil
	if vi.vecDb.NotFlushedPairs() != 0 {
		vi.vecDb.DropNotFlushed()
		vi.dropDependentCaches()
		// this cache is dependent, yet don't purge for performance reasons
		// instead, ensure that every incoming ID is unique
		//vi.cache.ForklessCause.Purge()
	}
}

func (vi *Index) fillGlobalBranchID(e dag.Event, meIdx idx.Validator) (idx.Validator, error) {
	// sanity checks
	if len(vi.bi.BranchIDCreatorIdxs) != len(vi.bi.BranchIDLastSeq) {
		return 0, errors.New("inconsistent BranchIDCreators len (inconsistent DB)")
	}
	if len(vi.bi.BranchIDCreatorIdxs) < vi.validators.Len() {
		return 0, errors.New("inconsistent BranchIDCreators len (inconsistent DB)")
	}

	if e.SelfParent() == nil {
		// is it first event indeed?
		if vi.bi.BranchIDLastSeq[meIdx] == 0 {
			// OK, not a new fork
			vi.bi.BranchIDLastSeq[meIdx] = e.Seq()
			return meIdx, nil
		}
	} else {
		selfParentBranchID := vi.getEventBranchID(*e.SelfParent())
		// sanity checks
		if len(vi.bi.BranchIDCreatorIdxs) != len(vi.bi.BranchIDLastSeq) {
			return 0, errors.New("inconsistent BranchIDCreators len (inconsistent DB)")
		}

		if vi.bi.BranchIDLastSeq[selfParentBranchID]+1 == e.Seq() {
			vi.bi.BranchIDLastSeq[selfParentBranchID] = e.Seq()
			// OK, not a new fork
			return selfParentBranchID, nil
		}
	}

	// if we're here, then new fork is observed (only globally), create new branchID due to a new fork
	vi.bi.BranchIDLastSeq = append(vi.bi.BranchIDLastSeq, e.Seq())
	vi.bi.BranchIDCreatorIdxs = append(vi.bi.BranchIDCreatorIdxs, meIdx)
	newBranchID := idx.Validator(len(vi.bi.BranchIDLastSeq) - 1)
	vi.bi.BranchIDByCreators[meIdx] = append(vi.bi.BranchIDByCreators[meIdx], newBranchID)
	return newBranchID, nil
}

func (vi *Index) setForkDetected(beforeSeq HighestBeforeSeq, branchID idx.Validator) {
	creatorIdx := vi.bi.BranchIDCreatorIdxs[branchID]
	for _, branchID := range vi.bi.BranchIDByCreators[creatorIdx] {
		beforeSeq.Set(idx.Validator(branchID), forkDetectedSeq)
	}
	// sanity check
	if !vi.atLeastOneFork() {
		vi.crit(errors.New("haven't written the correct branches info (inconsistent DB)"))
	}
}

// fillEventVectors calculates (and stores) event's vectors, and updates LowestAfter of newly-observed events.
func (vi *Index) fillEventVectors(e dag.Event) (allVecs, error) {
	meIdx := vi.validatorIdxs[e.Creator()]
	myVecs := allVecs{
		beforeSeq:  NewHighestBeforeSeq(len(vi.bi.BranchIDCreatorIdxs)),
		beforeTime: NewHighestBeforeTime(len(vi.bi.BranchIDCreatorIdxs)),
		after:      NewLowestAfterSeq(len(vi.bi.BranchIDCreatorIdxs)),
	}

	meBranchID, err := vi.fillGlobalBranchID(e, meIdx)

	// pre-load parents into RAM for quick access
	parentsVecs := make([]allVecs, len(e.Parents()))
	parentsBranchIDs := make([]idx.Validator, len(e.Parents()))
	for i, p := range e.Parents() {
		parentsBranchIDs[i] = vi.getEventBranchID(p)
		parentsVecs[i] = allVecs{
			beforeSeq:  vi.getHighestBeforeSeq(p),
			beforeTime: vi.getHighestBeforeTime(p),
			//after : vi.getLowestAfterSeq(p), not needed
		}
		if parentsVecs[i].beforeSeq == nil {
			return myVecs, fmt.Errorf("processed out of order, parent not found (inconsistent DB), parent=%s", p.String())
		}
	}

	// observed by himself
	myVecs.after.Set(meBranchID, e.Seq())
	myVecs.beforeSeq.Set(meBranchID, BranchSeq{seq: e.Seq(), minSeq: e.Seq()})
	myVecs.beforeTime.Set(meBranchID, e.RawTime())

	for _, pVec := range parentsVecs {
		// calculate HighestBefore vector. Detect forks for a case when parent observes a fork
		for branchID := idx.Validator(0); branchID < idx.Validator(len(vi.bi.BranchIDCreatorIdxs)); branchID++ {
			hisSeq := pVec.beforeSeq.get(branchID)
			if hisSeq.seq == 0 && !hisSeq.IsForkDetected() {
				// hisSeq doesn't observe anything about this branchID
				continue
			}
			mySeq := myVecs.beforeSeq.get(branchID)

			if mySeq.IsForkDetected() {
				// mySeq observes the maximum already
				continue
			}
			if hisSeq.IsForkDetected() {
				// set fork detected
				vi.setForkDetected(myVecs.beforeSeq, branchID)
			} else {
				if mySeq.seq == 0 || mySeq.minSeq > hisSeq.minSeq {
					// take hisSeq.MinSeq
					mySeq.minSeq = hisSeq.minSeq
					myVecs.beforeSeq.Set(branchID, mySeq)
				}
				if mySeq.seq < hisSeq.seq {
					// take hisSeq.Seq
					mySeq.seq = hisSeq.seq
					myVecs.beforeSeq.Set(branchID, mySeq)
					myVecs.beforeTime.Set(branchID, pVec.beforeTime.Get(branchID))
				}
			}
		}
	}
	// Detect forks, which were not observed by parents
	for n := idx.Validator(0); n < idx.Validator(vi.validators.Len()); n++ {
		if myVecs.beforeSeq.get(n).IsForkDetected() {
			// fork is already detected from the creator
			continue
		}
		for _, branchID1 := range vi.bi.BranchIDByCreators[n] {
			for _, branchID2 := range vi.bi.BranchIDByCreators[n] {
				if branchID1 == branchID2 {
					continue
				}

				a := myVecs.beforeSeq.get(branchID1)
				b := myVecs.beforeSeq.get(branchID2)

				if a.seq == 0 || b.seq == 0 {
					continue
				}
				if a.minSeq <= b.seq && b.minSeq <= a.seq {
					vi.setForkDetected(myVecs.beforeSeq, n)
					goto nextCreator
				}
			}
		}
	nextCreator:
	}

	// graph traversal starting from e, but excluding e
	onWalk := func(walk hash.Event) (godeeper bool) {
		wLowestAfterSeq := vi.getLowestAfterSeq(walk)

		godeeper = wLowestAfterSeq.Get(meBranchID) == 0
		if !godeeper {
			return
		}

		// update LowestAfter vector of the old event, because newly-connected event observes it
		wLowestAfterSeq.Set(meBranchID, e.Seq())
		vi.setLowestAfter(walk, wLowestAfterSeq)

		return
	}
	err = vi.dfsSubgraph(e, onWalk)
	if err != nil {
		vi.crit(err)
	}

	// store calculated vectors
	vi.setHighestBefore(e.ID(), myVecs.beforeSeq, myVecs.beforeTime)
	vi.setLowestAfter(e.ID(), myVecs.after)
	vi.setEventBranchID(e.ID(), meBranchID)

	return myVecs, nil
}

// GetHighestBeforeSeq returns HighestBefore vector clock without branches, where branches are merged into one
func (vi *Index) GetHighestBeforeSeq(id hash.Event) dagidx.HighestBeforeSeq {
	mergedSeq, _ := vi.getHighestBeforeAllBranchesTime(id)
	return mergedSeq
}

func (vi *Index) getHighestBeforeAllBranchesTime(id hash.Event) (HighestBeforeSeq, HighestBeforeTime) {
	vi.initBranchesInfo()

	if vi.atLeastOneFork() {
		beforeSeq := vi.getHighestBeforeSeq(id)
		times := vi.getHighestBeforeTime(id)
		mergedTimes := NewHighestBeforeTime(vi.validators.Len())
		mergedSeq := NewHighestBeforeSeq(vi.validators.Len())
		for creatorIdx, branches := range vi.bi.BranchIDByCreators {
			// read all branches to find highest event
			highestBranchSeq := BranchSeq{}
			highestBranchTime := dag.RawTimestamp(0)
			for _, branchID := range branches {
				branch := beforeSeq.get(branchID)
				if branch.IsForkDetected() {
					highestBranchSeq = branch
					break
				}
				if branch.seq > highestBranchSeq.seq {
					highestBranchSeq = branch
					highestBranchTime = times.Get(branchID)
				}
			}
			mergedTimes.Set(idx.Validator(creatorIdx), highestBranchTime)
			mergedSeq.Set(idx.Validator(creatorIdx), highestBranchSeq)
		}

		return mergedSeq, mergedTimes
	}
	return vi.getHighestBeforeSeq(id), vi.getHighestBeforeTime(id)
}
