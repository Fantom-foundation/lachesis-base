package vecengine

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

type Callbacks struct {
	GetHighestBefore func(hash.Event) HighestBeforeI
	GetLowestAfter   func(hash.Event) LowestAfterI
	SetHighestBefore func(hash.Event, HighestBeforeI)
	SetLowestAfter   func(hash.Event, LowestAfterI)
	NewHighestBefore func(idx.Validator) HighestBeforeI
	NewLowestAfter   func(idx.Validator) LowestAfterI
	OnDbReset        func(db kvdb.Store)
	OnDropNotFlushed func()
}

type Engine struct {
	crit          func(error)
	validators    *pos.Validators
	validatorIdxs map[idx.ValidatorID]idx.Validator

	bi *BranchesInfo

	getEvent func(hash.Event) dag.Event

	callback Callbacks

	vecDb kvdb.FlushableKVStore
	table struct {
		EventBranch  kvdb.Store `table:"b"`
		BranchesInfo kvdb.Store `table:"B"`
	}
}

// NewIndex creates Engine instance.
func NewIndex(crit func(error), callbacks Callbacks) *Engine {
	vi := &Engine{
		crit:     crit,
		callback: callbacks,
	}

	return vi
}

// Reset resets buffers.
func (vi *Engine) Reset(validators *pos.Validators, db kvdb.Store, getEvent func(hash.Event) dag.Event) {
	// use wrapper to be able to drop failed events by dropping cache
	vi.getEvent = getEvent
	vi.vecDb = flushable.WrapWithDrop(db, func() {})
	vi.validators = validators
	vi.validatorIdxs = validators.Idxs()
	vi.DropNotFlushed()

	table.MigrateTables(&vi.table, vi.vecDb)
	if vi.callback.OnDbReset != nil {
		vi.callback.OnDbReset(vi.vecDb)
	}
}

// Add calculates vector clocks for the event and saves into DB.
func (vi *Engine) Add(e dag.Event) error {
	vi.InitBranchesInfo()
	_, err := vi.fillEventVectors(e)
	return err
}

// Flush writes vector clocks to persistent store.
func (vi *Engine) Flush() {
	if vi.bi != nil {
		vi.setBranchesInfo(vi.bi)
	}
	if err := vi.vecDb.Flush(); err != nil {
		vi.crit(err)
	}
}

// DropNotFlushed not connected clocks. Call it if event has failed.
func (vi *Engine) DropNotFlushed() {
	vi.bi = nil
	if vi.vecDb.NotFlushedPairs() != 0 {
		vi.vecDb.DropNotFlushed()
		if vi.callback.OnDropNotFlushed != nil {
			vi.callback.OnDropNotFlushed()
		}
	}
}

func (vi *Engine) setForkDetected(before HighestBeforeI, branchID idx.Validator) {
	creatorIdx := vi.bi.BranchIDCreatorIdxs[branchID]
	for _, branchID := range vi.bi.BranchIDByCreators[creatorIdx] {
		before.SetForkDetected(branchID)
	}
}

func (vi *Engine) fillGlobalBranchID(e dag.Event, meIdx idx.Validator) (idx.Validator, error) {
	// sanity checks
	if len(vi.bi.BranchIDCreatorIdxs) != len(vi.bi.BranchIDLastSeq) {
		return 0, errors.New("inconsistent BranchIDCreators len (inconsistent DB)")
	}
	if idx.Validator(len(vi.bi.BranchIDCreatorIdxs)) < vi.validators.Len() {
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
		selfParentBranchID := vi.GetEventBranchID(*e.SelfParent())
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

// fillEventVectors calculates (and stores) event's vectors, and updates LowestAfter of newly-observed events.
func (vi *Engine) fillEventVectors(e dag.Event) (allVecs, error) {
	meIdx := vi.validatorIdxs[e.Creator()]
	myVecs := allVecs{
		before: vi.callback.NewHighestBefore(idx.Validator(len(vi.bi.BranchIDCreatorIdxs))),
		after:  vi.callback.NewLowestAfter(idx.Validator(len(vi.bi.BranchIDCreatorIdxs))),
	}

	meBranchID, err := vi.fillGlobalBranchID(e, meIdx)

	// pre-load parents into RAM for quick access
	parentsVecs := make([]HighestBeforeI, len(e.Parents()))
	parentsBranchIDs := make([]idx.Validator, len(e.Parents()))
	for i, p := range e.Parents() {
		parentsBranchIDs[i] = vi.GetEventBranchID(p)
		parentsVecs[i] = vi.callback.GetHighestBefore(p)
		if parentsVecs[i] == nil {
			return myVecs, fmt.Errorf("processed out of order, parent not found (inconsistent DB), parent=%s", p.String())
		}
	}

	// observed by himself
	myVecs.after.InitWithEvent(meBranchID, e)
	myVecs.before.InitWithEvent(meBranchID, e)

	for _, pVec := range parentsVecs {
		// calculate HighestBefore  Detect forks for a case when parent observes a fork
		myVecs.before.CollectFrom(pVec, idx.Validator(len(vi.bi.BranchIDCreatorIdxs)))
	}
	// Detect forks, which were not observed by parents
	if vi.AtLeastOneFork() {
		for n := idx.Validator(0); n < idx.Validator(vi.validators.Len()); n++ {
			if len(vi.bi.BranchIDByCreators[n]) <= 1 {
				continue
			}
			for _, branchID := range vi.bi.BranchIDByCreators[n] {
				if myVecs.before.IsForkDetected(branchID) {
					// if one branch observes a fork, mark all the branches as observing the fork
					vi.setForkDetected(myVecs.before, n)
					break
				}
			}
		}
		for n := idx.Validator(0); n < idx.Validator(vi.validators.Len()); n++ {
			if myVecs.before.IsForkDetected(n) {
				continue
			}
			for _, branchID1 := range vi.bi.BranchIDByCreators[n] {
				for _, branchID2 := range vi.bi.BranchIDByCreators[n] {
					a := branchID1
					b := branchID2
					if a == b {
						continue
					}

					if myVecs.before.IsEmpty(a) || myVecs.before.IsEmpty(b) {
						continue
					}
					if myVecs.before.MinSeq(a) <= myVecs.before.Seq(b) && myVecs.before.MinSeq(b) <= myVecs.before.Seq(a) {
						vi.setForkDetected(myVecs.before, n)
						goto nextCreator
					}
				}
			}
		nextCreator:
		}
	}

	// graph traversal starting from e, but excluding e
	onWalk := func(walk hash.Event) (godeeper bool) {
		wLowestAfterSeq := vi.callback.GetLowestAfter(walk)

		// update LowestAfter vector of the old event, because newly-connected event observes it
		if wLowestAfterSeq.Visit(meBranchID, e) {
			vi.callback.SetLowestAfter(walk, wLowestAfterSeq)
			return true
		}
		return false
	}
	err = vi.DfsSubgraph(e, onWalk)
	if err != nil {
		vi.crit(err)
	}

	// store calculated vectors
	vi.callback.SetHighestBefore(e.ID(), myVecs.before)
	vi.callback.SetLowestAfter(e.ID(), myVecs.after)
	vi.SetEventBranchID(e.ID(), meBranchID)

	return myVecs, nil
}

func (vi *Engine) GetMergedHighestBefore(id hash.Event) HighestBeforeI {
	vi.InitBranchesInfo()

	if vi.AtLeastOneFork() {
		scatteredBefore := vi.callback.GetHighestBefore(id)

		mergedBefore := vi.callback.NewHighestBefore(vi.validators.Len())

		for creatorIdx, branches := range vi.bi.BranchIDByCreators {
			mergedBefore.GatherFrom(idx.Validator(creatorIdx), scatteredBefore, branches)
		}

		return mergedBefore
	}
	return vi.callback.GetHighestBefore(id)
}
