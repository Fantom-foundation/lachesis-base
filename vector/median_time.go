package vector

import (
	"fmt"
	"sort"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

// medianTimeIndex is a handy index for the MedianTime() func
type medianTimeIndex struct {
	weight      pos.Weight
	claimedTime dag.RawTimestamp
}

// MedianTime calculates weighted median of claimed time within highest observed events.
func (vi *Index) MedianTime(id hash.Event, defaultTime dag.RawTimestamp) dag.RawTimestamp {
	vi.initBranchesInfo()
	// Get event by hash
	beforeSeq, times := vi.getHighestBeforeAllBranchesTime(id)
	if beforeSeq == nil || times == nil {
		vi.crit(fmt.Errorf("event=%s not found", id.String()))
	}

	honestTotalWeight := pos.Weight(0) // isn't equal to validators.TotalWeight(), because doesn't count cheaters
	highests := make([]medianTimeIndex, 0, len(vi.validatorIdxs))
	// convert []HighestBefore -> []medianTimeIndex
	for creatorIdxI := range vi.validators.IDs() {
		creatorIdx := idx.Validator(creatorIdxI)
		highest := medianTimeIndex{}
		highest.weight = vi.validators.GetWeightByIdx(creatorIdx)
		highest.claimedTime = times.Get(creatorIdx)
		seq := beforeSeq.Get(creatorIdx)

		// edge cases
		if seq.IsForkDetected() {
			// cheaters don't influence medianTime
			highest.weight = 0
		} else if seq.Seq() == 0 {
			// if no event was observed from this node, then use genesisTime
			highest.claimedTime = defaultTime
		}

		highests = append(highests, highest)
		honestTotalWeight += highest.weight
	}
	// it's technically possible honestTotalWeight == 0 (all validators are cheaters)

	// sort by claimed time (partial order is enough here, because we need only claimedTime)
	sort.Slice(highests, func(i, j int) bool {
		a, b := highests[i], highests[j]
		return a.claimedTime < b.claimedTime
	})

	// Calculate weighted median
	halfWeight := honestTotalWeight / 2
	var currWeight pos.Weight
	var median dag.RawTimestamp
	for _, highest := range highests {
		currWeight += highest.weight
		if currWeight >= halfWeight {
			median = highest.claimedTime
			break
		}
	}

	// sanity check
	if currWeight < halfWeight || currWeight > honestTotalWeight {
		vi.crit(fmt.Errorf("median wasn't calculated correctly, median=%d, currWeight=%d, totalWeight=%d, len(highests)=%d, id=%s",
			median,
			currWeight,
			honestTotalWeight,
			len(highests),
			id.String(),
		))
	}

	return median
}
