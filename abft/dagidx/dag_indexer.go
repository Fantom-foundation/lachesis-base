package dagidx

import (
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

type Seq interface {
	Seq() idx.Event
	IsForkDetected() bool
}

type HighestBeforeSeq interface {
	Size() int
	Get(i idx.Validator) Seq
}

type ForklessCause interface {
	// ForklessCause calculates "sufficient coherence" between the events.
	// The A.HighestBefore array remembers the sequence number of the last
	// event by each validator that is an ancestor of A. The array for
	// B.LowestAfter remembers the sequence number of the earliest
	// event by each validator that is a descendant of B. Compare the two arrays,
	// and find how many elements in the A.HighestBefore array are greater
	// than or equal to the corresponding element of the B.LowestAfter
	// array. If there are more than 2n/3 such matches, then the A and B
	// have achieved sufficient coherency.
	//
	// If B1 and B2 are forks, then they cannot BOTH forkless-cause any specific event A,
	// unless more than 1/3W are Byzantine.
	// This great property is the reason why this function exists,
	// providing the base for the BFT algorithm.
	ForklessCause(aID, bID hash.Event) bool
}

type VectorClock interface {
	GetMergedHighestBefore(id hash.Event) HighestBeforeSeq
}
