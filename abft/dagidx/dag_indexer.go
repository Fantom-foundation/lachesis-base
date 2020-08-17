package dagidx

import (
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/kvdb"
)

type Seq interface {
	IsForkDetected() bool
	Seq() idx.Event
}

type HighestBeforeSeq interface {
	Size() int
	Get(i idx.Validator) Seq
}

type DagIndex interface {
	GetHighestBeforeSeq(id hash.Event) HighestBeforeSeq

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

	// NoCheaters excludes events which are observed by selfParents as cheaters.
	// Called by emitter to exclude cheater's events from potential parents list.
	NoCheaters(selfParent *hash.Event, options hash.Events) hash.Events
}

type DagIndexer interface {
	DagIndex

	Add(dag.Event) error
	Flush()
	DropNotFlushed()

	Reset(validators *pos.Validators, db kvdb.Store, getEvent func(hash.Event) dag.Event)
}
