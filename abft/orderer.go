package abft

import (
	"github.com/Fantom-foundation/lachesis-base/abft/election"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

type OrdererCallbacks struct {
	ApplyAtropos func(decidedFrame idx.Frame, atropos hash.Event) (sealEpoch *pos.Validators)

	EpochDBLoaded func()
}

type OrdererDagIndex interface {
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

// Unlike processes events to reach finality on their order.
// Unlike abft.Lachesis, this raw level of abstraction doesn't track cheaters detection
type Orderer struct {
	config Config
	crit   func(error)
	store  *Store
	input  EventSource

	election *election.Election
	dagIndex OrdererDagIndex

	callback OrdererCallbacks
}

// New creates Orderer instance.
// Unlike Lachesis, Orderer doesn't updates DAG indexes for events, and doesn't detect cheaters
// It has only one purpose - reaching consensus on events order.
func NewOrderer(store *Store, input EventSource, dagIndex OrdererDagIndex, crit func(error), config Config) *Orderer {
	p := &Orderer{
		config:   config,
		store:    store,
		input:    input,
		crit:     crit,
		dagIndex: dagIndex,
	}

	return p
}
