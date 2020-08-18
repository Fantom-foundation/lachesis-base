package abft

import (
	"math/big"

	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/abft/election"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
)

// Lachesis processes events to reach finality on their order.
type Lachesis struct {
	config Config
	crit   func(error)
	store  *Store
	input  EventSource

	election *election.Election
	vecClock dagidx.DagIndexer

	uniqueDirtyID uniqueID

	callback lachesis.ConsensusCallbacks
}

var _ lachesis.Consensus = (*Lachesis)(nil)

// New creates Lachesis instance.
func New(store *Store, input EventSource, vecClock dagidx.DagIndexer, crit func(error), config Config) *Lachesis {
	p := &Lachesis{
		config:        config,
		store:         store,
		input:         input,
		crit:          crit,
		vecClock:      vecClock,
		uniqueDirtyID: uniqueID{new(big.Int)},
	}

	return p
}
