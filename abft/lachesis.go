package abft

import (
	"math/big"

	"github.com/Fantom-foundation/go-lachesis/abft/dagidx"
	"github.com/Fantom-foundation/go-lachesis/abft/election"
	"github.com/Fantom-foundation/go-lachesis/lachesis"
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
func New(config Config, crit func(error), store *Store, input EventSource, vecClock dagidx.DagIndexer) *Lachesis {
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
