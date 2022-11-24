package abft

import (
	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
)

var _ lachesis.Consensus = (*Lachesis)(nil)

type DagIndex interface {
	dagidx.VectorClock
	dagidx.ForklessCause
}

// Lachesis performs events ordering and detects cheaters
// It's a wrapper around Orderer, which adds features which might potentially be application-specific:
// confirmed events traversal, cheaters detection.
// Use this structure if need a general-purpose consensus. Instead, use lower-level abft.Orderer.
type Lachesis struct {
	*Orderer
	dagIndex      DagIndex
	uniqueDirtyID uniqueID
	callback      lachesis.ConsensusCallbacks
}

// NewLachesis creates Lachesis instance.
func NewLachesis(store *Store, input EventSource, dagIndex DagIndex, crit func(error), config Config) *Lachesis {
	p := &Lachesis{
		Orderer:  NewOrderer(store, input, dagIndex, crit, config),
		dagIndex: dagIndex,
	}

	return p
}

func (p *Lachesis) confirmEvents(frame idx.Frame, atropos hash.Event, onEventConfirmed func(dag.Event)) error {
	err := p.dfsSubgraph(atropos, func(e dag.Event) bool {
		decidedFrame := p.store.GetEventConfirmedOn(e.ID())
		if decidedFrame != 0 {
			return false
		}
		// mark all the walked events as confirmed
		p.store.SetEventConfirmedOn(e.ID(), frame)
		if onEventConfirmed != nil {
			onEventConfirmed(e)
		}
		return true
	})
	return err
}

func (p *Lachesis) applyAtropos(decidedFrame idx.Frame, atropos hash.Event) *pos.Validators {
	atroposVecClock := p.dagIndex.GetMergedHighestBefore(atropos)

	validators := p.store.GetValidators()
	// cheaters are ordered deterministically
	cheaters := make([]idx.ValidatorID, 0, validators.Len())
	for creatorIdx, creator := range validators.SortedIDs() {
		if atroposVecClock.Get(idx.Validator(creatorIdx)).IsForkDetected() {
			cheaters = append(cheaters, creator)
		}
	}

	if p.callback.BeginBlock == nil {
		return nil
	}
	blockCallback := p.callback.BeginBlock(&lachesis.Block{
		Atropos:  atropos,
		Cheaters: cheaters,
	})

	// traverse newly confirmed events
	err := p.confirmEvents(decidedFrame, atropos, blockCallback.ApplyEvent)
	if err != nil {
		p.crit(err)
	}

	if blockCallback.EndBlock != nil {
		return blockCallback.EndBlock()
	}
	return nil
}

func (p *Lachesis) Bootstrap(callback lachesis.ConsensusCallbacks) error {
	return p.BootstrapWithOrderer(callback, p.OrdererCallbacks())
}

func (p *Lachesis) BootstrapWithOrderer(callback lachesis.ConsensusCallbacks, ordererCallbacks OrdererCallbacks) error {
	err := p.Orderer.Bootstrap(ordererCallbacks)
	if err != nil {
		return err
	}
	p.callback = callback
	return nil
}

func (p *Lachesis) OrdererCallbacks() OrdererCallbacks {
	return OrdererCallbacks{
		ApplyAtropos: p.applyAtropos,
	}
}
