package abft

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
)

var _ lachesis.Consensus = (*Lachesis)(nil)

// Lachesis performs events ordering and detects cheaters
// It's a wrapper around Orderer, which adds features which might potentially be application-specific:
// confirmed events traversal, DAG index updates and cheaters detection.
// Use this structure if need a general-purpose consensus. Instead, use lower-level abft.Orderer.
type Lachesis struct {
	*Orderer
	dagIndexer    dagidx.DagIndexer
	uniqueDirtyID uniqueID
	callback      lachesis.ConsensusCallbacks
}

// New creates Lachesis instance.
func NewLachesis(store *Store, input EventSource, dagIndexer dagidx.DagIndexer, crit func(error), config Config) *Lachesis {
	p := &Lachesis{
		Orderer:       NewOrderer(store, input, dagIndexer, crit, config),
		dagIndexer:    dagIndexer,
		uniqueDirtyID: uniqueID{new(big.Int)},
	}

	return p
}

// Build fills consensus-related fields: Frame, IsRoot
// returns error if event should be dropped
func (p *Lachesis) Build(e dag.MutableEvent) error {
	e.SetID(p.uniqueDirtyID.sample())
	defer p.dagIndexer.DropNotFlushed()
	err := p.dagIndexer.Add(e)
	if err != nil {
		return err
	}

	return p.Orderer.Build(e)
}

// ProcessEvent takes event into processing.
// Event order matter: parents first.
// All the event checkers must be launched.
// ProcessEvent is not safe for concurrent use.
func (p *Lachesis) ProcessEvent(e dag.Event) (err error) {
	defer p.dagIndexer.DropNotFlushed()
	err = p.dagIndexer.Add(e)
	if err != nil {
		return err
	}

	err = p.Orderer.ProcessEvent(e)
	if err == nil {
		p.dagIndexer.Flush()
	}
	return err
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
	atroposVecClock := p.dagIndexer.GetHighestBeforeSeq(atropos)

	validators := p.store.GetValidators()
	// cheaters are ordered deterministically
	cheaters := make([]idx.StakerID, 0, validators.Len())
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
	err := p.Orderer.Bootstrap(OrdererCallbacks{
		ApplyAtropos: p.applyAtropos,
		EpochDBLoaded: func() {
			p.dagIndexer.Reset(p.store.GetValidators(), p.store.epochTable.VectorIndex, p.input.GetEvent)
		},
	})
	if err != nil {
		return err
	}
	p.callback = callback
	return nil
}

type uniqueID struct {
	counter *big.Int
}

func (u *uniqueID) sample() [24]byte {
	u.counter = u.counter.Add(u.counter, common.Big1)
	var id [24]byte
	copy(id[:], u.counter.Bytes())
	return id
}
