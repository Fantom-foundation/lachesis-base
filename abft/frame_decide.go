package abft

import (
	"fmt"
	"math"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/lachesis"
)

func (p *Lachesis) confirmEvents(frame idx.Frame, atropos hash.Event, onEventConfirmed func(dag.Event) error) error {
	var errInside error
	err := p.dfsSubgraph(atropos, func(header dag.Event) bool {
		decidedFrame := p.store.GetEventConfirmedOn(header.ID())
		if decidedFrame != 0 {
			return false
		}
		// mark all the walked events as confirmed
		p.store.SetEventConfirmedOn(header.ID(), frame)
		if onEventConfirmed != nil {
			errInside = onEventConfirmed(header)
			if errInside != nil {
				return false
			}
		}
		return true
	})
	if errInside != nil {
		return errInside
	}
	return err
}

func (p *Lachesis) confirmBlock(frame idx.Frame, atropos hash.Event) (block *lachesis.Block, err error) {
	validators := p.store.GetValidators()
	blockEventIDs := make(hash.Events, 0, 20*validators.Len())

	atroposHighestBefore := p.vecClock.GetHighestBeforeSeq(atropos)
	var highestLamport idx.Lamport
	var lowestLamport idx.Lamport

	// cheaters are ordered deterministically
	cheaters := make([]idx.StakerID, 0, validators.Len())
	for creatorIdx, creator := range validators.SortedIDs() {
		if atroposHighestBefore.Get(idx.Validator(creatorIdx)).IsForkDetected() {
			cheaters = append(cheaters, creator)
		}
	}

	err = p.confirmEvents(frame, atropos, func(confirmedEvent dag.Event) error {
		// track highest and lowest Lamports
		if highestLamport == 0 || highestLamport < confirmedEvent.Lamport() {
			highestLamport = confirmedEvent.Lamport()
		}
		if lowestLamport == 0 || lowestLamport > confirmedEvent.Lamport() {
			lowestLamport = confirmedEvent.Lamport()
		}

		// but not all the events are included into a block
		creatorHighest := atroposHighestBefore.Get(validators.GetIdx(confirmedEvent.Creator()))
		fromCheater := creatorHighest.IsForkDetected()
		// seqDepth is the depth in of this event in "chain" of self-parents of this creator
		seqDepth := creatorHighest.Seq() - confirmedEvent.Seq()
		if creatorHighest.Seq() < confirmedEvent.Seq() {
			seqDepth = math.MaxInt32
		}
		allowed := p.callback.IsEventAllowedIntoBlock == nil || p.callback.IsEventAllowedIntoBlock(confirmedEvent, seqDepth)
		// block consists of allowed events from non-cheaters
		if !fromCheater && allowed {
			blockEventIDs = append(blockEventIDs, confirmedEvent.ID())
		}
		// sanity check
		if !fromCheater && confirmedEvent.Seq() > creatorHighest.Seq() {
			return fmt.Errorf("DAG is inconsistent with vector clock, seq=%d, highest=%d", confirmedEvent.Seq(), creatorHighest.Seq())
		}

		if p.callback.OnEventConfirmed != nil {
			p.callback.OnEventConfirmed(confirmedEvent, seqDepth)
		}
		return nil
	})
	if err != nil {
		return
	}

	// block building
	lastDecidedState := p.store.GetLastDecidedState()
	return &lachesis.Block{
		Index:    lastDecidedState.LastBlockN + 1,
		Atropos:  atropos,
		Events:   blockEventIDs, // unordered!
		Cheaters: cheaters,
	}, nil
}

// onFrameDecided moves LastDecidedFrameN to frame.
// It includes: moving current decided frame, txs ordering and execution, epoch sealing.
func (p *Lachesis) onFrameDecided(frame idx.Frame, atropos hash.Event) (bool, error) {
	p.election.Reset(p.store.GetValidators(), frame+1)

	block, err := p.confirmBlock(frame, atropos)
	if err != nil {
		return false, err
	}

	// new checkpoint
	var newValidators *pos.Validators
	if p.callback.ApplyBlock != nil {
		newValidators = p.callback.ApplyBlock(block)
	}

	lastDecidedState := *p.store.GetLastDecidedState()
	lastDecidedState.LastBlockN++
	lastDecidedState.LastAtropos = atropos
	if newValidators != nil {
		lastDecidedState.LastDecidedFrame = firstFrame - 1
		err := p.sealEpoch(newValidators)
		if err != nil {
			return true, err
		}
	} else {
		lastDecidedState.LastDecidedFrame = frame
	}
	p.store.SetLastDecidedState(&lastDecidedState)
	return newValidators != nil, nil
}

func (p *Lachesis) sealEpoch(newValidators *pos.Validators) error {
	// new PrevEpoch state
	epochState := *p.store.GetEpochState()
	epochState.Epoch++
	epochState.Validators = newValidators
	p.store.SetEpochState(&epochState)

	// reset internal epoch DB
	err := p.store.dropEpochDB()
	if err != nil {
		return err
	}
	err = p.store.openEpochDB(epochState.Epoch)
	if err != nil {
		return err
	}

	// reset election & vectorindex to new epoch db
	p.vecClock.Reset(newValidators, p.store.epochTable.VectorIndex, p.input.GetEvent)
	p.election.Reset(newValidators, firstFrame)
	return nil
}
