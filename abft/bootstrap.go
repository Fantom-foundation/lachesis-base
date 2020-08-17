package abft

import (
	"errors"

	"github.com/Fantom-foundation/go-lachesis/abft/election"
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/lachesis"
)

const (
	firstFrame = idx.Frame(1)
	firstEpoch = idx.Epoch(1)
)

// LastDecidedState is for persistent storing.
type LastDecidedState struct {
	// fields can change only after a frame is decided
	LastDecidedFrame idx.Frame
	LastBlockN       idx.Block
	LastAtropos      hash.Event
}

type EpochState struct {
	// stored values
	// these values change only after a change of epoch
	Epoch      idx.Epoch
	Validators *pos.Validators
}

// Bootstrap restores abft's state from store.
func (p *Lachesis) Bootstrap(callback lachesis.ConsensusCallbacks) error {
	if p.election != nil {
		return errors.New("already bootstrapped")
	}
	// block handler must be set before p.handleElection
	p.callback = callback

	// restore current epoch DB
	err := p.loadEpochDB()
	if err != nil {
		return err
	}
	p.vecClock.Reset(p.store.GetValidators(), p.store.epochTable.VectorIndex, p.input.GetEvent)
	p.election = election.New(p.store.GetValidators(), p.store.GetLastDecidedFrame()+1, p.vecClock.ForklessCause, p.store.GetFrameRoots)

	// events reprocessing
	return p.handleElection(nil)
}

func (p *Lachesis) loadEpochDB() error {
	return p.store.openEpochDB(p.store.GetEpoch())
}
