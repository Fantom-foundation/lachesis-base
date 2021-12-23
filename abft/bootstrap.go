package abft

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/abft/election"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

const (
	FirstFrame = idx.Frame(1)
	FirstEpoch = idx.Epoch(1)
)

// LastDecidedState is for persistent storing.
type LastDecidedState struct {
	// fields can change only after a frame is decided
	LastDecidedFrame idx.Frame
}

type EpochState struct {
	// stored values
	// these values change only after a change of epoch
	Epoch      idx.Epoch
	Validators *pos.Validators
}

func (es EpochState) String() string {
	return fmt.Sprintf("%d/%s", es.Epoch, es.Validators.String())
}

// Bootstrap restores abft's state from store.
func (p *Orderer) Bootstrap(callback OrdererCallbacks) error {
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
	if p.callback.EpochDBLoaded != nil {
		p.callback.EpochDBLoaded(p.store.GetEpoch())
	}
	p.election = election.New(p.store.GetValidators(), p.store.GetLastDecidedFrame()+1, p.dagIndex.ForklessCause, p.store.GetFrameRoots)

	// events reprocessing
	_, err = p.bootstrapElection()
	return err
}

// Reset switches epoch state to a new empty epoch.
func (p *Orderer) Reset(epoch idx.Epoch, validators *pos.Validators) error {
	p.store.applyGenesis(epoch, validators)
	// reset internal epoch DB
	err := p.resetEpochStore(epoch)
	if err != nil {
		return err
	}
	p.election.Reset(validators, FirstFrame)
	return nil
}

func (p *Orderer) loadEpochDB() error {
	return p.store.openEpochDB(p.store.GetEpoch())
}
