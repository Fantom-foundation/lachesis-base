package abft

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

// Genesis stores genesis state
type Genesis struct {
	Epoch      idx.Epoch
	Validators *pos.Validators
}

// ApplyGenesis writes initial state.
func (s *Store) ApplyGenesis(g *Genesis) error {
	if g == nil {
		return fmt.Errorf("genesis config shouldn't be nil")
	}
	if g.Validators.Len() == 0 {
		return fmt.Errorf("genesis validators shouldn't be empty")
	}
	if ok, _ := s.table.LastDecidedState.Has([]byte(dsKey)); ok {
		return fmt.Errorf("genesis already applied")
	}

	s.applyGenesis(g.Epoch, g.Validators)
	return nil
}

// applyGenesis switches epoch state to a new empty epoch.
func (s *Store) applyGenesis(epoch idx.Epoch, validators *pos.Validators) {
	es := &EpochState{}
	ds := &LastDecidedState{}

	es.Validators = validators
	es.Epoch = epoch
	ds.LastDecidedFrame = FirstFrame - 1

	s.SetEpochState(es)
	s.SetLastDecidedState(ds)

}
