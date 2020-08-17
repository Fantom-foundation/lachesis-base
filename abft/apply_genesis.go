package abft

import (
	"fmt"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

// GenesisState stores state of previous Epoch
type Genesis struct {
	Validators *pos.Validators
	Atropos    hash.Event
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

	es := &EpochState{}
	ds := &LastDecidedState{}

	es.Validators = g.Validators
	es.Epoch = firstEpoch
	ds.LastAtropos = g.Atropos
	ds.LastDecidedFrame = firstFrame - 1
	ds.LastBlockN = 0

	s.SetEpochState(es)
	s.SetLastDecidedState(ds)

	return nil
}
