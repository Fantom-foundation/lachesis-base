package abft

import (
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

const esKey = "e"

// SetEpochState stores epoch.
func (s *Store) SetEpochState(e *EpochState) {
	s.cache.EpochState = e
	s.setEpochState([]byte(esKey), e)
}

// GetEpochState returns stored epoch.
func (s *Store) GetEpochState() *EpochState {
	if s.cache.EpochState != nil {
		return s.cache.EpochState
	}
	e := s.getEpochState([]byte(esKey))
	if e == nil {
		s.crit(ErrNoGenesis)
	}
	s.cache.EpochState = e
	return e
}

func (s *Store) setEpochState(key []byte, e *EpochState) {
	s.set(s.table.EpochState, key, e)
}

func (s *Store) getEpochState(key []byte) *EpochState {
	w, exists := s.get(s.table.EpochState, key, &EpochState{}).(*EpochState)
	if !exists {
		return nil
	}
	return w
}

// GetEpoch returns current epoch
func (s *Store) GetEpoch() idx.Epoch {
	return s.GetEpochState().Epoch
}

// GetValidators returns current validators
func (s *Store) GetValidators() *pos.Validators {
	return s.GetEpochState().Validators
}
