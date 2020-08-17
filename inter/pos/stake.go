package pos

import (
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
)

type (
	// Stake amount.
	Stake uint64
)

type (
	// StakeCounterProvider providers stake counter.
	StakeCounterProvider func() *StakeCounter

	// StakeCounter counts stakes.
	StakeCounter struct {
		validators Validators
		already    []bool // idx.Validator -> bool

		quorum Stake
		sum    Stake
	}
)

// NewCounter constructor.
func (vv Validators) NewCounter() *StakeCounter {
	return newStakeCounter(vv)
}

func newStakeCounter(vv Validators) *StakeCounter {
	return &StakeCounter{
		validators: vv,
		quorum:     vv.Quorum(),
		already:    make([]bool, vv.Len()),
		sum:        0,
	}
}

// Count validator and return true if it hadn't counted before.
func (s *StakeCounter) Count(v idx.StakerID) bool {
	stakerIdx := s.validators.GetIdx(v)
	return s.CountByIdx(stakerIdx)
}

// CountByIdx validator and return true if it hadn't counted before.
func (s *StakeCounter) CountByIdx(stakerIdx idx.Validator) bool {
	if s.already[stakerIdx] {
		return false
	}
	s.already[stakerIdx] = true

	s.sum += s.validators.GetStakeByIdx(stakerIdx)
	return true
}

// HasQuorum achieved.
func (s *StakeCounter) HasQuorum() bool {
	return s.sum >= s.quorum
}

// Sum of counted stakes.
func (s *StakeCounter) Sum() Stake {
	return s.sum
}
