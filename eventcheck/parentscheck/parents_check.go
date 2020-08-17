package parentscheck

import (
	"errors"

	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
)

var (
	ErrWrongSeq        = errors.New("event has wrong sequence time")
	ErrWrongLamport    = errors.New("event has wrong Lamport time")
	ErrWrongSelfParent = errors.New("event is missing self-parent")
)

// Checker performs checks, which require the parents list
type Checker struct{}

// New checker which performs checks, which require the parents list
func New() *Checker {
	return &Checker{}
}

// Validate event
func (v *Checker) Validate(e dag.Event, parents []dag.Event) error {
	if len(e.Parents()) != len(parents) {
		panic("parentscheck: expected event's parents as an argument")
	}

	// double parents are checked by basiccheck

	// lamport
	maxLamport := idx.Lamport(0)
	for _, p := range parents {
		maxLamport = idx.MaxLamport(maxLamport, p.Lamport())
	}
	if e.Lamport() != maxLamport+1 {
		return ErrWrongLamport
	}

	// self-parent
	for i, p := range parents {
		if (p.Creator() == e.Creator()) != e.IsSelfParent(e.Parents()[i]) {
			return ErrWrongSelfParent
		}
	}

	// seq
	if (e.Seq() <= 1) != (e.SelfParent() == nil) {
		return ErrWrongSeq
	}
	if e.SelfParent() != nil {
		selfParent := parents[0]
		if !e.IsSelfParent(selfParent.ID()) {
			// sanity check, self-parent is always first, it's how it's stored
			return ErrWrongSelfParent
		}
		if e.Seq() != selfParent.Seq()+1 {
			return ErrWrongSeq
		}
	}

	return nil
}
