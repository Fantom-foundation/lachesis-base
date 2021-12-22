package lachesis

import (
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

// Consensus is a consensus interface.
type Consensus interface {
	// Process takes event for processing.
	Process(e dag.Event) error
	// Build sets consensus fields. Returns an error if event should be dropped.
	Build(e dag.MutableEvent) error
	// Reset switches epoch state to a new empty epoch.
	Reset(epoch idx.Epoch, validators *pos.Validators) error
}

type ApplyEventFn func(event dag.Event)
type EndBlockFn func() (sealEpoch *pos.Validators)

type BlockCallbacks struct {
	// ApplyEvent is called on confirmation of each event during block processing.
	// Cannot be called twice for the same event.
	// The order in which ApplyBlock is called for events is deterministic but undefined. It's application's responsibility to sort events according to its needs.
	// It's application's responsibility to interpret this data (e.g. events may be related to batches of transactions or other ordered data).
	ApplyEvent ApplyEventFn
	// EndBlock indicates that ApplyEvent was called for all the events
	// Returns validators group for a new epoch, if epoch must be sealed after this bock
	// If epoch must not get sealed, then this callback must return nil
	EndBlock EndBlockFn
}

type BeginBlockFn func(block *Block) BlockCallbacks

// ConsensusCallbacks contains callbacks called during block processing by consensus engine
type ConsensusCallbacks struct {
	// BeginBlock returns further callbacks for processing of this block
	BeginBlock BeginBlockFn
}
