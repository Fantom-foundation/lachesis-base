package lachesis

import (
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

// Consensus is a consensus interface.
type Consensus interface {
	// PushEvent takes event for processing.
	ProcessEvent(e dag.Event) error
	// Build sets consensus fields. Returns an error if event should be dropped.
	Build(e dag.MutableEvent) error
}

// ConsensusCallbacks contains callbacks called during block processing by consensus engine
type ConsensusCallbacks struct {
	// ApplyBlock is callback type to apply the new block to the state
	// sealEpoch is nil if epoch shouldn't get sealed
	ApplyBlock func(block *Block) (sealEpoch *pos.Validators)
	// OnEventConfirmed is callback type to notify about event confirmation
	OnEventConfirmed func(event dag.Event, seqDepth idx.Event)
	// IsEventAllowedIntoBlock is callback type to check is event allowed be part of a block or not
	IsEventAllowedIntoBlock func(event dag.Event, seqDepth idx.Event) bool
}
