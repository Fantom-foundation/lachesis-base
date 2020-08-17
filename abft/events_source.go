package abft

import (
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
)

// EventSource is a callback for getting events from an external storage.
type EventSource interface {
	HasEvent(hash.Event) bool
	GetEvent(hash.Event) dag.Event
}
