package lachesis

import (
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
)

// Block is a part of an ordered chain of batches of events.
type Block struct {
	Index    idx.Block
	Atropos  hash.Event
	Events   hash.Events // Events order is undefined. Sort before using on-chain!
	Cheaters Cheaters
}
