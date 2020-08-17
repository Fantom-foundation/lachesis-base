package abft

import (
	"bytes"
	"fmt"

	"github.com/Fantom-foundation/go-lachesis/abft/election"
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
)

func rootRecordKey(r *election.RootAndSlot) []byte {
	key := bytes.Buffer{}
	key.Write(r.Slot.Frame.Bytes())
	key.Write(r.Slot.Validator.Bytes())
	key.Write(r.ID.Bytes())
	return key.Bytes()
}

// AddRoot stores the new root
// Not safe for concurrent use due to complex mutable cache!
func (s *Store) AddRoot(root dag.Event) {
	r := election.RootAndSlot{
		Slot: election.Slot{
			Frame:     root.Frame(),
			Validator: root.Creator(),
		},
		ID: root.ID(),
	}

	if err := s.epochTable.Roots.Put(rootRecordKey(&r), []byte{}); err != nil {
		s.crit(err)
	}

	// Add to cache.
	if s.cache.FrameRoots != nil {
		if c, ok := s.cache.FrameRoots.Get(root.Frame()); ok {
			if rr, ok := c.([]election.RootAndSlot); ok {
				s.cache.FrameRoots.Add(root.Frame(), append(rr, r))
			}
		}
	}
}

const (
	frameSize    = 4
	stakerIDSize = 4
	eventIDSize  = 32
)

// GetFrameRoots returns all the roots in the specified frame
// Not safe for concurrent use due to complex mutable cache!
func (s *Store) GetFrameRoots(f idx.Frame) []election.RootAndSlot {
	// get data from LRU cache first.
	if s.cache.FrameRoots != nil {
		if c, ok := s.cache.FrameRoots.Get(f); ok {
			if rr, ok := c.([]election.RootAndSlot); ok {
				return rr
			}
		}
	}
	rr := make([]election.RootAndSlot, 0, 100)

	it := s.epochTable.Roots.NewIteratorWithPrefix(f.Bytes())
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) != frameSize+stakerIDSize+eventIDSize {
			s.crit(fmt.Errorf("roots table: incorrect key len=%d", len(key)))
		}
		r := election.RootAndSlot{
			Slot: election.Slot{
				Frame:     idx.BytesToFrame(key[:frameSize]),
				Validator: idx.BytesToStakerID(key[frameSize : frameSize+stakerIDSize]),
			},
			ID: hash.BytesToEvent(key[frameSize+stakerIDSize:]),
		}
		if r.Slot.Frame != f {
			s.crit(fmt.Errorf("roots table: invalid frame=%d, expected=%d", r.Slot.Frame, f))
		}

		rr = append(rr, r)
	}
	if it.Error() != nil {
		s.crit(it.Error())
	}

	// Add to cache.
	if s.cache.FrameRoots != nil {
		s.cache.FrameRoots.Add(f, rr)
	}

	return rr
}
