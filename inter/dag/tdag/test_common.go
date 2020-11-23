package tdag

import (
	"crypto/sha256"
	"fmt"
	"math/rand"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

// GenNodes generates nodes.
// Result:
//   - nodes  is an array of node addresses;
func GenNodes(
	nodeCount int,
) (
	nodes []idx.ValidatorID,
) {
	// init results
	nodes = make([]idx.ValidatorID, nodeCount)
	// make and name nodes
	for i := 0; i < nodeCount; i++ {
		addr := hash.FakePeer()
		nodes[i] = addr
		hash.SetNodeName(addr, "node"+string('A'+rune(i)))
	}

	return
}

// ForEachRandFork generates random events with forks for test purpose.
// Result:
//   - callbacks are called for each new event;
//   - events maps node address to array of its events;
func ForEachRandFork(
	nodes []idx.ValidatorID,
	cheatersArr []idx.ValidatorID,
	eventCount int,
	parentCount int,
	forksCount int,
	r *rand.Rand,
	callback ForEachEvent,
) (
	events map[idx.ValidatorID]dag.Events,
) {
	if r == nil {
		// fixed seed
		r = rand.New(rand.NewSource(0))
	}
	// init results
	nodeCount := len(nodes)
	events = make(map[idx.ValidatorID]dag.Events, nodeCount)
	cheaters := map[idx.ValidatorID]int{}
	for _, cheater := range cheatersArr {
		cheaters[cheater] = 0
	}

	// make events
	for i := 0; i < nodeCount*eventCount; i++ {
		// seq parent
		self := i % nodeCount
		creator := nodes[self]
		parents := r.Perm(nodeCount)
		for j, n := range parents {
			if n == self {
				parents = append(parents[0:j], parents[j+1:]...)
				break
			}
		}
		parents = parents[:parentCount-1]
		// make
		e := &TestEvent{}
		e.SetCreator(creator)
		e.SetParents(hash.Events{})
		// first parent is a last creator's event or empty hash
		var parent dag.Event
		if ee := events[creator]; len(ee) > 0 {
			parent = ee[len(ee)-1]

			// may insert fork
			forksAlready, isCheater := cheaters[creator]
			forkPossible := len(ee) > 1
			forkLimitOk := forksAlready < forksCount
			forkFlipped := r.Intn(eventCount) <= forksCount || i < (nodeCount-1)*eventCount
			if isCheater && forkPossible && forkLimitOk && forkFlipped {
				parent = ee[r.Intn(len(ee)-1)]
				if r.Intn(len(ee)) == 0 {
					parent = nil
				}
				cheaters[creator]++
			}
		}
		if parent == nil {
			e.SetSeq(1)
			e.SetLamport(1)
		} else {
			e.SetSeq(parent.Seq() + 1)
			e.AddParent(parent.ID())
			e.SetLamport(parent.Lamport() + 1)
		}
		// other parents are the lasts other's events
		for _, other := range parents {
			if ee := events[nodes[other]]; len(ee) > 0 {
				parent := ee[len(ee)-1]
				e.AddParent(parent.ID())
				if e.Lamport() <= parent.Lamport() {
					e.SetLamport(parent.Lamport() + 1)
				}
			}
		}
		e.Name = fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator]))
		// buildEvent callback
		if callback.Build != nil {
			err := callback.Build(e, e.Name)
			if err != nil {
				continue
			}
		}
		// save and name event
		hasher := sha256.New()
		hasher.Write(e.Bytes())
		var id [24]byte
		copy(id[:], hasher.Sum(nil)[:24])
		e.SetID(id)
		hash.SetEventName(e.ID(), fmt.Sprintf("%s%03d", string('a'+rune(self)), len(events[creator])))
		events[creator] = append(events[creator], e)
		// callback
		if callback.Process != nil {
			callback.Process(e, e.Name)
		}
	}

	return
}

// ForEachRandEvent generates random events for test purpose.
// Result:
//   - callbacks are called for each new event;
//   - events maps node address to array of its events;
func ForEachRandEvent(
	nodes []idx.ValidatorID,
	eventCount int,
	parentCount int,
	r *rand.Rand,
	callback ForEachEvent,
) (
	events map[idx.ValidatorID]dag.Events,
) {
	return ForEachRandFork(nodes, []idx.ValidatorID{}, eventCount, parentCount, 0, r, callback)
}

// GenRandEvents generates random events for test purpose.
// Result:
//   - events maps node address to array of its events;
func GenRandEvents(
	nodes []idx.ValidatorID,
	eventCount int,
	parentCount int,
	r *rand.Rand,
) (
	events map[idx.ValidatorID]dag.Events,
) {
	return ForEachRandEvent(nodes, eventCount, parentCount, r, ForEachEvent{})
}

func delPeerIndex(events map[idx.ValidatorID]dag.Events) (res dag.Events) {
	for _, ee := range events {
		res = append(res, ee...)
	}
	return
}
