package tdag

import (
	"math/rand"
	"testing"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
)

func TestEventsByParents(t *testing.T) {
	nodes := GenNodes(5)
	events := GenRandEvents(nodes, 10, 3, nil)
	var ee dag.Events
	for _, e := range events {
		ee = append(ee, e...)
	}
	// shuffle
	unordered := make(dag.Events, len(ee))
	for i, j := range rand.Perm(len(ee)) {
		unordered[i] = ee[j]
	}

	ordered := ByParents(unordered)
	position := make(map[hash.Event]int)
	for i, e := range ordered {
		position[e.ID()] = i
	}

	for i, e := range ordered {
		for _, p := range e.Parents() {
			pos, ok := position[p]
			if !ok {
				continue
			}
			if pos > i {
				t.Fatalf("parent %s is not before %s", p.String(), e.ID().String())
				return
			}
		}
	}
}
