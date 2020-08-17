package ordering

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/dag/tdag"
)

func TestEventBuffer(t *testing.T) {
	nodes := tdag.GenNodes(5)

	var ordered []dag.Event
	r := rand.New(rand.NewSource(time.Now().Unix()))
	_ = tdag.ForEachRandEvent(nodes, 10, 3, r, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(1)
			e.SetRawTime(dag.RawTimestamp(e.Seq()))
			return nil
		},
	})

	checked := 0

	processed := make(map[hash.Event]dag.Event)
	buffer := New(len(nodes)*10, Callback{

		Process: func(e dag.Event) error {
			if _, ok := processed[e.ID()]; ok {
				t.Fatalf("%s already processed", e.String())
				return nil
			}
			for _, p := range e.Parents() {
				if _, ok := processed[p]; !ok {
					t.Fatalf("got %s before parent %s", e.String(), p.String())
					return nil
				}
			}
			processed[e.ID()] = e
			return nil
		},

		Drop: func(e dag.Event, peer string, err error) {
			t.Fatalf("%s unexpectedly dropped with %s", e.String(), err)
		},

		Exists: func(e hash.Event) bool {
			return processed[e] != nil
		},

		Get: func(e hash.Event) dag.Event {
			return processed[e]
		},

		Check: func(e dag.Event, parents []dag.Event) error {
			checked++
			if e.RawTime() != dag.RawTimestamp(e.Seq()) {
				return errors.New("malformed event time")
			}
			return nil
		},
	})

	for _, rnd := range rand.Perm(len(ordered)) {
		e := ordered[rnd]
		buffer.PushEvent(e, "")
	}

	// everything is processed
	for _, e := range ordered {
		if _, ok := processed[e.ID()]; !ok {
			t.Fatal("event wasn't processed")
		}
	}
	if checked != len(processed) {
		t.Fatal("not all the events were checked")
	}
}
