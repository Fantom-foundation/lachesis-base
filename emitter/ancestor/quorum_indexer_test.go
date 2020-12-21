package ancestor

import (
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/utils"
	"github.com/Fantom-foundation/lachesis-base/utils/adapters"
	"github.com/Fantom-foundation/lachesis-base/vecfc"
)

func TestCasualityStrategy(t *testing.T) {
	testSpecialNamedParents(t, `
a1.1   b1.2   c1.2   d1.2   e1.2
║      ║      ║      ║      ║
║      ╠──────╫───── d2.2   ║
║      ║      ║      ║      ║
║      b2.3 ──╫──────╣      e2.3
║      ║      ║      ║      ║
║      ╠──────╫───── d3.3   ║
a2.3 ──╣      ║      ║      ║
║      ║      ║      ║      ║
║      b3.4 ──╣      ║      ║
║      ║      ║      ║      ║
║      ╠──────╫───── d4.4   ║
║      ║      ║      ║      ║
║      ╠───── c2.4   ║      e3.4
║      ║      ║      ║      ║
`, map[int]map[string]string{
		0: {
			"nodeA": "[]",
			"nodeB": "[]",
			"nodeC": "[]",
			"nodeD": "[]",
			"nodeE": "[]",
		},
		1: {
			"nodeA": "[a1.1]",
			"nodeB": "[a1.1]",
			"nodeC": "[a1.1]",
			"nodeD": "[a1.1]",
			"nodeE": "[a1.1]",
		},
		2: {
			"nodeA": "[a1.1, d2.2, e1.2]",
			"nodeB": "[b1.2, d2.2, e1.2]",
			"nodeC": "[c1.2, d2.2, e1.2]",
			"nodeD": "[d2.2, c1.2, e1.2]",
			"nodeE": "[e1.2, c1.2, d2.2]",
		},
		3: {
			"nodeA": "[a2.3, c1.2, e2.3]",
			"nodeB": "[b2.3, a2.3, e2.3]",
			"nodeC": "[c1.2, a2.3, d3.3]",
			"nodeD": "[d3.3, a2.3, e2.3]",
			"nodeE": "[e2.3, a2.3, d3.3]",
		},
		4: {
			"nodeA": "[a2.3, c2.4, d4.4]",
			"nodeB": "[b3.4, d4.4, e3.4]",
			"nodeC": "[c2.4, d4.4, e3.4]",
			"nodeD": "[d4.4, a2.3, e3.4]",
			"nodeE": "[e3.4, c2.4, d4.4]",
		},
	})
}

// testSpecialNamedParents is a general test of parent selection.
// Event name means:
// - unique event name;
// - "." - separator;
// - stage - makes ;
func testSpecialNamedParents(t *testing.T, asciiScheme string, exp map[int]map[string]string) {
	assertar := assert.New(t)

	// decode is a event name parser
	decode := func(name string) (stage int) {
		n, err := strconv.ParseUint(strings.Split(name, ".")[1], 10, 32)
		if err != nil {
			panic(err.Error() + ". Name event " + name + " properly: <UniqueName>.<StageN>")
		}
		stage = int(n)
		return
	}

	ordered := make(dag.Events, 0)
	nodes, _, _ := tdag.ASCIIschemeForEach(asciiScheme, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
	})

	validators := pos.ArrayToValidators(nodes, []pos.Weight{5, 6, 7, 8, 9})

	events := make(map[hash.Event]dag.Event)
	getEvent := func(id hash.Event) dag.Event {
		return events[id]
	}

	crit := func(err error) {
		panic(err)
	}

	vecClock := vecfc.NewIndex(crit, vecfc.LiteConfig())
	vecClock.Reset(validators, memorydb.New(), getEvent)

	capFn := func(diff idx.Event, weight pos.Weight) Metric {
		if diff > 2 {
			return Metric(2 * weight)
		}
		return Metric(diff) * Metric(weight)
	}
	diffMetricFn := func(median, current, update idx.Event, validatorIdx idx.Validator) Metric {
		if update <= median || update <= current {
			return 0
		}
		if median < current {
			return capFn(update-median, validators.GetWeightByIdx(validatorIdx)) - capFn(current-median, validators.GetWeightByIdx(validatorIdx))
		}
		return capFn(update-median, validators.GetWeightByIdx(validatorIdx))
	}
	quorumIndexers := make([]*QuorumIndexer, validators.Len())
	for i, _ := range validators.IDs() {
		quorumIndexers[i] = NewQuorumIndexer(validators, &adapters.VectorToDagIndexer{vecClock}, diffMetricFn)
	}
	// build vector index
	for _, e := range ordered {
		events[e.ID()] = e
		_ = vecClock.Add(e)
	}

	// divide events by stages
	var stages []dag.Events
	for _, e := range ordered {
		name := e.(*tdag.TestEvent).Name
		stage := decode(name)
		for i := len(stages); i <= stage; i++ {
			stages = append(stages, nil)
		}
		stages[stage] = append(stages[stage], e)
	}

	heads := hash.EventsSet{}
	tips := map[idx.ValidatorID]*hash.Event{}
	// check
	for stage, ee := range stages {
		t.Logf("Stage %d:", stage)

		// build heads/tips and quorum indexers
		for _, e := range ee {
			for _, p := range e.Parents() {
				if heads.Contains(p) {
					heads.Erase(p)
				}
			}
			heads.Add(e.ID())
			id := e.ID()
			tips[e.Creator()] = &id

			for i, id := range validators.IDs() {
				quorumIndexers[i].ProcessEvent(e, e.Creator() == id)
			}
		}

		for _, validatorID := range nodes {
			selfParent := tips[validatorID]

			var strategies []SearchStrategy
			for len(strategies) < 2 {
				strategies = append(strategies, quorumIndexers[validators.GetIdx(validatorID)].SearchStrategy())
			}

			var existingParents hash.Events
			if selfParent != nil {
				existingParents = append(existingParents, *selfParent)
			}
			parents := ChooseParents(existingParents, heads.Slice(), strategies)

			if selfParent != nil {
				assertar.Equal(parents[0], *selfParent)
			}
			//t.Logf("\"%s\": \"%s\",", node.String(), parentsToString(parents))
			if !assertar.Equal(
				exp[stage][utils.NameOf(validatorID)],
				parentsToString(parents),
				"stage %d, %s", stage, utils.NameOf(validatorID),
			) {
				return
			}
		}
	}

	assertar.NoError(nil)
}

func parentsToString(pp hash.Events) string {
	if len(pp) < 3 {
		return pp.String()
	}

	res := make(hash.Events, len(pp))
	copy(res, pp)

	sort.Slice(res[1:], func(i, j int) bool {
		return res[i+1].String() < res[j+1].String()
	})

	return res.String()
}
