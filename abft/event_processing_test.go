package abft

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

const (
	TestMaxEpochBlocks = 200
)

func TestLachesisRandom_1(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{1}, 0)
}

func TestLachesisRandom_big1(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{math.MaxUint32 / 2}, 0)
}

func TestLachesisRandom_big2(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{math.MaxUint32 / 4, math.MaxUint32 / 4}, 0)
}

func TestLachesisRandom_big3(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{math.MaxUint32 / 8, math.MaxUint32 / 8, math.MaxUint32 / 4}, 0)
}

func TestLachesisRandom_4(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{1, 2, 3, 4}, 0)
}

func TestLachesisRandom_3_1(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{1, 1, 1, 1}, 1)
}

func TestLachesisRandom_67_33(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{33, 67}, 1)
}

func TestLachesisRandom_67_33_4(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{11, 11, 11, 67}, 3)
}

func TestLachesisRandom_67_33_5(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{11, 11, 11, 33, 34}, 3)
}

func TestLachesisRandom_2_8_10(t *testing.T) {
	testLachesisRandom(t, []pos.Weight{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, 3)
}

// TestLachesis 's possibility to get consensus in general on any event order.
func testLachesisRandom(t *testing.T, weights []pos.Weight, cheatersCount int) {
	assertar := assert.New(t)

	const lchCount = 3
	nodes := tdag.GenNodes(len(weights))

	lchs := make([]*TestLachesis, 0, lchCount)
	inputs := make([]*EventStore, 0, lchCount)
	for i := 0; i < lchCount; i++ {
		lch, _, input := FakeLachesis(nodes, weights)
		lchs = append(lchs, lch)
		inputs = append(inputs, input)
	}

	// create events on lch0
	var ordered dag.Events
	eventCount := int(TestMaxEpochBlocks)
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	r := rand.New(rand.NewSource(int64(len(nodes) + cheatersCount)))
	tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)

			inputs[0].SetEvent(e)
			assertar.NoError(
				lchs[0].Process(e))
		},
		Build: func(e dag.MutableEvent, name string) error {
			e.SetEpoch(FirstEpoch)
			return lchs[0].Build(e)
		},
	})

	for i := 1; i < len(lchs); i++ {
		ee := reorder(ordered)
		for _, e := range ee {
			if e.Epoch() != FirstEpoch {
				continue
			}
			inputs[i].SetEvent(e)
			assertar.NoError(
				lchs[i].Process(e))
		}
	}

	t.Run("Check consensus", func(t *testing.T) {
		compareResults(t, lchs)
	})
}

// reorder events, but ancestors are before it's descendants.
func reorder(events dag.Events) dag.Events {
	unordered := make(dag.Events, len(events))
	for i, j := range rand.Perm(len(events)) {
		unordered[j] = events[i]
	}

	reordered := tdag.ByParents(unordered)
	return reordered
}

func compareResults(t *testing.T, lchs []*TestLachesis) {
	assertar := assert.New(t)

	for i := 0; i < len(lchs)-1; i++ {
		lch0 := lchs[i]
		for j := i + 1; j < len(lchs); j++ {
			lch1 := lchs[j]

			assertar.Equal(*(lchs[j].store.GetLastDecidedState()), *(lchs[i].store.GetLastDecidedState()))
			assertar.Equal(*(lchs[j].store.GetEpochState()), *(lchs[i].store.GetEpochState()))

			both := idx.Block(len(lch0.blocks))
			if both > idx.Block(len(lch1.blocks)) {
				both = idx.Block(len(lch1.blocks))
			}

			for b := idx.Block(1); b <= both; b++ {
				if !assertar.Equal(
					lch0.blocks[b], lch1.blocks[b],
					"block %d", b) {
					break
				}
			}

		}
	}
}
