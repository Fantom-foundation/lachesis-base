package abft

import (
	"errors"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/dag/tdag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

const (
	TestMaxEpochBlocks = 200
)

func TestLachesisRandom_1(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{1}, 0)
}

func TestLachesisRandom_big1(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{math.MaxUint64}, 0)
}

func TestLachesisRandom_big2(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{math.MaxUint64, math.MaxUint64}, 0)
}

func TestLachesisRandom_4(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{1, 2, 3, 4}, 0)
}

func TestLachesisRandom_3_1(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{1, 1, 1, 1}, 1)
}

func TestLachesisRandom_67_33(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{33, 67}, 1)
}

func TestLachesisRandom_67_33_4(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{11, 11, 11, 67}, 3)
}

func TestLachesisRandom_67_33_5(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{11, 11, 11, 33, 34}, 3)
}

func TestLachesisRandom_2_8_10(t *testing.T) {
	testLachesisRandom(t, []pos.Stake{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, 3)
}

// TestLachesis 's possibility to get consensus in general on any event order.
func testLachesisRandom(t *testing.T, stakes []pos.Stake, cheatersCount int) {
	assertar := assert.New(t)

	const lchCount = 3
	nodes := tdag.GenNodes(len(stakes))

	lchs := make([]*TestLachesis, 0, lchCount)
	inputs := make([]*EventStore, 0, lchCount)
	for i := 0; i < lchCount; i++ {
		lch, _, input := FakeLachesis(nodes, stakes)
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
	r := rand.New(rand.NewSource(int64((len(nodes) + cheatersCount))))
	tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)

			inputs[0].SetEvent(e)
			assertar.NoError(
				lchs[0].ProcessEvent(e))
		},
		Build: func(e dag.MutableEvent, name string) error {
			if e.SelfParent() != nil {
				selfParent := *e.SelfParent()
				filtered := lchs[0].vecClock.NoCheaters(e.SelfParent(), e.Parents())
				if len(filtered) == 0 || filtered[0] != selfParent {
					return errors.New("observe myself as a cheater")
				}
				e.SetParents(filtered)
			}
			e.SetEpoch(firstEpoch)
			return lchs[0].Build(e)
		},
	})

	for i := 1; i < len(lchs); i++ {
		ee := reorder(ordered)
		for _, e := range ee {
			if e.Epoch() != firstEpoch {
				continue
			}
			inputs[i].SetEvent(e)
			assertar.NoError(
				lchs[i].ProcessEvent(e))
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

			both := lch0.store.GetLastDecidedBlock()
			if both > lch1.store.GetLastDecidedBlock() {
				both = lch1.store.GetLastDecidedBlock()
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
