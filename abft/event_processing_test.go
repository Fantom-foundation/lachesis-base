package abft

import (
	"errors"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
)

const (
	TestMaxEpochEvents = 200
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

func testLachesisRandom(t *testing.T, weights []pos.Weight, cheatersCount int) {
	testLachesisRandomAndReset(t, weights, false, cheatersCount, false)
	testLachesisRandomAndReset(t, weights, false, cheatersCount, true)
	testLachesisRandomAndReset(t, weights, true, 0, false)
	testLachesisRandomAndReset(t, weights, true, 0, true)
}

// TestLachesis 's possibility to get consensus in general on any event order.
func testLachesisRandomAndReset(t *testing.T, weights []pos.Weight, mutateWeights bool, cheatersCount int, reset bool) {
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

	eventCount := int(TestMaxEpochEvents)
	const epochs = 5
	// maxEpochBlocks should be much smaller than eventCount so that there would be enough events to seal epoch
	var maxEpochBlocks = eventCount / 20

	// seal epoch on decided frame == maxEpochBlocks
	for _, _lch := range lchs {
		lch := _lch // capture
		lch.applyBlock = func(block *lachesis.Block) *pos.Validators {
			if lch.store.GetLastDecidedFrame()+1 == idx.Frame(maxEpochBlocks) {
				// seal epoch
				if mutateWeights {
					return mutateValidators(lch.store.GetValidators())
				}
				return lch.store.GetValidators()
			}
			return nil
		}
	}

	// create events on lch0
	ordered := map[idx.Epoch]dag.Events{}
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	epochStates := map[idx.Epoch]*EpochState{}
	r := rand.New(rand.NewSource(int64(len(nodes) + cheatersCount)))
	for epoch := idx.Epoch(1); epoch <= idx.Epoch(epochs); epoch++ {
		tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				ordered[epoch] = append(ordered[epoch], e)

				inputs[0].SetEvent(e)
				assertar.NoError(
					lchs[0].Process(e))
				epochStates[lchs[0].store.GetEpoch()] = lchs[0].store.GetEpochState()
			},
			Build: func(e dag.MutableEvent, name string) error {
				if epoch != lchs[0].store.GetEpoch() {
					return errors.New("epoch already sealed, skip")
				}
				e.SetEpoch(epoch)
				return lchs[0].Build(e)
			},
		})
		if lchs[0].store.GetEpoch() != epoch+1 {
			assertar.Fail("epoch wasn't sealed", epoch)
		}
	}

	// connect events to other instances
	for epoch := idx.Epoch(1); epoch <= idx.Epoch(epochs); epoch++ {
		for i := 1; i < len(lchs); i++ {
			if reset && epoch != epochs-1 && r.Intn(2) == 0 {
				// never reset last epoch to be able to compare latest state
				resetEpoch := epoch + 1
				err := lchs[i].Reset(resetEpoch, epochStates[resetEpoch].Validators)
				assertar.NoError(err)
				continue
			}
			ee := reorder(ordered[epoch])
			for _, e := range ee {
				inputs[i].SetEvent(e)
				assertar.NoError(
					lchs[i].Process(e))
				if lchs[i].store.GetEpoch() != epoch {
					break
				}
			}
			if lchs[i].store.GetEpoch() != epoch+1 {
				assertar.Fail("epoch wasn't sealed", epoch)
			}
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

			for e := idx.Epoch(1); e <= lch0.store.GetEpoch(); e++ {
				both := lch0.epochBlocks[e]
				if both > lch1.epochBlocks[e] {
					both = lch1.epochBlocks[e]
				}
				for f := idx.Frame(1); f < both; f++ {
					key := BlockKey{e, f}
					if !assertar.Equal(
						lch0.blocks[key], lch1.blocks[key],
						"block %v", key) {
						break
					}
				}
			}

		}
	}
}
