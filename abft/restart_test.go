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
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
	"github.com/Fantom-foundation/lachesis-base/utils/adapters"
	"github.com/Fantom-foundation/lachesis-base/vecfc"
)

func TestRestart_1(t *testing.T) {
	testRestart(t, []pos.Weight{1}, 0)
}

func TestRestart_big1(t *testing.T) {
	testRestart(t, []pos.Weight{math.MaxUint32 / 2}, 0)
}

func TestRestart_big2(t *testing.T) {
	testRestart(t, []pos.Weight{math.MaxUint32 / 4, math.MaxUint32 / 4}, 0)
}

func TestRestart_big3(t *testing.T) {
	testRestart(t, []pos.Weight{math.MaxUint32 / 8, math.MaxUint32 / 8, math.MaxUint32 / 4}, 0)
}

func TestRestart_4(t *testing.T) {
	testRestart(t, []pos.Weight{1, 2, 3, 4}, 0)
}

func TestRestart_3_1(t *testing.T) {
	testRestart(t, []pos.Weight{1, 1, 1, 1}, 1)
}

func TestRestart_67_33(t *testing.T) {
	testRestart(t, []pos.Weight{33, 67}, 1)
}

func TestRestart_67_33_4(t *testing.T) {
	testRestart(t, []pos.Weight{11, 11, 11, 67}, 3)
}

func TestRestart_67_33_5(t *testing.T) {
	testRestart(t, []pos.Weight{11, 11, 11, 33, 34}, 3)
}

func TestRestart_2_8_10(t *testing.T) {
	testRestart(t, []pos.Weight{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, 3)
}

func testRestart(t *testing.T, weights []pos.Weight, cheatersCount int) {
	testRestartAndReset(t, weights, false, cheatersCount, false)
	testRestartAndReset(t, weights, false, cheatersCount, true)
	testRestartAndReset(t, weights, true, 0, false)
	testRestartAndReset(t, weights, true, 0, true)
}

func testRestartAndReset(t *testing.T, weights []pos.Weight, mutateWeights bool, cheatersCount int, resets bool) {
	assertar := assert.New(t)

	const (
		COUNT     = 3 // 3 abft instances
		GENERATOR = 0 // event generator
		EXPECTED  = 1 // sample
		RESTORED  = 2 // compare with sample
	)
	nodes := tdag.GenNodes(len(weights))

	lchs := make([]*TestLachesis, 0, COUNT)
	inputs := make([]*EventStore, 0, COUNT)
	for i := 0; i < COUNT; i++ {
		lch, _, input := FakeLachesis(nodes, weights)
		lchs = append(lchs, lch)
		inputs = append(inputs, input)
	}

	eventCount := TestMaxEpochEvents
	const epochs = 5
	// maxEpochBlocks should be much smaller than eventCount so that there would be enough events to seal epoch
	var maxEpochBlocks = eventCount / 4

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

	var ordered dag.Events
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	epochStates := map[idx.Epoch]*EpochState{}
	r := rand.New(rand.NewSource(int64(len(nodes) + cheatersCount)))
	for epoch := idx.Epoch(1); epoch <= idx.Epoch(epochs); epoch++ {
		tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				inputs[GENERATOR].SetEvent(e)
				assertar.NoError(
					lchs[GENERATOR].Process(e))

				ordered = append(ordered, e)
				epochStates[lchs[GENERATOR].store.GetEpoch()] = lchs[GENERATOR].store.GetEpochState()
			},
			Build: func(e dag.MutableEvent, name string) error {
				if epoch != lchs[GENERATOR].store.GetEpoch() {
					return errors.New("epoch already sealed, skip")
				}
				e.SetEpoch(epoch)
				return lchs[GENERATOR].Build(e)
			},
		})
	}
	if !assertar.Equal(maxEpochBlocks*epochs, len(lchs[GENERATOR].blocks)) {
		return
	}

	resetEpoch := idx.Epoch(0)

	// use pre-ordered events, call consensus(es) directly
	for _, e := range ordered {
		if e.Epoch() < resetEpoch {
			continue
		}
		if resets && epochStates[e.Epoch()+2] != nil && r.Intn(30) == 0 {
			// never reset last epoch to be able to compare latest state
			resetEpoch = e.Epoch() + 1
			err := lchs[EXPECTED].Reset(resetEpoch, epochStates[resetEpoch].Validators)
			assertar.NoError(err)
			err = lchs[RESTORED].Reset(resetEpoch, epochStates[resetEpoch].Validators)
			assertar.NoError(err)
		}
		if e.Epoch() < resetEpoch {
			continue
		}
		if r.Intn(10) == 0 {
			prev := lchs[RESTORED]

			store := NewMemStore()
			// copy prev DB into new one
			{
				it := prev.store.mainDB.NewIterator(nil, nil)
				for it.Next() {
					assertar.NoError(store.mainDB.Put(it.Key(), it.Value()))
				}
				it.Release()
			}
			restartEpochDB := memorydb.New()
			{
				it := prev.store.epochDB.NewIterator(nil, nil)
				for it.Next() {
					assertar.NoError(restartEpochDB.Put(it.Key(), it.Value()))
				}
				it.Release()
			}
			restartEpoch := prev.store.GetEpoch()
			store.getEpochDB = func(epoch idx.Epoch) kvdb.DropableStore {
				if epoch == restartEpoch {
					return restartEpochDB
				}
				return memorydb.New()
			}

			restored := NewIndexedLachesis(store, prev.input, &adapters.VectorToDagIndexer{vecfc.NewIndex(prev.crit, vecfc.LiteConfig())}, prev.crit, prev.config)
			assertar.NoError(restored.Bootstrap(prev.callback))

			lchs[RESTORED].IndexedLachesis = restored
		}

		if !assertar.Equal(e.Epoch(), lchs[EXPECTED].store.GetEpoch()) {
			break
		}
		inputs[EXPECTED].SetEvent(e)
		assertar.NoError(
			lchs[EXPECTED].Process(e))

		inputs[RESTORED].SetEvent(e)
		assertar.NoError(
			lchs[RESTORED].Process(e))

		compareStates(assertar, lchs[EXPECTED], lchs[RESTORED])
		if t.Failed() {
			return
		}
	}

	compareStates(assertar, lchs[GENERATOR], lchs[RESTORED])
	compareBlocks(assertar, lchs[EXPECTED], lchs[RESTORED])
}

func compareStates(assertar *assert.Assertions, expected, restored *TestLachesis) {
	assertar.Equal(
		*(expected.store.GetLastDecidedState()), *(restored.store.GetLastDecidedState()))
	assertar.Equal(
		expected.store.GetEpochState().String(), restored.store.GetEpochState().String())
	// check last block
	if len(expected.blocks) != 0 {
		assertar.Equal(expected.lastBlock, restored.lastBlock)
		assertar.Equal(
			expected.blocks[expected.lastBlock],
			restored.blocks[restored.lastBlock],
			"block doesn't match")
	}
}

func compareBlocks(assertar *assert.Assertions, expected, restored *TestLachesis) {
	assertar.Equal(expected.lastBlock, restored.lastBlock)
	for e := idx.Epoch(1); e <= expected.lastBlock.Epoch; e++ {
		assertar.Equal(expected.epochBlocks[e], restored.epochBlocks[e])
		for f := idx.Frame(1); f < expected.epochBlocks[e]; f++ {
			key := BlockKey{e, f}
			if !assertar.NotNil(restored.blocks[key]) ||
				!assertar.Equal(expected.blocks[key], restored.blocks[key]) {
				return
			}
		}
	}
}
