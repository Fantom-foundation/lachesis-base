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

	const epochs = 5
	var maxEpochBlocks = 30

	// seal epoch on decided frame == maxEpochBlocks
	for _, _lch := range lchs {
		lch := _lch // capture
		lch.applyBlock = func(block *lachesis.Block) *pos.Validators {
			if lch.store.GetLastDecidedFrame()+1 == idx.Frame(maxEpochBlocks) {
				// seal epoch
				return lch.store.GetValidators()
			}
			return nil
		}
	}

	var ordered dag.Events
	eventCount := int(maxEpochBlocks) * 4
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	r := rand.New(rand.NewSource(int64(len(nodes) + cheatersCount)))
	for epoch := idx.Epoch(1); epoch <= idx.Epoch(epochs); epoch++ {
		tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				inputs[GENERATOR].SetEvent(e)
				assertar.NoError(
					lchs[GENERATOR].Process(e))

				ordered = append(ordered, e)
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

	// use pre-ordered events, call consensus(es) directly
	for n, e := range ordered {
		if r.Intn(10) == 0 || n%20 == 0 {
			prev := lchs[RESTORED]

			store := NewMemStore()
			// copy prev DB into new one
			{
				it := prev.store.mainDB.NewIterator(nil, nil)
				defer it.Release()
				for it.Next() {
					assertar.NoError(store.mainDB.Put(it.Key(), it.Value()))
				}
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

	if !assertar.Equal(maxEpochBlocks*epochs, len(lchs[EXPECTED].blocks)) {
		return
	}
	compareBlocks(assertar, lchs[EXPECTED], lchs[RESTORED])
}

func compareStates(assertar *assert.Assertions, expected, restored *TestLachesis) {
	assertar.Equal(
		*(expected.store.GetLastDecidedState()), *(restored.store.GetLastDecidedState()))
	assertar.Equal(
		*(expected.store.GetEpochState()), *(restored.store.GetEpochState()))
	// check last Atropos
	if len(expected.blocks) != 0 {
		assertar.Equal(
			expected.blocks[idx.Block(len(expected.blocks))].Atropos,
			restored.blocks[idx.Block(len(restored.blocks))].Atropos,
			"block atropos doesn't match")
	}
}

func compareBlocks(assertar *assert.Assertions, expected, restored *TestLachesis) {
	assertar.Equal(len(expected.blocks), len(restored.blocks))
	for i := idx.Block(1); i <= idx.Block(len(restored.blocks)); i++ {
		if !assertar.NotNil(restored.blocks[i]) ||
			!assertar.Equal(expected.blocks[i], restored.blocks[i]) {
			return
		}
	}
}
