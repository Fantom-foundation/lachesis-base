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
	"github.com/Fantom-foundation/go-lachesis/kvdb"
	"github.com/Fantom-foundation/go-lachesis/kvdb/memorydb"
	"github.com/Fantom-foundation/go-lachesis/lachesis"
	"github.com/Fantom-foundation/go-lachesis/vector"
)

func TestRestart_1(t *testing.T) {
	testRestart(t, []pos.Stake{1}, 0)
}

func TestRestart_big1(t *testing.T) {
	testRestart(t, []pos.Stake{math.MaxUint64}, 0)
}

func TestRestart_big2(t *testing.T) {
	testRestart(t, []pos.Stake{math.MaxUint64, math.MaxUint64}, 0)
}

func TestRestart_4(t *testing.T) {
	testRestart(t, []pos.Stake{1, 2, 3, 4}, 0)
}

func TestRestart_3_1(t *testing.T) {
	testRestart(t, []pos.Stake{1, 1, 1, 1}, 1)
}

func TestRestart_67_33(t *testing.T) {
	testRestart(t, []pos.Stake{33, 67}, 1)
}

func TestRestart_67_33_4(t *testing.T) {
	testRestart(t, []pos.Stake{11, 11, 11, 67}, 3)
}

func TestRestart_67_33_5(t *testing.T) {
	testRestart(t, []pos.Stake{11, 11, 11, 33, 34}, 3)
}

func TestRestart_2_8_10(t *testing.T) {
	testRestart(t, []pos.Stake{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, 3)
}

func testRestart(t *testing.T, stakes []pos.Stake, cheatersCount int) {
	assertar := assert.New(t)

	const (
		COUNT     = 3 // 3 abft instances
		GENERATOR = 0 // event generator
		EXPECTED  = 1 // sample
		RESTORED  = 2 // compare with sample
	)

	nodes := tdag.GenNodes(len(stakes))
	lchs := make([]*TestLachesis, 0, COUNT)
	inputs := make([]*EventStore, 0, COUNT)
	for i := 0; i < COUNT; i++ {
		lch, _, input := FakeLachesis(nodes, stakes)
		lchs = append(lchs, lch)
		inputs = append(inputs, input)
	}

	const epochs = 5
	var maxEpochBlocks = 30

	// seal epoch on decided frame == maxEpochBlocks
	for _, _lch := range lchs {
		lch := _lch // capture
		applyBlock := lch.callback.ApplyBlock
		lch.callback.ApplyBlock = func(block *lachesis.Block) *pos.Validators {
			_ = applyBlock(block)
			if lch.store.GetLastDecidedFrame()+1 == idx.Frame(maxEpochBlocks) {
				// seal epoch
				return lch.store.GetValidators()
			}
			return nil
		}
	}

	var ordered []dag.Event
	eventCount := int(maxEpochBlocks) * 4
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	r := rand.New(rand.NewSource(int64((len(nodes) + cheatersCount))))
	for epoch := idx.Epoch(1); epoch <= idx.Epoch(epochs); epoch++ {
		tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
			Process: func(e dag.Event, name string) {
				inputs[GENERATOR].SetEvent(e)
				assertar.NoError(
					lchs[GENERATOR].ProcessEvent(e))

				ordered = append(ordered, e)
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
				if epoch != lchs[GENERATOR].store.GetEpoch() {
					return errors.New("epoch already sealed, skip")
				}
				e.SetEpoch(epoch)
				return lchs[GENERATOR].Build(e)
			},
		})
	}

	// use pre-ordered events, call consensus(e) directly, to avoid issues with restoring state of EventBuffer
	for n, e := range ordered {
		if r.Intn(10) == 0 || n%20 == 0 {
			prev := lchs[RESTORED]

			store := NewMemStore()
			// copy prev DB into new one
			{
				it := prev.store.mainDB.NewIterator()
				defer it.Release()
				for it.Next() {
					assertar.NoError(store.mainDB.Put(it.Key(), it.Value()))
				}
			}
			restartEpochDB := memorydb.New()
			{
				it := prev.store.epochDB.NewIterator()
				for it.Next() {
					assertar.NoError(restartEpochDB.Put(it.Key(), it.Value()))
				}
				it.Release()
			}
			restartEpoch := prev.store.GetEpoch()
			store.getDB = func(epoch idx.Epoch) kvdb.DropableStore {
				if epoch == restartEpoch {
					return restartEpochDB
				}
				return memorydb.New()
			}

			restored := New(prev.config, prev.crit, store, prev.input, vector.NewIndex(vector.LiteConfig(), prev.crit))
			assertar.NoError(restored.Bootstrap(prev.callback))

			lchs[RESTORED].Lachesis = restored
		}

		if !assertar.Equal(e.Epoch(), lchs[EXPECTED].store.GetEpoch()) {
			break
		}
		inputs[EXPECTED].SetEvent(e)
		assertar.NoError(
			lchs[EXPECTED].ProcessEvent(e))

		inputs[RESTORED].SetEvent(e)
		assertar.NoError(
			lchs[RESTORED].ProcessEvent(e))

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
	// check LastAtropos and Head() method
	if expected.store.GetLastDecidedBlock() != 0 {
		assertar.Equal(
			expected.blocks[idx.Block(len(expected.blocks))].Atropos,
			restored.store.GetLastDecidedState().LastAtropos,
			"block atropos doesn't match")
	}
}

func compareBlocks(assertar *assert.Assertions, expected, restored *TestLachesis) {
	assertar.Equal(len(expected.blocks), len(restored.blocks))
	assertar.Equal(len(expected.blocks), int(restored.store.GetLastDecidedBlock()))
	for i := idx.Block(1); i <= idx.Block(len(restored.blocks)); i++ {
		if !assertar.NotNil(restored.blocks[i]) ||
			!assertar.Equal(expected.blocks[i], restored.blocks[i]) {
			return
		}
	}
}
