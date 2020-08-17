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
	"github.com/Fantom-foundation/go-lachesis/lachesis"
)

func TestConfirmBlocks_1(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{1}, 0)
}

func TestConfirmBlocks_big1(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{math.MaxUint64}, 0)
}

func TestConfirmBlocks_big2(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{math.MaxUint64, math.MaxUint64}, 0)
}

func TestConfirmBlocks_4(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{1, 2, 3, 4}, 0)
}

func TestConfirmBlocks_3_1(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{1, 1, 1, 1}, 1)
}

func TestConfirmBlocks_67_33(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{33, 67}, 1)
}

func TestConfirmBlocks_67_33_4(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{11, 11, 11, 67}, 3)
}

func TestConfirmBlocks_67_33_5(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{11, 11, 11, 33, 34}, 3)
}

func TestConfirmBlocks_2_8_10(t *testing.T) {
	testConfirmBlocks(t, []pos.Stake{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, 3)
}

func testConfirmBlocks(t *testing.T, stakes []pos.Stake, cheatersCount int) {
	assertar := assert.New(t)

	nodes := tdag.GenNodes(len(stakes))
	lch, _, input := FakeLachesis(nodes, stakes)

	var (
		frames []idx.Frame
		blocks []*lachesis.Block
	)
	applyBlock := lch.callback.ApplyBlock
	lch.callback.ApplyBlock = func(block *lachesis.Block) *pos.Validators {
		frames = append(frames, lch.store.GetLastDecidedFrame()+1)
		blocks = append(blocks, block)

		return applyBlock(block)
	}

	eventCount := int(TestMaxEpochBlocks)
	parentCount := 5
	if parentCount > len(nodes) {
		parentCount = len(nodes)
	}
	r := rand.New(rand.NewSource(int64((len(nodes) + cheatersCount))))
	tdag.ForEachRandFork(nodes, nodes[:cheatersCount], eventCount, parentCount, 10, r, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			input.SetEvent(e)
			assertar.NoError(
				lch.ProcessEvent(e))

		},
		Build: func(e dag.MutableEvent, name string) error {
			if e.SelfParent() != nil {
				selfParent := *e.SelfParent()
				filtered := lch.vecClock.NoCheaters(e.SelfParent(), e.Parents())
				if len(filtered) == 0 || filtered[0] != selfParent {
					return errors.New("observe myself as a cheater")
				}
				e.SetParents(filtered)
			}
			e.SetEpoch(firstEpoch)
			return lch.Build(e)
		},
	})

	// unconfirm all events
	it := lch.store.epochTable.ConfirmedEvent.NewIterator()
	batch := lch.store.epochTable.ConfirmedEvent.NewBatch()
	for it.Next() {
		assertar.NoError(batch.Delete(it.Key()))
	}
	assertar.NoError(batch.Write())
	it.Release()

	for i, block := range blocks {
		frame := frames[i]
		atropos := blocks[i].Atropos

		// call confirmBlock again
		gotBlock, err := lch.confirmBlock(frame, atropos)

		if !assertar.NoError(err) {
			break
		}
		if !assertar.LessOrEqual(gotBlock.Cheaters.Len(), cheatersCount) {
			break
		}
		if !assertar.Equal(block.Events, gotBlock.Events) {
			break
		}
	}
	assertar.GreaterOrEqual(len(blocks), TestMaxEpochBlocks/5)
}
