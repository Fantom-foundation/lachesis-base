package abft

import (
	"math/rand"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
	"github.com/Fantom-foundation/lachesis-base/utils/adapters"
	"github.com/Fantom-foundation/lachesis-base/vecfc"
)

type applyBlockFn func(block *lachesis.Block) *pos.Validators

type BlockKey struct {
	Epoch idx.Epoch
	Frame idx.Frame
}

type BlockResult struct {
	Atropos    hash.Event
	Cheaters   lachesis.Cheaters
	Validators *pos.Validators
}

// TestLachesis extends Lachesis for tests.
type TestLachesis struct {
	*IndexedLachesis

	blocks      map[BlockKey]*BlockResult
	lastBlock   BlockKey
	epochBlocks map[idx.Epoch]idx.Frame

	applyBlock applyBlockFn
}

// FakeLachesis creates empty abft with mem store and equal weights of nodes in genesis.
func FakeLachesis(nodes []idx.ValidatorID, weights []pos.Weight, mods ...memorydb.Mod) (*TestLachesis, *Store, *EventStore) {
	validators := make(pos.ValidatorsBuilder, len(nodes))
	for i, v := range nodes {
		if weights == nil {
			validators[v] = 1
		} else {
			validators[v] = weights[i]
		}
	}

	openEDB := func(epoch idx.Epoch) kvdb.DropableStore {
		return memorydb.New()
	}
	crit := func(err error) {
		panic(err)
	}
	store := NewStore(memorydb.New(), openEDB, crit, LiteStoreConfig())

	err := store.ApplyGenesis(&Genesis{
		Validators: validators.Build(),
		Epoch:      FirstEpoch,
	})
	if err != nil {
		panic(err)
	}

	input := NewEventStore()

	config := LiteConfig()
	lch := NewIndexedLachesis(store, input, &adapters.VectorToDagIndexer{vecfc.NewIndex(crit, vecfc.LiteConfig())}, crit, config)

	extended := &TestLachesis{
		IndexedLachesis: lch,
		blocks:          map[BlockKey]*BlockResult{},
		epochBlocks:     map[idx.Epoch]idx.Frame{},
	}

	err = extended.Bootstrap(lachesis.ConsensusCallbacks{
		BeginBlock: func(block *lachesis.Block) lachesis.BlockCallbacks {
			return lachesis.BlockCallbacks{
				EndBlock: func() (sealEpoch *pos.Validators) {
					// track blocks
					key := BlockKey{
						Epoch: extended.store.GetEpoch(),
						Frame: extended.store.GetLastDecidedFrame() + 1,
					}
					extended.blocks[key] = &BlockResult{
						Atropos:    block.Atropos,
						Cheaters:   block.Cheaters,
						Validators: extended.store.GetValidators(),
					}
					// check that prev block exists
					if extended.lastBlock.Epoch != key.Epoch && key.Frame != 1 {
						panic("first frame must be 1")
					}
					extended.epochBlocks[key.Epoch]++
					extended.lastBlock = key
					if extended.applyBlock != nil {
						return extended.applyBlock(block)
					}
					return nil
				},
			}
		},
	})
	if err != nil {
		panic(err)
	}

	return extended, store, input
}

func mutateValidators(validators *pos.Validators) *pos.Validators {
	r := rand.New(rand.NewSource(int64(validators.TotalWeight())))
	builder := pos.NewBuilder()
	for _, vid := range validators.IDs() {
		stake := uint64(validators.Get(vid))*uint64(500+r.Intn(500))/1000 + 1
		builder.Set(vid, pos.Weight(stake))
	}
	return builder.Build()
}
