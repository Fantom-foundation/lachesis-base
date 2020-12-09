package abft

import (
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
	"github.com/Fantom-foundation/lachesis-base/utils/adapters"
	"github.com/Fantom-foundation/lachesis-base/vecfc"
)

type applyBlockFn func(block *lachesis.Block) *pos.Validators

// TestLachesis extends Lachesis for tests.
type TestLachesis struct {
	*IndexedLachesis

	blocks map[idx.Block]*lachesis.Block

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
		blocks:          map[idx.Block]*lachesis.Block{},
	}

	blockIdx := idx.Block(0)

	err = extended.Bootstrap(lachesis.ConsensusCallbacks{
		BeginBlock: func(block *lachesis.Block) lachesis.BlockCallbacks {
			blockIdx++
			return lachesis.BlockCallbacks{
				EndBlock: func() (sealEpoch *pos.Validators) {
					// track blocks
					extended.blocks[blockIdx] = block
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
