package abft

import (
	"errors"
	"fmt"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/kvdb"
	"github.com/Fantom-foundation/go-lachesis/kvdb/memorydb"
	"github.com/Fantom-foundation/go-lachesis/lachesis"
	"github.com/Fantom-foundation/go-lachesis/vector"
)

// TestLachesis extends Lachesis for tests.
type TestLachesis struct {
	*Lachesis

	blocks map[idx.Block]*lachesis.Block
}

func (p *TestLachesis) EventsTillBlock(until idx.Block) hash.Events {
	res := make(hash.Events, 0)
	for i := idx.Block(1); i <= until; i++ {
		if p.blocks[i] == nil {
			break
		}
		res = append(res, p.blocks[i].Events...)
	}
	return res
}

// FakeLachesis creates empty abft with mem store and equal stakes of nodes in genesis.
func FakeLachesis(nodes []idx.StakerID, stakes []pos.Stake, mods ...memorydb.Mod) (*TestLachesis, *Store, *EventStore) {
	validators := make(pos.ValidatorsBuilder, len(nodes))
	for i, v := range nodes {
		if stakes == nil {
			validators[v] = 1
		} else {
			validators[v] = stakes[i]
		}
	}

	mems := memorydb.NewProducer("", mods...)
	openEDB := func(epoch idx.Epoch) kvdb.DropableStore {
		return mems.OpenDb(fmt.Sprintf("test%d", epoch))
	}
	crit := func(err error) {
		panic(err)
	}
	store := NewStore(mems.OpenDb("test"), openEDB, crit, LiteStoreConfig())

	err := store.ApplyGenesis(&Genesis{
		Validators: validators.Build(),
		Atropos:    hash.ZeroEvent,
	})
	if err != nil {
		panic(err)
	}

	input := NewEventStore()

	config := LiteConfig()
	lch := New(config, crit, store, input, vector.NewIndex(vector.LiteConfig(), crit))

	extended := &TestLachesis{
		Lachesis: lch,
		blocks:   map[idx.Block]*lachesis.Block{},
	}

	err = extended.Bootstrap(lachesis.ConsensusCallbacks{
		ApplyBlock: func(block *lachesis.Block) (sealEpoch *pos.Validators) {
			// track block events
			if extended.blocks[block.Index] != nil {
				extended.crit(errors.New("created block twice"))
			}
			if block.Index < 1 {
				extended.crit(errors.New("invalid block number"))
			}
			if block.Index > 1 && extended.blocks[block.Index-1] == nil {
				extended.crit(errors.New("created a block without previous block"))
			}
			extended.blocks[block.Index] = block

			return nil
		},
	})
	if err != nil {
		panic(err)
	}

	return extended, store, input
}
