package main

import (
	"fmt"
	"io"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/utils/adapters"
	"github.com/Fantom-foundation/lachesis-base/vecfc"
)

func main() {
	openEDB := func(epoch idx.Epoch) kvdb.Store {
		return memorydb.New()
	}

	crit := func(err error) {
		panic(err)
	}

	store := abft.NewStore(memorydb.New(), openEDB, crit, abft.LiteStoreConfig())
	restored := abft.NewIndexedLachesis(store, nil, &adapters.VectorToDagIndexer{Index: vecfc.NewIndex(crit, vecfc.LiteConfig())}, crit, abft.LiteConfig())

	// prevent compiler optimizations
	fmt.Fprint(io.Discard, restored == nil)
}
