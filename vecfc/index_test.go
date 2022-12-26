package vecfc

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/vecengine/vecflushable"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func BenchmarkIndex_Add_MemoryDB(b *testing.B) {
	dbProducer := func() kvdb.FlushableKVStore {
		return flushable.Wrap(memorydb.New())
	}
	benchmark_Index_Add(b, dbProducer)
}

func BenchmarkIndex_Add_vecflushable_NoBackup(b *testing.B) {
	// the total database produced by the test is roughly 2'000'000 bytes (checked
	// against multiple runs) so we set the limit to double that to ensure that
	// no offloading to levelDB occurs
	dbProducer := func() kvdb.FlushableKVStore {
		db, _ := tempLevelDB()
		return vecflushable.Wrap(db, 4000000)
	}
	benchmark_Index_Add(b, dbProducer)
}

func BenchmarkIndex_Add_vecflushable_Backup(b *testing.B) {
	// the total database produced by the test is roughly 2'000'000 bytes (checked
	// against multiple runs) so we set the limit to half of that to force the
	// database to unload the cache into leveldb halfway through.
	dbProducer := func() kvdb.FlushableKVStore {
		db, _ := tempLevelDB()
		return vecflushable.Wrap(db, 1000000)
	}
	benchmark_Index_Add(b, dbProducer)
}

func benchmark_Index_Add(b *testing.B, dbProducer func() kvdb.FlushableKVStore) {
	b.StopTimer()

	nodes := tdag.GenNodes(70)
	ordered := make(dag.Events, 0)
	tdag.ForEachRandEvent(nodes, 10, 10, nil, tdag.ForEachEvent{
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
	})

	validatorsBuilder := pos.NewBuilder()
	for _, peer := range nodes {
		validatorsBuilder.Set(peer, 1)
	}
	validators := validatorsBuilder.Build()
	events := make(map[hash.Event]dag.Event)
	getEvent := func(id hash.Event) dag.Event {
		return events[id]
	}
	for _, e := range ordered {
		events[e.ID()] = e
	}

	vecClock := NewIndex(func(err error) { panic(err) }, LiteConfig())

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		vecClock.Reset(validators, dbProducer(), getEvent)
		b.StartTimer()
		for i, e := range ordered {
			err := vecClock.Add(e)
			if err != nil {
				panic(err)
			}
			vecClock.Flush()
			i++
			if i >= b.N {
				break
			}
		}
	}
}

func tempLevelDB() (kvdb.Store, error) {
	cache16mb := func(string) (int, int) {
		return 16 * opt.MiB, 64
	}
	dir, err := ioutil.TempDir("", "bench")
	if err != nil {
		panic(fmt.Sprintf("can't create temporary directory %s: %v", dir, err))
	}
	disk := leveldb.NewProducer(dir, cache16mb)
	ldb, _ := disk.OpenDB("0")
	return ldb, nil
}
