package vector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/dag/tdag"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
	"github.com/Fantom-foundation/go-lachesis/kvdb/memorydb"
)

func TestMedianTimeOnIndex(t *testing.T) {
	nodes := tdag.GenNodes(5)
	weights := []pos.Stake{5, 4, 3, 2, 1}
	validators := pos.ArrayToValidators(nodes, weights)

	vi := NewIndex(LiteConfig(), func(err error) { panic(err) })
	vi.Reset(validators, memorydb.New(), nil)

	assertar := assert.New(t)
	{ // seq=0
		e := hash.ZeroEvent
		// validator indexes are sorted by stake amount
		beforeSeq := NewHighestBeforeSeq(validators.Len())
		beforeTime := NewHighestBeforeTime(validators.Len())

		beforeSeq.Set(0, BranchSeq{seq: 0})
		beforeTime.Set(0, 100)

		beforeSeq.Set(1, BranchSeq{seq: 0})
		beforeTime.Set(1, 100)

		beforeSeq.Set(2, BranchSeq{seq: 1})
		beforeTime.Set(2, 10)

		beforeSeq.Set(3, BranchSeq{seq: 1})
		beforeTime.Set(3, 10)

		beforeSeq.Set(4, BranchSeq{seq: 1})
		beforeTime.Set(4, 10)

		vi.setHighestBefore(e, beforeSeq, beforeTime)
		assertar.Equal(dag.RawTimestamp(1), vi.MedianTime(e, 1))
	}

	{ // fork seen = true
		e := hash.ZeroEvent
		// validator indexes are sorted by stake amount
		beforeSeq := NewHighestBeforeSeq(validators.Len())
		beforeTime := NewHighestBeforeTime(validators.Len())

		beforeSeq.Set(0, forkDetectedSeq)
		beforeTime.Set(0, 100)

		beforeSeq.Set(1, forkDetectedSeq)
		beforeTime.Set(1, 100)

		beforeSeq.Set(2, BranchSeq{seq: 1})
		beforeTime.Set(2, 10)

		beforeSeq.Set(3, BranchSeq{seq: 1})
		beforeTime.Set(3, 10)

		beforeSeq.Set(4, BranchSeq{seq: 1})
		beforeTime.Set(4, 10)

		vi.setHighestBefore(e, beforeSeq, beforeTime)
		assertar.Equal(dag.RawTimestamp(10), vi.MedianTime(e, 1))
	}

	{ // normal
		e := hash.ZeroEvent
		// validator indexes are sorted by stake amount
		beforeSeq := NewHighestBeforeSeq(validators.Len())
		beforeTime := NewHighestBeforeTime(validators.Len())

		beforeSeq.Set(0, BranchSeq{seq: 1})
		beforeTime.Set(0, 11)

		beforeSeq.Set(1, BranchSeq{seq: 2})
		beforeTime.Set(1, 12)

		beforeSeq.Set(2, BranchSeq{seq: 2})
		beforeTime.Set(2, 13)

		beforeSeq.Set(3, BranchSeq{seq: 3})
		beforeTime.Set(3, 14)

		beforeSeq.Set(4, BranchSeq{seq: 4})
		beforeTime.Set(4, 15)

		vi.setHighestBefore(e, beforeSeq, beforeTime)
		assertar.Equal(dag.RawTimestamp(12), vi.MedianTime(e, 1))
	}

}

func TestMedianTimeOnDAG(t *testing.T) {
	dagAscii := `
 ║
 nodeA001
 ║
 nodeA012
 ║            ║
 ║            nodeB001
 ║            ║            ║
 ║            ╠═══════════ nodeC001
 ║║           ║            ║            ║
 ║╚══════════─╫─══════════─╫─══════════ nodeD001
║║            ║            ║            ║
╚ nodeA002════╬════════════╬════════════╣
 ║║           ║            ║            ║
 ║╚══════════─╫─══════════─╫─══════════ nodeD002
 ║            ║            ║            ║
 nodeA003════─╫─══════════─╫─═══════════╣
 ║            ║            ║
 ╠════════════nodeB002     ║
 ║            ║            ║
 ╠════════════╫═══════════ nodeC002
`

	weights := []pos.Stake{3, 4, 2, 1}
	genesisTime := dag.RawTimestamp(1)
	claimedTimes := map[string]dag.RawTimestamp{
		"nodeA001": dag.RawTimestamp(111),
		"nodeB001": dag.RawTimestamp(112),
		"nodeC001": dag.RawTimestamp(13),
		"nodeD001": dag.RawTimestamp(14),
		"nodeA002": dag.RawTimestamp(120),
		"nodeD002": dag.RawTimestamp(20),
		"nodeA012": dag.RawTimestamp(120),
		"nodeA003": dag.RawTimestamp(20),
		"nodeB002": dag.RawTimestamp(20),
		"nodeC002": dag.RawTimestamp(35),
	}
	medianTimes := map[string]dag.RawTimestamp{
		"nodeA001": genesisTime,
		"nodeB001": genesisTime,
		"nodeC001": dag.RawTimestamp(13),
		"nodeD001": genesisTime,
		"nodeA002": dag.RawTimestamp(112),
		"nodeD002": genesisTime,
		"nodeA012": genesisTime,
		"nodeA003": dag.RawTimestamp(20),
		"nodeB002": dag.RawTimestamp(20),
		"nodeC002": dag.RawTimestamp(35),
	}
	t.Run("testMedianTimeOnDAG", func(t *testing.T) {
		testMedianTime(t, dagAscii, weights, claimedTimes, medianTimes, genesisTime)
	})
}

func testMedianTime(t *testing.T, dagAscii string, weights []pos.Stake, claimedTimes map[string]dag.RawTimestamp, medianTimes map[string]dag.RawTimestamp, genesis dag.RawTimestamp) {
	assertar := assert.New(t)

	var ordered []dag.Event
	nodes, _, named := tdag.ASCIIschemeForEach(dagAscii, tdag.ForEachEvent{
		Build: func(e dag.MutableEvent, name string) error {
			e.SetRawTime(claimedTimes[name])
			return nil
		},
		Process: func(e dag.Event, name string) {
			ordered = append(ordered, e)
		},
	})

	validators := pos.ArrayToValidators(nodes, weights)

	events := make(map[hash.Event]dag.Event)
	getEvent := func(id hash.Event) dag.Event {
		return events[id]
	}

	vi := NewIndex(LiteConfig(), func(err error) { panic(err) })
	vi.Reset(validators, memorydb.New(), getEvent)

	// push
	for _, e := range ordered {
		events[e.ID()] = e
		assertar.NoError(vi.Add(e))
		vi.Flush()
	}

	// check
	for name, e := range named {
		expected, ok := medianTimes[name]
		if !ok {
			continue
		}
		assertar.Equal(expected, vi.MedianTime(e.ID(), genesis), name)
	}
}
