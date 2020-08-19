package adapters

import (
	"github.com/Fantom-foundation/lachesis-base/abft/dagidx"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/vector"
)

type VectorSeqToDagIndexSeq struct {
	vector.HighestBeforeSeq
}

// Get i's position in the byte-encoded vector clock
func (b VectorSeqToDagIndexSeq) Get(i idx.Validator) dagidx.Seq {
	seq := b.HighestBeforeSeq.Get(i)
	return &seq
}

type VectorToDagIndexer struct {
	*vector.Index
}

func (v *VectorToDagIndexer) GetHighestBeforeSeq(id hash.Event) dagidx.HighestBeforeSeq {
	return VectorSeqToDagIndexSeq{v.Index.GetHighestBeforeSeq(id)}
}
