package vecengine

import (
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

type LowestAfterI interface {
	InitWithEvent(i idx.Validator, e dag.Event)
	Visit(i idx.Validator, e dag.Event) bool
}

type HighestBeforeI interface {
	InitWithEvent(i idx.Validator, e dag.Event)
	IsEmpty(i idx.Validator) bool
	IsForkDetected(i idx.Validator) bool
	Seq(i idx.Validator) idx.Event
	MinSeq(i idx.Validator) idx.Event
	SetForkDetected(i idx.Validator)
	CollectFrom(other HighestBeforeI, branches idx.Validator)
	GatherFrom(to idx.Validator, other HighestBeforeI, from []idx.Validator)
}

type allVecs struct {
	after  LowestAfterI
	before HighestBeforeI
}
