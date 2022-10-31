package vecfc

import (
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/vecengine"
)

func (b *LowestAfterSeq) InitWithEvent(i idx.Validator, e dag.Event) {
	b.Set(i, e.Seq())
}

func (b *LowestAfterSeq) Visit(i idx.Validator, e dag.Event) bool {
	if b.Get(i) != 0 {
		return false
	}

	b.Set(i, e.Seq())
	return true
}

func (b *LowestAfterSeq) IsInterfaceNil() bool {
	return (b == nil)
}

func (b *HighestBeforeSeq) InitWithEvent(i idx.Validator, e dag.Event, cacheID idx.Event) {
	b.Set(i, BranchSeq{Seq: e.Seq(), MinSeq: e.Seq(), CacheID: cacheID})
}

func (b *HighestBeforeSeq) IsEmpty(i idx.Validator) bool {
	seq := b.Get(i)
	return !seq.IsForkDetected() && seq.Seq == 0
}

func (b *HighestBeforeSeq) IsForkDetected(i idx.Validator) bool {
	return b.Get(i).IsForkDetected()
}

func (b *HighestBeforeSeq) Seq(i idx.Validator) idx.Event {
	val := b.Get(i)
	return val.Seq
}

func (b *HighestBeforeSeq) MinSeq(i idx.Validator) idx.Event {
	val := b.Get(i)
	return val.MinSeq
}

func (b *HighestBeforeSeq) CacheID(i idx.Validator) idx.Event {
	val := b.Get(i)
	return val.CacheID
}

func (b *HighestBeforeSeq) SetForkDetected(i idx.Validator) {
	b.Set(i, forkDetectedSeq)
}

// CollectFrom collects the elements from _other that are higher than ours. When
// _other has a higher seq than us for a given branch, we take that seq, and
// record the number of new events in the diff map.
func (b *HighestBeforeSeq) CollectFrom(
	_other vecengine.HighestBeforeI,
	num idx.Validator,
	diff []idx.Event) {

	other := _other.(*HighestBeforeSeq)
	for branchID := idx.Validator(0); branchID < num; branchID++ {
		hisSeq := other.Get(branchID)
		if hisSeq.Seq == 0 && !hisSeq.IsForkDetected() {
			// hisSeq doesn't observe anything about this branchID
			continue
		}

		mySeq := b.Get(branchID)
		if mySeq.IsForkDetected() {
			// mySeq observes the maximum already
			continue
		}

		if hisSeq.IsForkDetected() {
			// set fork detected
			b.SetForkDetected(branchID)
		} else {
			if mySeq.Seq == 0 || mySeq.MinSeq > hisSeq.MinSeq {
				// take hisSeq.MinSeq
				mySeq.MinSeq = hisSeq.MinSeq
				b.Set(branchID, mySeq)
			}
			if mySeq.Seq < hisSeq.Seq {
				// take hisSeq.Seq
				diff[branchID] += hisSeq.Seq - mySeq.Seq
				mySeq.Seq = hisSeq.Seq
				mySeq.CacheID = hisSeq.CacheID
				b.Set(branchID, mySeq)
			}
		}
	}
}

func (b *HighestBeforeSeq) GatherFrom(to idx.Validator, _other vecengine.HighestBeforeI, from []idx.Validator) {
	other := _other.(*HighestBeforeSeq)
	// read all branches to find highest event
	highestBranchSeq := BranchSeq{}
	for _, branchID := range from {
		branch := other.Get(branchID)
		if branch.IsForkDetected() {
			highestBranchSeq = branch
			break
		}
		if branch.Seq > highestBranchSeq.Seq {
			highestBranchSeq = branch
		}
	}
	b.Set(to, highestBranchSeq)
}
