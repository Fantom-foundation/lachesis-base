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

func (b *HighestBeforeSeq) InitWithEvent(i idx.Validator, e dag.Event) {
	b.Set(i, BranchSeq{Seq: e.Seq(), MinSeq: e.Seq()})
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

func (b *HighestBeforeSeq) SetForkDetected(i idx.Validator) {
	b.Set(i, forkDetectedSeq)
}

func (self *HighestBeforeSeq) CollectFrom(_other vecengine.HighestBeforeI, num idx.Validator) {
	other := _other.(*HighestBeforeSeq)
	for branchID := idx.Validator(0); branchID < num; branchID++ {
		hisSeq := other.Get(branchID)
		if hisSeq.Seq == 0 && !hisSeq.IsForkDetected() {
			// hisSeq doesn't observe anything about this branchID
			continue
		}
		mySeq := self.Get(branchID)

		if mySeq.IsForkDetected() {
			// mySeq observes the maximum already
			continue
		}
		if hisSeq.IsForkDetected() {
			// set fork detected
			self.SetForkDetected(branchID)
		} else {
			if mySeq.Seq == 0 || mySeq.MinSeq > hisSeq.MinSeq {
				// take hisSeq.MinSeq
				mySeq.MinSeq = hisSeq.MinSeq
				self.Set(branchID, mySeq)
			}
			if mySeq.Seq < hisSeq.Seq {
				// take hisSeq.Seq
				mySeq.Seq = hisSeq.Seq
				self.Set(branchID, mySeq)
			}
		}
	}
}

func (self *HighestBeforeSeq) GatherFrom(to idx.Validator, _other vecengine.HighestBeforeI, from []idx.Validator) {
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
	self.Set(to, highestBranchSeq)
}
