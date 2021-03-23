package abft

import (
	"github.com/pkg/errors"

	"github.com/Fantom-foundation/lachesis-base/abft/election"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

var (
	ErrWrongFrame = errors.New("claimed frame mismatched with calculated")
)

// Build fills consensus-related fields: Frame, IsRoot
// returns error if event should be dropped
func (p *Orderer) Build(e dag.MutableEvent) error {
	// sanity check
	if e.Epoch() != p.store.GetEpoch() {
		p.crit(errors.New("event has wrong epoch"))
	}
	if !p.store.GetValidators().Exists(e.Creator()) {
		p.crit(errors.New("event wasn't created by an existing validator"))
	}

	_, frame := p.calcFrameIdx(e, false)
	e.SetFrame(frame)

	return nil
}

// Process takes event into processing.
// Event order matter: parents first.
// All the event checkers must be launched.
// Process is not safe for concurrent use.
func (p *Orderer) Process(e dag.Event) (err error) {
	err, selfParentFrame := p.checkAndSaveEvent(e)
	if err != nil {
		return err
	}

	err = p.handleElection(selfParentFrame, e)
	if err != nil {
		// election doesn't fail under normal circumstances
		// storage is in an inconsistent state
		p.crit(err)
	}
	return err
}

// checkAndSaveEvent checks consensus-related fields: Frame, IsRoot
func (p *Orderer) checkAndSaveEvent(e dag.Event) (error, idx.Frame) {
	// check frame & isRoot
	selfParentFrame, frameIdx := p.calcFrameIdx(e, true)
	if e.Frame() != frameIdx {
		return ErrWrongFrame, 0
	}

	if selfParentFrame != frameIdx {
		p.store.AddRoot(selfParentFrame, e)
	}
	return nil, selfParentFrame
}

// calculates Atropos election for the root, calls p.onFrameDecided if election was decided
func (p *Orderer) handleElection(selfParentFrame idx.Frame, root dag.Event) error {
	for f := selfParentFrame + 1; f <= root.Frame(); f++ {
		decided, err := p.election.ProcessRoot(election.RootAndSlot{
			ID: root.ID(),
			Slot: election.Slot{
				Frame:     f,
				Validator: root.Creator(),
			},
		})
		if err != nil {
			return err
		}
		if decided == nil {
			continue
		}

		// if we’re here, then this root has observed that lowest not decided frame is decided now
		sealed, err := p.onFrameDecided(decided.Frame, decided.Atropos)
		if err != nil {
			return err
		}
		if sealed {
			break
		}
		sealed, err = p.bootstrapElection()
		if err != nil {
			return err
		}
		if sealed {
			break
		}
	}
	return nil
}

// bootstrapElection calls processKnownRoots until it returns nil
func (p *Orderer) bootstrapElection() (bool, error) {
	for {
		decided, err := p.processKnownRoots()
		if err != nil {
			return false, err
		}
		if decided == nil {
			break
		}

		sealed, err := p.onFrameDecided(decided.Frame, decided.Atropos)
		if err != nil {
			return false, err
		}
		if sealed {
			return true, nil
		}
	}
	return false, nil
}

// The function is similar to processRoot, but it fully re-processes the current voting.
// This routine should be called after node startup, and after each decided frame.
func (p *Orderer) processKnownRoots() (*election.Res, error) {
	// iterate all the roots from LastDecidedFrame+1 to highest, call processRoot for each
	lastDecidedFrame := p.store.GetLastDecidedFrame()
	var decided *election.Res
	for f := lastDecidedFrame + 1; ; f++ {
		frameRoots := p.store.GetFrameRoots(f)
		for _, it := range frameRoots {
			var err error
			decided, err = p.election.ProcessRoot(it)
			if err != nil {
				return nil, err
			}
			if decided != nil {
				return decided, nil
			}
		}
		if len(frameRoots) == 0 {
			break
		}
	}
	return nil, nil
}

// forklessCausedByQuorumOn returns true if event is forkless caused by 2/3W roots on specified frame
func (p *Orderer) forklessCausedByQuorumOn(e dag.Event, f idx.Frame) bool {
	observedCounter := p.store.GetValidators().NewCounter()
	// check "observing" prev roots only if called by creator, or if creator has marked that event as root
	for _, it := range p.store.GetFrameRoots(f) {
		if p.dagIndex.ForklessCause(e.ID(), it.ID) {
			observedCounter.Count(it.Slot.Validator)
		}
		if observedCounter.HasQuorum() {
			break
		}
	}
	return observedCounter.HasQuorum()
}

// calcFrameIdx checks root-conditions for new event
// and returns event's frame.
// It is not safe for concurrent use.
func (p *Orderer) calcFrameIdx(e dag.Event, checkOnly bool) (selfParentFrame, frame idx.Frame) {
	selfParentFrame = idx.Frame(0)
	if e.SelfParent() != nil {
		selfParentFrame = p.input.GetEvent(*e.SelfParent()).Frame()
	}

	// Note: we cannot "skip" frames and also we must check that event is caused by 2/3W+1 roots at F, even if one
	// of the parents has a frame >= F+1
	// The reason of those checks is that "forkless caused" relation isn't transitive in a case if there's at least one
	// cheater

	maxFrameToCheck := selfParentFrame + 100
	if checkOnly {
		maxFrameToCheck = e.Frame()
	}

	var f idx.Frame
	for f = selfParentFrame; f < maxFrameToCheck && p.forklessCausedByQuorumOn(e, f); f++ {
	}
	if f == 0 {
		f = 1
	}
	return selfParentFrame, f
}
