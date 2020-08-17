package abft

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"

	"github.com/Fantom-foundation/go-lachesis/abft/election"
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
)

var (
	ErrCheatersObserved = errors.New("cheaters observed by self-parent aren't allowed as parents")
	ErrWrongFrame       = errors.New("claimed frame mismatched with calculated")
	ErrWrongIsRoot      = errors.New("claimed isRoot mismatched with calculated")
)

type uniqueID struct {
	counter *big.Int
}

func (u *uniqueID) sample() [24]byte {
	u.counter = u.counter.Add(u.counter, common.Big1)
	var id [24]byte
	copy(id[:], u.counter.Bytes())
	return id
}

// Build fills consensus-related fields: Frame, IsRoot
// returns error if event should be dropped
func (p *Lachesis) Build(e dag.MutableEvent) error {
	// sanity check
	if e.Epoch() != p.store.GetEpoch() {
		p.crit(errors.New("event has wrong epoch"))
	}
	if !p.store.GetValidators().Exists(e.Creator()) {
		p.crit(errors.New("event wasn't created by an existing validator"))
	}
	e.SetID(p.uniqueDirtyID.sample())
	err := p.vecClock.Add(e)
	defer p.vecClock.DropNotFlushed()
	if err != nil {
		return err
	}

	frame, isRoot := p.calcFrameIdx(e, false)
	e.SetFrame(frame)
	e.SetIsRoot(isRoot)

	return nil
}

// ProcessEvent takes event into processing.
// Event order matter: parents first.
// All the event checkers must be launched.
// ProcessEvent is not safe for concurrent use.
func (p *Lachesis) ProcessEvent(e dag.Event) (err error) {
	err = p.checkAndSaveEvent(e)
	if err != nil {
		return err
	}

	err = p.handleElection(e)
	if err != nil {
		// election doesn't fail under normal circumstances
		// storage is in an inconsistent state
		p.crit(err)
	}
	return err
}

// checkAndSaveEvent checks consensus-related fields: Frame, IsRoot
func (p *Lachesis) checkAndSaveEvent(e dag.Event) error {
	// don't link to known cheaters
	if len(p.vecClock.NoCheaters(e.SelfParent(), e.Parents())) != len(e.Parents()) {
		return ErrCheatersObserved
	}

	err := p.vecClock.Add(e)
	defer p.vecClock.DropNotFlushed()
	if err != nil {
		return err
	}

	// check frame & isRoot
	frameIdx, isRoot := p.calcFrameIdx(e, true)
	if e.IsRoot() != isRoot {
		return ErrWrongIsRoot
	}
	if e.Frame() != frameIdx {
		return ErrWrongFrame
	}

	if e.IsRoot() {
		p.store.AddRoot(e)
	}

	// save in DB the {vectorindex, e, heads}
	p.vecClock.Flush()
	return nil
}

// calculates Atropos election for the root, calls p.onFrameDecided if election was decided
func (p *Lachesis) handleElection(root dag.Event) error {
	if root != nil { // if root is nil, then just bootstrap election
		if !root.IsRoot() {
			return nil
		}

		decided, err := p.processRoot(root.Frame(), root.Creator(), root.ID())
		if err != nil {
			return err
		}
		if decided == nil {
			return nil
		}

		// if we’re here, then this root has observed that lowest not decided frame is decided now
		sealed, err := p.onFrameDecided(decided.Frame, decided.Atropos)
		if err != nil {
			return err
		}
		if sealed {
			return nil
		}
	}

	// then call processKnownRoots until it returns nil -
	// it’s needed because new elections may already have enough votes, because we process elections from lowest to highest
	for {
		decided, err := p.processKnownRoots()
		if err != nil {
			return err
		}
		if decided == nil {
			break
		}

		sealed, err := p.onFrameDecided(decided.Frame, decided.Atropos)
		if err != nil {
			return err
		}
		if sealed {
			return nil
		}
	}
	return nil
}

func (p *Lachesis) processRoot(f idx.Frame, from idx.StakerID, id hash.Event) (*election.Res, error) {
	return p.election.ProcessRoot(election.RootAndSlot{
		ID: id,
		Slot: election.Slot{
			Frame:     f,
			Validator: from,
		},
	})
}

// The function is similar to processRoot, but it fully re-processes the current voting.
// This routine should be called after node startup, and after each decided frame.
func (p *Lachesis) processKnownRoots() (*election.Res, error) {
	// iterate all the roots from LastDecidedFrame+1 to highest, call processRoot for each
	lastDecidedFrame := p.store.GetLastDecidedFrame()
	var decided *election.Res
	for f := lastDecidedFrame + 1; ; f++ {
		frameRoots := p.store.GetFrameRoots(f)
		for _, it := range frameRoots {
			var err error
			decided, err = p.processRoot(it.Slot.Frame, it.Slot.Validator, it.ID)
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
func (p *Lachesis) forklessCausedByQuorumOn(e dag.Event, f idx.Frame) bool {
	observedCounter := p.store.GetValidators().NewCounter()
	// check "observing" prev roots only if called by creator, or if creator has marked that event as root
	for _, it := range p.store.GetFrameRoots(f) {
		if p.vecClock.ForklessCause(e.ID(), it.ID) {
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
func (p *Lachesis) calcFrameIdx(e dag.Event, checkOnly bool) (frame idx.Frame, isRoot bool) {
	if len(e.Parents()) == 0 {
		// special case for very first events in the epoch
		return 1, true
	}

	// calc maxParentsFrame, i.e. max(parent's frame height)
	maxParentsFrame := idx.Frame(0)
	selfParentFrame := idx.Frame(0)

	for _, parent := range e.Parents() {
		pFrame := p.input.GetEvent(parent).Frame()
		if maxParentsFrame == 0 || pFrame > maxParentsFrame {
			maxParentsFrame = pFrame
		}

		if e.IsSelfParent(parent) {
			selfParentFrame = pFrame
		}
	}

	if checkOnly {
		// check frame & isRoot
		frame = e.Frame()
		if !e.IsRoot() {
			// don't check forklessCausedByQuorumOn if not claimed as root
			// if not root, then not allowed to move frame
			return selfParentFrame, false
		}
		// every root must be greater than prev. self-root. Instead, election will be faulty
		// roots aren't allowed to "jump" to higher frame than selfParentFrame+1, even if they are forkless caused
		// by 2/3W+1 there. It's because of liveness with forks, when up to 1/3W of roots on any frame may become "invisible"
		// for forklessCause relation (so if we skip frames, there's may be deadlock when frames cannot advance because there's
		// less than 2/3W visible roots)
		isRoot = frame == selfParentFrame+1 && (e.Frame() <= 1 || p.forklessCausedByQuorumOn(e, e.Frame()-1))
		return selfParentFrame + 1, isRoot
	}

	// calculate frame & isRoot
	if e.SelfParent() == nil {
		return 1, true
	}
	if p.forklessCausedByQuorumOn(e, selfParentFrame) {
		return selfParentFrame + 1, true
	}
	// Note: if we assign maxParentsFrame, it'll break the liveness for a case with forks, because there may be less
	// than 2/3W possible roots at maxParentsFrame, even if 1 validator is cheater and 1/3W were offline for some time
	// and didn't create roots at maxParentsFrame - they won't be able to create roots at maxParentsFrame because
	// every frame must be greater than previous
	return selfParentFrame, false

}
