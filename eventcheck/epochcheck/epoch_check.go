package epochcheck

import (
	"errors"

	"github.com/Fantom-foundation/go-lachesis/inter/dag"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

var (
	// ErrNotRelevant indicates the event's epoch isn't equal to current epoch.
	ErrNotRelevant = errors.New("event is too old or too new")
	// ErrAuth indicates that event's creator isn't authorized to create events in current epoch.
	ErrAuth = errors.New("event creator isn't a validator")
)

// Reader returns currents epoch and its validators group.
type Reader interface {
	GetEpochValidators() (*pos.Validators, idx.Epoch)
}

// Checker which require only current epoch info
type Checker struct {
	reader Reader
}

func New(reader Reader) *Checker {
	return &Checker{
		reader: reader,
	}
}

// Validate event
func (v *Checker) Validate(e dag.Event) error {
	// check epoch first, because validators group is returned only for the current epoch
	validators, epoch := v.reader.GetEpochValidators()
	if e.Epoch() != epoch {
		return ErrNotRelevant
	}
	if !validators.Exists(e.Creator()) {
		return ErrAuth
	}
	return nil
}
