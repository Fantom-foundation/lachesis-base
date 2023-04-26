package eventcheck

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/eventcheck/basiccheck"
	"github.com/Fantom-foundation/lachesis-base/eventcheck/epochcheck"
	"github.com/Fantom-foundation/lachesis-base/eventcheck/parentscheck"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
)

type testReader struct{}

func (tr *testReader) GetEpochValidators() (*pos.Validators, idx.Epoch) {
	vb := pos.NewBuilder()
	vb.Set(1, 1)
	return vb.Build(), 1
}

func TestBasicEventValidation(t *testing.T) {
	var tests = []struct {
		e       dag.Event
		wantErr error
	}{
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(1)
			e.SetLamport(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			return e
		}(), nil},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(0)
			e.SetLamport(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			return e
		}(), basiccheck.ErrNotInited},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			return e
		}(), basiccheck.ErrNoParents},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(math.MaxInt32 - 1)
			e.SetLamport(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			return e
		}(), basiccheck.ErrHugeValue},
	}

	for _, tt := range tests {
		basicCheck := basiccheck.New()
		assert.Equal(t, tt.wantErr, basicCheck.Validate(tt.e))
	}
}

func TestEpochEventValidation(t *testing.T) {
	var tests = []struct {
		e       dag.Event
		wantErr error
	}{
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetEpoch(1)
			e.SetCreator(1)
			return e
		}(), nil},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetEpoch(2)
			e.SetCreator(1)
			return e
		}(), epochcheck.ErrNotRelevant},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetEpoch(1)
			e.SetCreator(2)
			return e
		}(), epochcheck.ErrAuth},
	}

	for _, tt := range tests {
		tr := new(testReader)
		epochCheck := epochcheck.New(tr)
		assert.Equal(t, tt.wantErr, epochCheck.Validate(tt.e))
	}
}

func TestParentsEventValidation(t *testing.T) {
	var tests = []struct {
		e         dag.Event
		pe        dag.Events
		wantErr   error
		wantPanic bool
	}{
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(2)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(1)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			nil, false},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(1)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(1)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			parentscheck.ErrWrongLamport, false},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(1)
			e.SetLamport(2)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(1)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			parentscheck.ErrWrongSelfParent, false},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(2)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(2)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			parentscheck.ErrWrongSeq, false},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(1)
			return e
		}(),
			nil,
			parentscheck.ErrWrongSeq, false},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(1)
			e.SetLamport(1)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			nil,
			nil, true},
	}

	for _, tt := range tests {
		parentsCheck := parentscheck.New()
		if tt.wantPanic {
			assert.Panics(t, func() {
				err := parentsCheck.Validate(tt.e, tt.pe)
				if err != nil {
					return
				}
			})
		} else {
			assert.Equal(t, tt.wantErr, parentsCheck.Validate(tt.e, tt.pe))
		}
	}
}

func TestAllEventValidation(t *testing.T) {
	var tests = []struct {
		e       dag.Event
		pe      dag.Events
		wantErr error
	}{
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(2)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			nil,
			basiccheck.ErrNotInited},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(1)
			e.SetLamport(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			return e
		}(),
			nil,
			epochcheck.ErrAuth},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(2)
			e.SetLamport(2)
			e.SetCreator(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(1)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			parentscheck.ErrWrongSelfParent},
		{func() dag.Event {
			e := &tdag.TestEvent{}
			e.SetSeq(1)
			e.SetLamport(2)
			e.SetCreator(1)
			e.SetEpoch(1)
			e.SetFrame(1)
			e.SetParents(hash.Events{e.ID()})
			return e
		}(),
			func() dag.Events {
				e := &tdag.TestEvent{}
				e.SetSeq(1)
				e.SetLamport(1)
				return dag.Events{e}
			}(),
			nil},
	}

	tr := new(testReader)

	checkers := Checkers{
		Basiccheck:   basiccheck.New(),
		Epochcheck:   epochcheck.New(tr),
		Parentscheck: parentscheck.New(),
	}

	for _, tt := range tests {
		assert.Equal(t, tt.wantErr, checkers.Validate(tt.e, tt.pe))
	}
}
