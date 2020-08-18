package eventcheck

import (
	"errors"

	"github.com/Fantom-foundation/lachesis-base/eventcheck/epochcheck"
)

var (
	ErrAlreadyConnectedEvent = errors.New("event is connected already")
)

func IsBan(err error) bool {
	if err == epochcheck.ErrNotRelevant ||
		err == ErrAlreadyConnectedEvent {
		return false
	}
	return err != nil
}
