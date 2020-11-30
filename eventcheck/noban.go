package eventcheck

import (
	"errors"
)

var (
	ErrAlreadyConnectedEvent = errors.New("event is connected already")
	ErrSpilledEvent          = errors.New("event is spilled")
	ErrDuplicateEvent        = errors.New("event is duplicated")
)
