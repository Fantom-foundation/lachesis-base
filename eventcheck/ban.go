package eventcheck

import (
	"errors"
)

var (
	ErrAlreadyConnectedEvent = errors.New("event is connected already")
)
