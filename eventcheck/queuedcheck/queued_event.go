package queuedcheck

import "github.com/Fantom-foundation/lachesis-base/inter/dag"

type EventTask interface {
	Event() dag.Event
	Result() error
	SetResult(error)
}

type wrappedEvent struct {
	event   dag.Event
	result  error
	checked bool
}

func NewTask(event dag.Event) *wrappedEvent {
	return &wrappedEvent{
		event:   event,
		result:  nil,
		checked: false,
	}
}

func (e *wrappedEvent) Event() dag.Event {
	return e.event
}

func (e *wrappedEvent) Result() error {
	if !e.checked {
		panic("event is not checked")
	}
	return e.result
}

func (e *wrappedEvent) SetResult(res error) {
	if e.checked {
		panic("event is already checked")
	}
	e.result = res
	e.checked = true
}
