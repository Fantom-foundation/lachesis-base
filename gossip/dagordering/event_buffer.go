package dagordering

import (
	"github.com/Fantom-foundation/lachesis-base/eventcheck"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/utils/wlru"
)

type (
	// event is a inter.Event and data for ordering purpose.
	event struct {
		dag.Event

		size uint
		peer string
	}

	// Callback is a set of EventsBuffer()'s args.
	Callback struct {
		Process func(e dag.Event) error
		Drop    func(e dag.Event, peer string, err error)
		Get     func(hash.Event) dag.Event
		Exists  func(hash.Event) bool
		Check   func(e dag.Event, parents dag.Events) error
	}
)

type EventsBuffer struct {
	incompletes *wlru.Cache // event hash -> event
	callback    Callback
}

func New(maxEventsSize uint, maxEventsNum int, callback Callback) *EventsBuffer {
	incompletes, _ := wlru.New(maxEventsSize, maxEventsNum)
	return &EventsBuffer{
		incompletes: incompletes,
		callback:    callback,
	}
}

func (buf *EventsBuffer) PushEvent(e dag.Event, size uint, peer string) {
	w := &event{
		Event: e,
		peer:  peer,
		size:  size,
	}

	buf.pushEvent(w, buf.getIncompleteEventsList(), true)
}

func (buf *EventsBuffer) getIncompleteEventsList() []*event {
	res := make([]*event, 0, buf.incompletes.Len())
	for _, childID := range buf.incompletes.Keys() {
		child, _ := buf.incompletes.Peek(childID)
		if child == nil {
			continue
		}
		res = append(res, child.(*event))
	}
	return res
}

func (buf *EventsBuffer) pushEvent(e *event, incompleteEventsList []*event, strict bool) {
	// LRU is thread-safe, no need in mutex
	if buf.callback.Exists(e.ID()) {
		if strict {
			buf.callback.Drop(e.Event, e.peer, eventcheck.ErrAlreadyConnectedEvent)
		}
		return
	}

	parents := make(dag.Events, len(e.Parents())) // use local buffer for thread safety
	for i, p := range e.Parents() {
		_, _ = buf.incompletes.Get(p) // updating the "recently used"-ness of the key
		parent := buf.callback.Get(p)
		if parent == nil {
			buf.incompletes.Add(e.ID(), e, e.size)
			return
		}
		parents[i] = parent
	}

	// validate
	if buf.callback.Check != nil {
		err := buf.callback.Check(e.Event, parents)
		if err != nil {
			buf.callback.Drop(e.Event, e.peer, err)
			return
		}
	}

	// process
	err := buf.callback.Process(e.Event)
	if err != nil {
		buf.callback.Drop(e.Event, e.peer, err)
		return
	}

	// now child events may become complete, check it again
	eHash := e.ID()
	buf.incompletes.Remove(eHash)
	for _, child := range incompleteEventsList {
		for _, parent := range child.Parents() {
			if parent == eHash {
				buf.pushEvent(child, incompleteEventsList, false)
			}
		}
	}
}

func (buf *EventsBuffer) IsBuffered(id hash.Event) bool {
	return buf.incompletes.Contains(id) // LRU is thread-safe, no need in mutex
}

func (buf *EventsBuffer) Clear() {
	buf.incompletes.Purge() // LRU is thread-safe, no need in mutex
}
