package dagfetcher

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/utils"
)

/*
 * Fetcher is a network agent, which handles basic hash-based events sync.
 * The core mechanic is very simple: interested hash arrived => request it.
 * Fetcher has additional code to protect itself (and other nodes) against DoS.
 */

var (
	errTerminated       = errors.New("terminated")
	ErrTooManyAnnounces = errors.New("peer exceeded outstanding announces")
)

// PeerMisbehaviourFn is a callback type for dropping a peer detected as malicious.
type PeerMisbehaviourFn func(peer string, err error) bool

// EventsRequesterFn is a callback type for sending a event retrieval request.
type EventsRequesterFn func(hash.Events) error

// inject represents a schedules import operation.
type inject struct {
	events dag.Events // Incoming events
	time   time.Time  // Timestamp when received

	peer string // Identifier of the peer which sent events

	fetchEvents EventsRequesterFn
}

// announces is the hash notification of the availability of new events in the
// network.
type announcesBatch struct {
	hashes hash.Events // Hashes of the events being announced
	time   time.Time   // Timestamp of the announcement

	peer string // Identifier of the peer originating the notification

	fetchEvents EventsRequesterFn

	internal bool
}
type oneAnnounce struct {
	batch *announcesBatch
	i     int
}

// Fetcher is responsible for accumulating event announcements from various peers
// and scheduling them for retrieval.
type Fetcher struct {
	cfg Config

	// Various event channels
	notifications chan *announcesBatch
	inject        chan *inject
	quit          chan struct{}

	// Callbacks
	callback Callback

	// Announce states
	stateMu   utils.SpinLock                // Protects announces and announced
	announces map[string]int                // Per peer announce counts to prevent memory exhaustion
	announced map[hash.Event][]*oneAnnounce // Announced events, scheduled for fetching

	fetching     map[hash.Event]*oneAnnounce // Announced events, currently fetching
	fetchingTime map[hash.Event]time.Time
	wg           sync.WaitGroup
}

type Callback struct {
	// PushEvent is a callback type to connect a received event
	PushEvent func(e dag.Event, peer string)
	// FilterInterested returns only event which may be requested.
	OnlyInterested func(ids hash.Events) hash.Events
	// PeerMisbehaviour is a callback type for dropping a peer detected as malicious.
	PeerMisbehaviour func(peer string, err error) bool

	ReleasedEvent func(e dag.Event, peer string, err error)
}

// New creates a event fetcher to retrieve events based on hash announcements.
func New(cfg Config, callback Callback) *Fetcher {
	return &Fetcher{
		cfg:           cfg,
		notifications: make(chan *announcesBatch, cfg.MaxQueuedHashesBatches),
		inject:        make(chan *inject, cfg.MaxQueuedEventsBatches),
		quit:          make(chan struct{}),
		announces:     make(map[string]int),
		announced:     make(map[hash.Event][]*oneAnnounce),
		fetching:      make(map[hash.Event]*oneAnnounce),
		fetchingTime:  make(map[hash.Event]time.Time),
		callback:      callback,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and event fetches until termination requested.
func (f *Fetcher) Start() {
	f.wg.Add(1)
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *Fetcher) Stop() {
	close(f.quit)
	f.wg.Wait()
}

// Overloaded returns true if too much events are being processed or requested
func (f *Fetcher) Overloaded() bool {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	return f.overloaded()
}

func (f *Fetcher) overloaded() bool {
	return len(f.inject) > f.cfg.MaxQueuedEventsBatches*3/4 ||
		len(f.notifications) > f.cfg.MaxQueuedHashesBatches*3/4 ||
		len(f.announced) > f.cfg.HashLimit // protected by stateMu
}

// OverloadedPeer returns true if too much events are being processed or requested from the peer
func (f *Fetcher) OverloadedPeer(peer string) bool {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	return f.overloaded() || f.announces[peer] > f.cfg.HashLimit/2 // protected by stateMu
}

func (f *Fetcher) setAnnounces(peer string, num int) {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	f.announces[peer] = num
}

func (f *Fetcher) setAnnounced(id hash.Event, announces []*oneAnnounce) {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	f.announced[id] = announces
}

// Notify announces the fetcher of the potential availability of a new event in
// the network.
func (f *Fetcher) Notify(peer string, hashes hash.Events, time time.Time, fetchEvents EventsRequesterFn) error {
	// divide big batch into smaller ones
	for start := 0; start < len(hashes); start += f.cfg.MaxHashesBatch {
		end := len(hashes)
		if end > start+f.cfg.MaxHashesBatch {
			end = start + f.cfg.MaxHashesBatch
		}
		op := &announcesBatch{
			hashes:      hashes[start:end],
			time:        time,
			peer:        peer,
			fetchEvents: fetchEvents,
		}
		select {
		case <-f.quit:
			return errTerminated
		case f.notifications <- op:
			continue
		}
	}
	return nil
}

func (f *Fetcher) Enqueue(peer string, events dag.Events, time time.Time, fetchEvents EventsRequesterFn) error {
	// divide big batch into smaller ones
	for start := 0; start < len(events); start += f.cfg.MaxEventsBatch {
		end := len(events)
		if end > start+f.cfg.MaxEventsBatch {
			end = start + f.cfg.MaxEventsBatch
		}
		op := &inject{
			events:      events[start:end],
			time:        time,
			peer:        peer,
			fetchEvents: fetchEvents,
		}
		select {
		case <-f.quit:
			for _, e := range op.events {
				f.callback.ReleasedEvent(e, peer, errTerminated)
			}
			return errTerminated
		case f.inject <- op:
			continue
		}
	}
	return nil
}

func (f *Fetcher) processNotification(notification *announcesBatch, fetchTimer *time.Timer) {
	count := f.announces[notification.peer]
	if !notification.internal {
		if count+len(notification.hashes) > f.cfg.HashLimit {
			if f.callback.PeerMisbehaviour(notification.peer, ErrTooManyAnnounces) {
				return
			}
		}
	}

	first := len(f.fetching) == 0

	// filter only not known
	notification.hashes = f.callback.OnlyInterested(notification.hashes)
	if len(notification.hashes) == 0 {
		return
	}

	toFetch := make(hash.Events, 0, len(notification.hashes))
	for i, id := range notification.hashes {
		// add new announcement. other peers may already have announced it, so it's an array
		ann := &oneAnnounce{
			batch: notification,
			i:     i,
		}
		f.setAnnounced(id, append(f.announced[id], ann))
		count++ // f.announced and f.announces must be synced!
		// if it wasn't announced before, then schedule for fetching this time
		if _, ok := f.fetching[id]; !ok {
			f.fetching[id] = ann
			f.fetchingTime[id] = notification.time
			toFetch.Add(id)
		}
	}
	if !notification.internal {
		f.setAnnounces(notification.peer, count)
	}

	if len(toFetch) != 0 {
		_ = notification.fetchEvents(toFetch)
	}

	if first && len(f.fetching) != 0 {
		f.rescheduleFetch(fetchTimer)
	}
}

func (f *Fetcher) processInjection(op *inject, fetchTimer *time.Timer) {
	// A direct event insertion was requested, try and fill any pending gaps
	parents := make(hash.Events, 0, len(op.events))
	for _, e := range op.events {
		// fetch unknown parents
		for _, p := range e.Parents() {
			if _, ok := f.fetching[p]; ok {
				continue
			}
			parents.Add(p)
		}

		f.callback.PushEvent(e, op.peer)
		f.forgetHash(e.ID())
		f.callback.ReleasedEvent(e, op.peer, nil)
	}

	parents = f.callback.OnlyInterested(parents)
	if len(parents) != 0 {
		f.processNotification(&announcesBatch{
			hashes:      parents,
			time:        op.time,
			peer:        op.peer,
			fetchEvents: op.fetchEvents,
			internal:    true,
		}, fetchTimer)
	}
}

// Loop is the main fetcher loop, checking and processing various notifications
func (f *Fetcher) loop() {
	defer f.wg.Done()
	// Iterate the event fetching until a quit is requested
	fetchTimer := time.NewTimer(0)

	for {
		// Clean up any expired event fetches
		for id, announce := range f.fetching {
			if time.Since(announce.batch.time) > f.cfg.FetchTimeout {
				f.forgetHash(id)
			}
		}
		// Wait for an outside event to occur
		select {
		case <-f.quit:
			// Fetcher terminating, abort all operations
			return

		case notification := <-f.notifications:
			f.processNotification(notification, fetchTimer)

		case op := <-f.inject:
			f.processInjection(op, fetchTimer)

		case now := <-fetchTimer.C:
			// At least one event's timer ran out, check for needing retrieval
			request := make(map[string]hash.Events)

			// Find not not arrived events
			all := make(hash.Events, 0, len(f.announced))
			for e := range f.announced {
				all.Add(e)
			}
			notArrived := f.callback.OnlyInterested(all)

			for _, e := range notArrived {
				// Re-fetch not arrived events
				announces := f.announced[e]

				oldest := announces[0] // first is the oldest
				if time.Since(oldest.batch.time) > f.cfg.ForgetTimeout {
					// Forget too old announces
					f.forgetHash(e)
				} else if time.Since(f.fetchingTime[e]) > f.cfg.ArriveTimeout-f.cfg.GatherSlack {
					// The event still didn't arrive, queue for fetching from a random peer
					announce := announces[rand.Intn(len(announces))]
					request[announce.batch.peer] = append(request[announce.batch.peer], e)
					f.fetching[e] = announce
					f.fetchingTime[e] = now
				}
			}

			// Forget arrived events.
			// It's possible to get here only if event arrived out-of-fetcher, via another channel.
			// Also may be possible after a change of an epoch.
			notArrivedM := notArrived.Set()
			for _, e := range all {
				if !notArrivedM.Contains(e) {
					f.forgetHash(e)
				}
			}

			// Send out all event requests
			for peer, hashes := range request {
				// Create a closure of the fetch and schedule in on a new thread
				fetchEvents, hashes := f.fetching[hashes[0]].batch.fetchEvents, hashes
				go func(peer string) {
					_ = fetchEvents(hashes)
				}(peer)
			}
			// Schedule the next fetch if events are still pending
			f.rescheduleFetch(fetchTimer)
		}
	}
}

// rescheduleFetch resets the specified fetch timer to the next announce timeout.
func (f *Fetcher) rescheduleFetch(fetch *time.Timer) {
	// Short circuit if no events are announced
	if len(f.announced) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, t := range f.fetchingTime {
		if earliest.After(t) {
			earliest = t
		}
	}
	fetch.Reset(f.cfg.ArriveTimeout - time.Since(earliest))
}

// forgetHash removes all traces of a event announcement from the fetcher's
// internal state.
func (f *Fetcher) forgetHash(hash hash.Event) {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()

	// Remove all pending announces and decrement DOS counters
	for _, announce := range f.announced[hash] {
		if announce.batch.internal {
			continue
		}
		f.announces[announce.batch.peer]--
		if f.announces[announce.batch.peer] <= 0 {
			delete(f.announces, announce.batch.peer)
		}
	}
	delete(f.announced, hash)
	delete(f.fetching, hash)
	delete(f.fetchingTime, hash)
}
