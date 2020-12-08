package dagfetcher

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/utils/wlru"
	"github.com/Fantom-foundation/lachesis-base/utils/workers"
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

type announceData struct {
	time        time.Time // Timestamp of the announcement
	peer        string    // Identifier of the peer originating the notification
	fetchEvents EventsRequesterFn
}

type announcesBatch struct {
	announceData
	ids hash.Events // Hashes of the events being announced
}

type fetchingEvent struct {
	announce     announceData
	fetchingTime time.Time
}

// Fetcher is responsible for accumulating event announcements from various peers
// and scheduling them for retrieval.
type Fetcher struct {
	cfg Config

	// Various event channels
	notifications  chan announcesBatch
	receivedEvents chan hash.Events
	quit           chan struct{}

	// Callbacks
	callback Callback

	// Announce states
	announces *wlru.Cache // Announced events, scheduled for fetching

	fetching map[hash.Event]fetchingEvent // Announced events, currently fetching
	wg       sync.WaitGroup

	parallelTasks *workers.Workers
}

type Callback struct {
	// FilterInterested returns only event which may be requested.
	OnlyInterested func(ids hash.Events) hash.Events
	// PeerMisbehaviour is a callback type for dropping a peer detected as malicious.
	PeerMisbehaviour func(peer string, err error) bool
}

// New creates a event fetcher to retrieve events based on hash announcements.
func New(cfg Config, callback Callback) *Fetcher {
	f := &Fetcher{
		cfg:            cfg,
		notifications:  make(chan announcesBatch, cfg.MaxQueuedBatches),
		receivedEvents: make(chan hash.Events, cfg.MaxQueuedBatches),
		quit:           make(chan struct{}),
		fetching:       make(map[hash.Event]fetchingEvent),
		callback:       callback,
	}
	f.announces, _ = wlru.NewWithEvict(uint(cfg.HashLimit), cfg.HashLimit, func(key interface{}, _ interface{}) {
		delete(f.fetching, key.(hash.Event))
	})
	f.parallelTasks = workers.New(&f.wg, f.quit, f.cfg.MaxParallelRequests*2)
	return f
}

// Start boots up the events fetcher.
func (f *Fetcher) Start() {
	f.parallelTasks.Start(f.cfg.MaxParallelRequests)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.loop()
	}()
}

// Stop interrupts the fetcher, canceling all the pending operations.
// Stop waits until all the internal goroutines have finished.
func (f *Fetcher) Stop() {
	close(f.quit)
	f.parallelTasks.Drain()
	f.wg.Wait()
}

// Overloaded returns true if too much events are being requested
func (f *Fetcher) Overloaded() bool {
	return len(f.receivedEvents) > f.cfg.MaxQueuedBatches*3/4 ||
		len(f.notifications) > f.cfg.MaxQueuedBatches*3/4 ||
		f.announces.Len() > f.cfg.HashLimit*3/4
}

// NotifyAnnounces announces the fetcher of the potential availability of a new event in
// the network.
func (f *Fetcher) NotifyAnnounces(peer string, ids hash.Events, time time.Time, fetchEvents EventsRequesterFn) error {
	// divide big batch into smaller ones
	for start := 0; start < len(ids); start += f.cfg.MaxBatch {
		end := len(ids)
		if end > start+f.cfg.MaxBatch {
			end = start + f.cfg.MaxBatch
		}
		op := announcesBatch{
			announceData: announceData{
				time:        time,
				peer:        peer,
				fetchEvents: fetchEvents,
			},
			ids: ids[start:end],
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

func (f *Fetcher) NotifyReceived(ids hash.Events) error {
	// divide big batch into smaller ones
	for start := 0; start < len(ids); start += f.cfg.MaxBatch {
		end := len(ids)
		if end > start+f.cfg.MaxBatch {
			end = start + f.cfg.MaxBatch
		}
		select {
		case <-f.quit:
			return errTerminated
		case f.receivedEvents <- ids[start:end]:
			continue
		}
	}
	return nil
}

func (f *Fetcher) getAnnounces(id hash.Event) []announceData {
	announces, ok := f.announces.Get(id)
	if !ok {
		return []announceData{}
	}
	return announces.([]announceData)
}

func (f *Fetcher) processNotification(notification announcesBatch, fetchTimer *time.Timer) {
	first := len(f.fetching) == 0

	// filter only not known
	notification.ids = f.callback.OnlyInterested(notification.ids)
	if len(notification.ids) == 0 {
		return
	}

	toFetch := make(hash.Events, 0, len(notification.ids))
	now := time.Now()
	for _, id := range notification.ids {
		// add new announcement. other peers may already have announced it, so it's an array
		anns := f.getAnnounces(id)
		anns = append(anns, notification.announceData)
		f.announces.Add(id, append(anns, notification.announceData), uint(len(anns)))
		// if it wasn't announced before, then schedule for fetching this time
		if _, ok := f.fetching[id]; !ok {
			f.fetching[id] = fetchingEvent{
				announce:     notification.announceData,
				fetchingTime: now,
			}
			toFetch.Add(id)
		}
	}

	if len(toFetch) != 0 {
		// Create a closure of the fetch and schedule in on a new thread
		fetchEvents, hashes := notification.fetchEvents, toFetch
		_ = f.parallelTasks.Enqueue(func() {
			_ = fetchEvents(hashes)
		})
	}

	if first && len(f.fetching) != 0 {
		f.rescheduleFetch(fetchTimer)
	}
}

// Loop is the main fetcher loop, checking and processing various notifications
func (f *Fetcher) loop() {
	// Iterate the event fetching until a quit is requested
	fetchTimer := time.NewTimer(0)

	for {
		// Wait for an outside event to occur
		select {
		case <-f.quit:
			// Fetcher terminating, abort all operations
			return

		case notification := <-f.notifications:
			f.processNotification(notification, fetchTimer)

		case ids := <-f.receivedEvents:
			for _, id := range ids {
				f.forgetHash(id)
			}

		case <-fetchTimer.C:
			now := time.Now()
			// At least one event's timer ran out, check for needing retrieval
			request := make(map[string]hash.Events)
			requestFns := make(map[string]EventsRequesterFn)

			// Find not arrived events
			all := make(hash.Events, 0, f.announces.Len())
			for _, e := range f.announces.Keys() {
				all.Add(e.(hash.Event))
			}
			notArrived := f.callback.OnlyInterested(all)

			for _, e := range notArrived {
				// Re-fetch not arrived events
				announces := f.getAnnounces(e)
				if len(announces) == 0 {
					continue
				}

				oldest := announces[0] // first is the oldest
				if time.Since(oldest.time) > f.cfg.ForgetTimeout {
					// Forget too old announces
					f.forgetHash(e)
				} else if time.Since(f.fetching[e].fetchingTime) > f.cfg.ArriveTimeout-f.cfg.GatherSlack {
					// The event still didn't arrive, queue for fetching from a random peer
					announce := announces[rand.Intn(len(announces))]
					request[announce.peer] = append(request[announce.peer], e)
					requestFns[announce.peer] = announce.fetchEvents
					f.fetching[e] = fetchingEvent{
						announce:     announce,
						fetchingTime: now,
					}
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
			for peer, req := range request {
				// Create a closure of the fetch and schedule in on a new thread
				fetchEvents, hashes := requestFns[peer], req
				_ = f.parallelTasks.Enqueue(func() {
					_ = fetchEvents(hashes)
				})
			}
			// Schedule the next fetch if events are still pending
			f.rescheduleFetch(fetchTimer)
		}
	}
}

// rescheduleFetch resets the specified fetch timer to the next announce timeout.
func (f *Fetcher) rescheduleFetch(fetch *time.Timer) {
	// Short circuit if no events are announced
	if f.announces.Len() == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, fetch := range f.fetching {
		if earliest.After(fetch.fetchingTime) {
			earliest = fetch.fetchingTime
		}
	}
	fetch.Reset(f.cfg.ArriveTimeout - time.Since(earliest))
}

// forgetHash removes all traces of a event announcement from the fetcher's
// internal state.
func (f *Fetcher) forgetHash(id hash.Event) {
	f.announces.Remove(id) // f.fetching is deleted inside the evict callback
}
