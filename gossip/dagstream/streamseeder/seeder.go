package streamseeder

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/lachesis-base/gossip/dagstream"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/utils/workers"
)

var (
	ErrWrongType        = errors.New("wrong request type")
	ErrWrongSelectorLen = errors.New("wrong event selector length")
	ErrSelectorMismatch = errors.New("session selector mismatch")
	ErrTooManyChunks    = errors.New("too many request chunks")
	errTerminated       = errors.New("terminated")
)

const (
	hashMemSize = 128
)

type Seeder struct {
	callback Callbacks

	peerSessions map[string][]uint32
	sessions     map[sessionIDAndPeer]sessionState

	notifyUnregisteredPeer chan string
	notifyReceivedRequest  chan *requestAndPeer
	quit                   chan struct{}
	done                   bool

	cfg Config

	wg sync.WaitGroup

	senders              []*workers.Workers
	pendingResponsesSize int32
	sessionsCounter      uint32
}

func New(cfg Config, callbacks Callbacks) *Seeder {
	s := &Seeder{
		callback:               callbacks,
		peerSessions:           make(map[string][]uint32),
		sessions:               make(map[sessionIDAndPeer]sessionState),
		notifyUnregisteredPeer: make(chan string, 128),
		notifyReceivedRequest:  make(chan *requestAndPeer, 16),
		senders:                make([]*workers.Workers, cfg.SenderThreads),
		quit:                   make(chan struct{}),
		cfg:                    cfg,
	}
	for i := 0; i < cfg.SenderThreads; i++ {
		s.senders[i] = workers.New(&s.wg, s.quit, s.cfg.MaxSenderTasks)
	}
	return s
}

type Callbacks struct {
	ForEachEvent func(start []byte, onEvent func(key hash.Event, event interface{}, size uint64) bool)
}

type Peer struct {
	ID           string
	SendChunk    func(dagstream.Response, hash.Events) error
	Misbehaviour func(error)
}

type sessionIDAndPeer struct {
	id   uint32
	peer string
}

type requestAndPeer struct {
	request dagstream.Request
	peer    Peer
}

type sessionState struct {
	origSelector []byte
	next         []byte
	stop         []byte
	done         bool
	senderI      int
	sendChunk    func(dagstream.Response, hash.Events) error
}

func (s *Seeder) Start() {
	for i := 0; i < s.cfg.SenderThreads; i++ {
		s.senders[i].Start(1)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.readerLoop()
	}()
}

// Stop interrupts the seeder, canceling all the pending operations.
// Stop waits until all the internal goroutines have finished.
func (s *Seeder) Stop() {
	close(s.quit)
	s.done = true
	for i := 0; i < s.cfg.SenderThreads; i++ {
		s.senders[i].Drain()
	}
	s.wg.Wait()
}

func (s *Seeder) NotifyRequestReceived(peer Peer, r dagstream.Request) (err error, peerErr error) {
	if len(r.Session.Start) > len(hash.ZeroEvent) || len(r.Session.Stop) > len(hash.ZeroEvent) {
		return nil, ErrWrongSelectorLen
	}
	if r.Type != dagstream.RequestIDs && r.Type != dagstream.RequestEvents {
		return nil, ErrWrongType
	}
	if r.MaxChunks > s.cfg.MaxResponseChunks {
		return nil, ErrTooManyChunks
	}
	// sanitize maximum chunk limits
	maxNumLimit := idx.Event(s.cfg.MaxPendingResponsesSize/hashMemSize) / 4
	if r.Limit.Num > maxNumLimit {
		r.Limit.Num = maxNumLimit
	}
	maxSizeLimit := (uint64(s.cfg.MaxPendingResponsesSize) - uint64(r.Limit.Num*hashMemSize)) / 2
	if r.Limit.Size > maxSizeLimit {
		r.Limit.Size = maxSizeLimit
	}
	// submit request
	op := &requestAndPeer{
		peer:    peer,
		request: r,
	}
	select {
	case s.notifyReceivedRequest <- op:
		return nil, nil
	case <-s.quit:
		return errTerminated, nil
	}
}

func (s *Seeder) UnregisterPeer(peer string) error {
	select {
	case s.notifyUnregisteredPeer <- peer:
		return nil
	case <-s.quit:
		return errTerminated
	}
}

func (s *Seeder) waitPendingResponsesBelowLimit() {
	for atomic.LoadInt32(&s.pendingResponsesSize) >= s.cfg.MaxPendingResponsesSize {
		if s.done {
			// terminating, abort all operations
			return
		}
		// we shouldn't get here normally, so it's fine to spin instead of a conditional variable
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Seeder) readerLoop() {
	for {
		// Wait for an outside event to occur
		select {
		case <-s.quit:
			// terminating, abort all operations
			return

		case peerID := <-s.notifyUnregisteredPeer:
			sessions := s.peerSessions[peerID]
			for _, sid := range sessions {
				delete(s.sessions, sessionIDAndPeer{sid, peerID})
			}
			delete(s.peerSessions, peerID)

		case op := <-s.notifyReceivedRequest:
			s.waitPendingResponsesBelowLimit()

			// prune oldest session
			sessions := s.peerSessions[op.peer.ID]
			if len(sessions) > 2 {
				oldest := sessions[0]
				sessions = sessions[1:]
				delete(s.sessions, sessionIDAndPeer{oldest, op.peer.ID})
			}

			// add session
			session, ok := s.sessions[sessionIDAndPeer{op.request.Session.ID, op.peer.ID}]
			if !ok {
				session.origSelector = op.request.Session.Start
				session.next = op.request.Session.Start
				session.stop = op.request.Session.Stop
				session.sendChunk = op.peer.SendChunk
				session.senderI = int(s.sessionsCounter % uint32(s.cfg.SenderThreads))
				sessions = append(sessions, op.request.Session.ID)
				s.peerSessions[op.peer.ID] = sessions
				s.sessionsCounter++
			}

			// sanity check (cannot change session parameters after it's created)
			if bytes.Compare(session.origSelector, op.request.Session.Start) != 0 {
				op.peer.Misbehaviour(ErrSelectorMismatch)
				continue
			}

			for i := uint32(0); i < op.request.MaxChunks && !session.done; i++ {
				allConsumed := true
				resp := dagstream.Response{}
				size := uint64(0)
				var last hash.Event
				var ids hash.Events
				s.callback.ForEachEvent(session.next, func(id hash.Event, event interface{}, eventSize uint64) bool {
					if bytes.Compare(id.Bytes(), session.stop) >= 0 {
						return false
					}
					lim := op.request.Limit
					limitReached := idx.Event(len(resp.IDs)) >= lim.Num || idx.Event(len(resp.Events)) >= lim.Num || size >= lim.Size
					if size != 0 && limitReached {
						allConsumed = false
						return false
					}
					if op.request.Type == dagstream.RequestEvents {
						resp.Events = append(resp.Events, event)
						ids = append(ids, id)
					} else {
						resp.IDs = append(resp.IDs, id)
						ids = resp.IDs
					}
					size += eventSize
					last = id
					return true
				})
				// update session
				nextBn := last.Big()
				nextBn.Add(nextBn, common.Big1)
				session.next = common.BytesToHash(nextBn.Bytes()).Bytes()
				session.done = allConsumed
				s.sessions[sessionIDAndPeer{op.request.Session.ID, op.peer.ID}] = session

				resp.Done = allConsumed
				resp.SessionID = op.request.Session.ID

				memSize := int32(size) + int32(len(resp.Events)*hashMemSize)
				if op.request.Type != dagstream.RequestEvents {
					memSize = int32(len(resp.IDs) * hashMemSize)
				}
				s.waitPendingResponsesBelowLimit()
				atomic.AddInt32(&s.pendingResponsesSize, memSize)
				_ = s.senders[session.senderI].Enqueue(func() {
					_ = session.sendChunk(resp, ids)
					atomic.AddInt32(&s.pendingResponsesSize, -memSize)
				})
			}
		}
	}
}
