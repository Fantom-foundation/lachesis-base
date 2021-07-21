package streamseeder

import (
	"bytes"
	"math/big"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/lachesis-base/gossip/dagstream"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/dag/tdag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
)

func TestSeederResponsesOrder(t *testing.T) {
	for try := 0; try < 25; try++ {
		testSeederResponsesOrder(t, 1+rand.Intn(70), 1+rand.Intn(70))
	}
}

type ResponsesContainer struct {
	mu          sync.RWMutex
	peerSession map[string][]dagstream.Response
}

func locatorOf(peer string, sessionID uint32) string {
	return peer + ":" + strconv.Itoa(int(sessionID))
}

func (rr *ResponsesContainer) Get(peer string, sessionID uint32) []dagstream.Response {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.peerSession[locatorOf(peer, sessionID)]
}

func (rr *ResponsesContainer) Append(peer string, sessionID uint32, response dagstream.Response) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.peerSession[locatorOf(peer, sessionID)] = append(rr.peerSession[locatorOf(peer, sessionID)], response)
}

func testSeederResponsesOrder(t *testing.T, maxPeers int, maxEvents int) {
	config := DefaultConfig(cachescale.Identity)
	config.MaxPendingResponsesSize = 5000

	events := make(dag.Events, maxEvents)
	for i := range events {
		e := &tdag.TestEvent{}
		e.SetEpoch(idx.Epoch(i / 10))
		e.SetLamport(idx.Lamport(i / 2))
		var rID [24]byte
		copy(rID[:], big.NewInt(int64(i+1)).Bytes())
		events[i] = e.Build(rID)
	}

	seeder := New(config, Callbacks{
		ForEachEvent: func(start []byte, onEvent func(key hash.Event, event interface{}, size uint64) bool) {
			for _, e := range events {
				if bytes.Compare(e.ID().Bytes(), start) > 0 {
					continue
				}
				if !onEvent(e.ID(), e, uint64(e.Size())) {
					break
				}
			}
		},
	})

	responses := ResponsesContainer{
		peerSession: map[string][]dagstream.Response{},
	}

	terminated := false
	seeder.Start()
	for i := 0; i < maxPeers; i++ {
		peer := strconv.Itoa(rand.Intn(maxPeers))
		if rand.Intn(5) == 0 {
			err := seeder.UnregisterPeer(peer)
			if !terminated {
				require.NoError(t, err)
			}
		} else {
			sessionID := uint32(i)
			reqType := dagstream.RequestIDs
			if sessionID%2 == 0 {
				reqType = dagstream.RequestEvents
			}
			startIdx := rand.Intn(len(events))
			endIdx := startIdx
			if endIdx < len(events) {
				endIdx += rand.Intn(len(events) - endIdx)
			}
			err, peerErr := seeder.NotifyRequestReceived(Peer{
				ID: peer,
				SendChunk: func(response dagstream.Response, ids hash.Events) error {
					if reqType == dagstream.RequestIDs {
						require.Empty(t, response.Events)
						require.Equal(t, response.IDs, ids)
					} else {
						require.Empty(t, response.IDs)
						require.Equal(t, len(response.Events), len(ids))
					}
					time.Sleep(time.Duration(rand.Int63n(int64(time.Millisecond))))
					responses.Append(peer, response.SessionID, response)
					return nil
				},
				Misbehaviour: func(err error) {},
			}, dagstream.Request{
				Session: dagstream.Session{
					ID:    sessionID,
					Start: events[startIdx].ID().Bytes(),
					Stop:  events[endIdx].ID().Bytes(),
				},
				Limit: dag.Metric{
					Num:  idx.Event(rand.Intn(10)),
					Size: uint64(rand.Intn(5000)),
				},
				Type:      reqType,
				MaxChunks: uint32(rand.Intn(int(config.MaxResponseChunks + 1))),
			})
			require.NoError(t, peerErr)
			if !terminated {
				require.NoError(t, err)
			}
		}
		if !terminated && rand.Intn(maxPeers*2) == 0 {
			terminated = true
			seeder.Stop()
		}
		require.LessOrEqual(t, atomic.LoadInt32(&seeder.pendingResponsesSize), 2*config.MaxPendingResponsesSize)
	}
	time.Sleep(5 * time.Millisecond) // give some time for seeder to finish the some of the work
	if !terminated {
		seeder.Stop()
	}
	// check that all the responses were sent in a correct order
	for _, sessionResponses := range responses.peerSession {
		prev := hash.Event{}
		done := false
		for _, r := range sessionResponses {
			require.False(t, done)
			ids := r.IDs
			if len(r.Events) != 0 {
				for _, e := range r.Events {
					ids = append(ids, e.(dag.Event).ID())
					require.Equal(t, e.(dag.Event).ID().Lamport(), e.(dag.Event).Lamport())
				}
			}
			for _, id := range ids {
				require.Equal(t, -1, bytes.Compare(prev.Bytes(), id.Bytes()))
			}
			if r.Done {
				done = true
			}
		}
	}
}
