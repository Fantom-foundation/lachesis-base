package peerleecher

import (
	"errors"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
)

var (
	errTerminated = errors.New("terminated")
)

// OnlyNotConnectedFn returns only not connected events.
type OnlyNotConnectedFn func(ids hash.Events) hash.Events

type receivedChunk struct {
	last hash.Event
}

type EpochDownloaderCallbacks struct {
	OnlyNotConnected OnlyNotConnectedFn

	RequestChunks func(n dag.Metric, maxChunks uint32) error

	Suspend func() bool

	Done func() bool
}

// PeerLeecher is responsible for accumulating pack announcements from various peers
// and scheduling them for retrieval.
type PeerLeecher struct {
	cfg EpochDownloaderConfig

	totalRequested int
	totalProcessed int

	processingChunks []receivedChunk

	// Various event channels
	notifyReceivedChunk chan *receivedChunk

	quitMu sync.Mutex
	quit   chan struct{}
	done   bool

	wg *sync.WaitGroup

	// Callbacks
	callback EpochDownloaderCallbacks
}

// New creates a packs fetcher to retrieve events based on pack announcements. Works only with 1 peer.
func New(wg *sync.WaitGroup, cfg EpochDownloaderConfig, callback EpochDownloaderCallbacks) *PeerLeecher {
	quit := make(chan struct{})
	return &PeerLeecher{
		processingChunks:    make([]receivedChunk, 0, cfg.ParallelChunksDownload*2),
		notifyReceivedChunk: make(chan *receivedChunk, cfg.ParallelChunksDownload*2),
		quit:                quit,
		cfg:                 cfg,
		callback:            callback,
		wg:                  wg,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and event fetches until termination requested.
func (d *PeerLeecher) Start() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.loop()
	}()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (d *PeerLeecher) Stop() {
	d.Terminate()
	d.wg.Wait()
}

func (d *PeerLeecher) Terminate() {
	d.quitMu.Lock()
	defer d.quitMu.Unlock()
	if !d.done {
		close(d.quit)
		d.done = true
	}
}

func (d *PeerLeecher) Stopped() bool {
	return d.done
}

// NotifyPackInfo injects new pack infos from a peer
func (d *PeerLeecher) NotifyChunkReceived(last hash.Event) error {
	op := &receivedChunk{
		last: last,
	}
	select {
	case d.notifyReceivedChunk <- op:
		return nil
	case <-d.quit:
		return errTerminated
	}
}

// Loop is the main leecher's loop, checking and processing various notifications
func (d *PeerLeecher) loop() {
	// Iterate the event fetching until a quit is requested
	syncTicker := time.NewTicker(d.cfg.RecheckInterval)
	defer syncTicker.Stop()

	for {
		// Wait for an outside event to occur
		select {
		case <-d.quit:
			// terminating, abort all operations
			return

		case op := <-d.notifyReceivedChunk:

			if d.done {
				d.Terminate()
				continue
			}
			if len(d.processingChunks) < d.cfg.ParallelChunksDownload*2 {
				d.processingChunks = append(d.processingChunks, *op)
				d.routine()
			}

		case <-syncTicker.C:
			d.routine()
		}
	}
}

func (d *PeerLeecher) routine() {
	if d.callback.Done() {
		d.Terminate()
		return
	}
	d.processingChunks = d.sweepProcessedChunks()
	d.tryToSync()
}

func (d *PeerLeecher) sweepProcessedChunks() []receivedChunk {
	notProcessed := make([]receivedChunk, 0, len(d.processingChunks))
	for _, op := range d.processingChunks {
		if len(d.callback.OnlyNotConnected(hash.Events{op.last})) != 0 {
			notProcessed = append(notProcessed, op)
		} else {
			d.totalProcessed++
		}
	}
	return notProcessed
}

func (d *PeerLeecher) tryToSync() {
	if d.callback.Suspend() {
		return
	}

	if d.totalRequested < d.totalProcessed+d.cfg.ParallelChunksDownload {
		requestsToSend := (d.totalProcessed + d.cfg.ParallelChunksDownload) - d.totalRequested
		d.totalRequested += requestsToSend
		_ = d.callback.RequestChunks(d.cfg.DefaultChunkSize, uint32(requestsToSend))
	}
}
