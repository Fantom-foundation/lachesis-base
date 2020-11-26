package peerleecher

import (
	"errors"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

var (
	errTerminated = errors.New("terminated")
)

// OnlyNotConnectedFn returns only not connected events.
type OnlyNotConnectedFn func(ids hash.Events) hash.Events

type receivedChunk struct {
	last  hash.Event
	total dag.Metric
}

type EpochDownloaderCallbacks struct {
	OnlyNotConnected OnlyNotConnectedFn

	RequestChunk func(dag.Metric) error

	Suspend func() bool

	Done func() bool
}

// PeerLeecher is responsible for accumulating pack announcements from various peers
// and scheduling them for retrieval.
type PeerLeecher struct {
	cfg EpochDownloaderConfig

	totalRequested dag.Metric
	totalProcessed dag.Metric

	processingChunks []receivedChunk

	// Various event channels
	notifyReceivedChunk chan *receivedChunk

	quitMu sync.Mutex
	quit   chan struct{}
	done   bool

	wg *sync.WaitGroup

	parallelTasks chan func()

	// Callbacks
	callback EpochDownloaderCallbacks
}

// New creates a packs fetcher to retrieve events based on pack announcements. Works only with 1 peer.
func New(wg *sync.WaitGroup, cfg EpochDownloaderConfig, callback EpochDownloaderCallbacks) *PeerLeecher {
	return &PeerLeecher{
		processingChunks:    make([]receivedChunk, 0, cfg.ParallelChunksDownload+1),
		parallelTasks:       make(chan func(), cfg.ParallelChunksDownload+1),
		notifyReceivedChunk: make(chan *receivedChunk, cfg.ParallelChunksDownload+1),
		quit:                make(chan struct{}),
		cfg:                 cfg,
		callback:            callback,
		wg:                  wg,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and event fetches until termination requested.
func (d *PeerLeecher) Start() {
	for i := 0; i < d.cfg.ParallelChunksDownload; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			worker(d.parallelTasks, d.quit)
		}()
	}
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
func (d *PeerLeecher) NotifyChunkReceived(last hash.Event, total dag.Metric) error {
	op := &receivedChunk{
		last:  last,
		total: total,
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
			d.totalProcessed.Num += op.total.Num
			d.totalProcessed.Size += op.total.Size
		}
	}
	return notProcessed
}

func (d *PeerLeecher) tryToSync() {
	if d.callback.Suspend() {
		return
	}

	requestsToSend := make([]dag.Metric, 0, d.cfg.ParallelChunksDownload)

	for i := 0; i < d.cfg.ParallelChunksDownload; i++ {
		want := dag.Metric{
			Num:  idx.Event(d.cfg.ParallelChunksDownload)*d.cfg.DefaultChunkSize.Num + d.totalProcessed.Num,
			Size: uint64(d.cfg.ParallelChunksDownload)*d.cfg.DefaultChunkSize.Size + d.totalProcessed.Size,
		}
		got := d.totalRequested
		toRequest := dag.Metric{}
		if want.Num > got.Num {
			toRequest.Num = want.Num - got.Num
		}
		if want.Size > got.Size {
			toRequest.Size = want.Size - got.Size
		}
		if toRequest.Num > d.cfg.DefaultChunkSize.Num {
			toRequest.Num = d.cfg.DefaultChunkSize.Num
		}
		if toRequest.Size > d.cfg.DefaultChunkSize.Size {
			toRequest.Size = d.cfg.DefaultChunkSize.Size
		}
		if toRequest.Num == 0 || toRequest.Size == 0 {
			break
		}

		d.totalRequested.Num += toRequest.Num
		d.totalRequested.Size += toRequest.Size
		requestsToSend = append(requestsToSend, toRequest)
	}
	for _, r := range requestsToSend {
		request := r
		d.parallelTasks <- func() {
			_ = d.callback.RequestChunk(request)
		}
	}
}

func worker(tasksC <-chan func(), quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case job := <-tasksC:
			job()
		}
	}
}
