package dagfetcher

import "time"

type Config struct {
	ForgetTimeout time.Duration // RawTime before an announced event is forgotten
	ArriveTimeout time.Duration // RawTime allowance before an announced event is explicitly requested
	GatherSlack   time.Duration // Interval used to collate almost-expired announces with fetches
	FetchTimeout  time.Duration // Maximum allowed time to return an explicitly requested event
	HashLimit     int           // Maximum number of unique events a peer may have announced

	MaxEventsBatch int // Maximum number of events in an inject batch (batch is divided if exceeded)
	MaxHashesBatch int // Maximum number of hashes in an announce batch (batch is divided if exceeded)

	// MaxQueuedEventsBatches is the maximum number of inject batches to queue up before
	// dropping incoming events.
	MaxQueuedEventsBatches int
	// MaxQueuedHashesBatches is the maximum number of announce batches to queue up before
	// dropping incoming hashes.
	MaxQueuedHashesBatches int
}

func DefaultConfig() Config {
	return Config{
		ForgetTimeout:          1 * time.Minute,
		ArriveTimeout:          1000 * time.Millisecond,
		GatherSlack:            100 * time.Millisecond,
		FetchTimeout:           10 * time.Second,
		HashLimit:              10000,
		MaxEventsBatch:         8,
		MaxHashesBatch:         256,
		MaxQueuedEventsBatches: 128,
		MaxQueuedHashesBatches: 128,
	}
}
