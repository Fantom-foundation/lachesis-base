package streamleecher

import (
	"time"

	"github.com/Fantom-foundation/lachesis-base/gossip/dagstream/streamleecher/peerleecher"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
)

type Config struct {
	Session              peerleecher.EpochDownloaderConfig
	RecheckInterval      time.Duration
	BaseProgressWatchdog time.Duration
	BaseSessionWatchdog  time.Duration
	MinSessionRestart    time.Duration
	MaxSessionRestart    time.Duration
}

// DefaultConfig returns default leecher config
func DefaultConfig() Config {
	return Config{
		Session: peerleecher.EpochDownloaderConfig{
			DefaultChunkSize: dag.Metric{
				Num:  500, // has to be smaller than fetcher's MaxQueuedEventsBatches * MaxEventsBatch
				Size: 512 * 1024,
			},
			ParallelChunksDownload: 6,
			RecheckInterval:        10 * time.Millisecond,
		},
		RecheckInterval:      time.Second,
		BaseProgressWatchdog: time.Second * 5,
		BaseSessionWatchdog:  time.Second * 30 * 5,
		MinSessionRestart:    time.Second * 5,
		MaxSessionRestart:    time.Minute * 5,
	}
}

// DefaultConfig returns default leecher config for tests
func LiteConfig() Config {
	cfg := DefaultConfig()
	cfg.Session.DefaultChunkSize.Size /= 10
	cfg.Session.DefaultChunkSize.Num /= 10
	cfg.Session.ParallelChunksDownload = cfg.Session.ParallelChunksDownload/2 + 1
	return cfg
}
