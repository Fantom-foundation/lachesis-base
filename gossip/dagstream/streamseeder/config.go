package streamseeder

import "github.com/Fantom-foundation/lachesis-base/utils/cachescale"

type Config struct {
	SenderThreads           int
	MaxSenderTasks          int
	MaxPendingResponsesSize int32
	MaxResponseChunks       uint32
}

func DefaultConfig(scale cachescale.Func) Config {
	return Config{
		SenderThreads:           8,
		MaxPendingResponsesSize: scale.I32(64 * 1024 * 1024),
		MaxSenderTasks:          128,
		MaxResponseChunks:       12,
	}
}
