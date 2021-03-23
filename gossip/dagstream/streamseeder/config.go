package streamseeder

type Config struct {
	SenderThreads           int
	MaxSenderTasks          int
	MaxPendingResponsesSize int32
	MaxResponseChunks       uint32
}

func DefaultConfig() Config {
	return Config{
		SenderThreads:           8,
		MaxPendingResponsesSize: 64 * 1024 * 1024,
		MaxSenderTasks:          128,
		MaxResponseChunks:       12,
	}
}
