package streamseeder

type Config struct {
	SenderThreads           int
	MaxMaxSenderTasks       int
	MaxPendingResponsesSize int32
}

func DefaultConfig() Config {
	return Config{
		SenderThreads:           8,
		MaxPendingResponsesSize: 64 * 1024 * 1024,
		MaxMaxSenderTasks:       128,
	}
}
