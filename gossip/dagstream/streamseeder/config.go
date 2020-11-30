package streamseeder

type Config struct {
	SenderThreads int
}

func DefaultConfig() Config {
	return Config{
		SenderThreads: 1,
	}
}
