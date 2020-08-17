package abft

type Config struct {
}

// DefaultStoreConfig for livenet.
func DefaultConfig() Config {
	return Config{}
}

// LiteStoreConfig is for tests or inmemory.
func LiteConfig() Config {
	return Config{}
}

// StoreConfig is a config for store db.
type StoreConfig struct {
	// Cache size for Roots.
	Roots int
}

// DefaultStoreConfig for livenet.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		Roots: 20,
	}
}

// LiteStoreConfig is for tests or inmemory.
func LiteStoreConfig() StoreConfig {
	return StoreConfig{
		Roots: 5,
	}
}
