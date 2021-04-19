package abft

import "github.com/Fantom-foundation/lachesis-base/utils/cachescale"

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

// StoreCacheConfig is a cache config for store db.
type StoreCacheConfig struct {
	// Cache size for Roots.
	RootsNum    uint
	RootsFrames int
}

// StoreConfig is a config for store db.
type StoreConfig struct {
	Cache StoreCacheConfig
}

// DefaultStoreConfig for livenet.
func DefaultStoreConfig(scale cachescale.Func) StoreConfig {
	return StoreConfig{
		StoreCacheConfig{
			RootsNum:    scale.U(1000),
			RootsFrames: scale.I(100),
		},
	}
}

// LiteStoreConfig is for tests or inmemory.
func LiteStoreConfig() StoreConfig {
	return StoreConfig{
		StoreCacheConfig{
			RootsNum:    50,
			RootsFrames: 10,
		},
	}
}
