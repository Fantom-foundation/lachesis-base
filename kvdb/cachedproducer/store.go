package cachedproducer

import "github.com/Fantom-foundation/lachesis-base/kvdb"

type StoreWithFn struct {
	kvdb.Store
	CloseFn func() error
	DropFn  func()
}

func (s *StoreWithFn) Close() error {
	return s.CloseFn()
}

func (s *StoreWithFn) Drop() {
	s.DropFn()
}
