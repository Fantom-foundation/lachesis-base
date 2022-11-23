package abft

import "github.com/Fantom-foundation/lachesis-base/inter/idx"

func (s *Store) setLookupEnabled(epoch idx.Epoch) {
	err := s.table.LookupEnabled.Put(epoch.Bytes(), []byte{1})
	if err != nil {
		s.crit(err)
	}
}

func (s *Store) isLookupEnabled(epoch idx.Epoch) bool {
	w, err := s.table.LookupEnabled.Get(epoch.Bytes())
	if err != nil {
		s.crit(err)
	}
	if w == nil {
		return false
	}
	return true
}
