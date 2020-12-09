package memorydb

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type Mod func(kvdb.DropableStore) kvdb.DropableStore

type Producer struct {
	fs   *fakeFS
	mods []Mod
}

// NewProducer of memory db.
func NewProducer(namespace string, mods ...Mod) kvdb.IterableDBProducer {
	return &Producer{
		fs:   newFakeFS(namespace),
		mods: mods,
	}
}

// Names of existing databases.
func (p *Producer) Names() []string {
	return p.fs.ListFakeDBs()
}

// OpenDB or create db with name.
func (p *Producer) OpenDB(name string) (kvdb.DropableStore, error) {
	db := p.fs.OpenFakeDB(name)

	for _, mod := range p.mods {
		db = mod(db)
	}

	return db, nil
}
