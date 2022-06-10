package skipkeys

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

func openDB(p kvdb.DBProducer, skipPrefix []byte, name string) (kvdb.Store, error) {
	store, err := p.OpenDB(name)
	if err != nil {
		return nil, err
	}
	return &Store{store, skipPrefix}, nil
}

type AllDBProducer struct {
	kvdb.FullDBProducer
	skipPrefix []byte
}

func WrapAllProducer(p kvdb.FullDBProducer, skipPrefix []byte) *AllDBProducer {
	return &AllDBProducer{
		FullDBProducer: p,
		skipPrefix:     skipPrefix,
	}
}

func (p *AllDBProducer) OpenDB(name string) (kvdb.Store, error) {
	return openDB(p.FullDBProducer, p.skipPrefix, name)
}

type DBProducer struct {
	kvdb.DBProducer
	skipPrefix []byte
}

func WrapProducer(p kvdb.DBProducer, skipPrefix []byte) *DBProducer {
	return &DBProducer{
		DBProducer: p,
		skipPrefix: skipPrefix,
	}
}

func (p *DBProducer) OpenDB(name string) (kvdb.Store, error) {
	return openDB(p.DBProducer, p.skipPrefix, name)
}
