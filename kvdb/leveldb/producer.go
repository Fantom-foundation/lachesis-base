package leveldb

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type Producer struct {
	datadir  string
	getCache func(string) int
}

// NewProducer of level db.
func NewProducer(datadir string, getCache func(string) int) kvdb.IterableDBProducer {
	return &Producer{
		datadir:  datadir,
		getCache: getCache,
	}
}

// Names of existing databases.
func (p *Producer) Names() []string {
	var names []string

	files, err := ioutil.ReadDir(p.datadir)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		names = append(names, f.Name())
	}
	return names
}

// OpenDB or create db with name.
func (p *Producer) OpenDB(name string) (kvdb.DropableStore, error) {
	path := p.resolvePath(name)

	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}

	onDrop := func() {
		_ = os.RemoveAll(path)
	}

	db, err := New(path, p.getCache(name), 0, nil, onDrop)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (p *Producer) resolvePath(name string) string {
	return filepath.Join(p.datadir, name)
}
