package multidb

import (
	"errors"
	"testing"

	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/status-im/keycard-go/hexutils"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	const (
		dbname1 = "db1"
		dbname2 = "db2"
	)

	var (
		MetadataPrefix = hexutils.HexToBytes("0068c2927bf842c3e9e2f1364494a33a752db334b9a819534bc9f17d2c3b4e5970008ff519d35a86f29fcaa5aae706b75dee871f65f174fcea1747f2915fc92158f6bfbf5eb79f65d16225738594bffb")
		TablesKey      = append(common.CopyBytes(MetadataPrefix), 0x0d)
	)

	t.Run("Empty producer", func(t *testing.T) {
		_, err := NewProducer(nil, nil, nil)
		if assert.Error(t, err) {
			assert.Equal(t, errors.New("default route must always be defined"), err)
		}
	})

	t.Run("Multi producer route", func(t *testing.T) {
		dbs := memorydb.NewProducer("")
		pool := flushable.NewSyncedPool(dbs, []byte("flushID"))

		producers := map[TypeName]kvdb.FullDBProducer{
			"": pool,
		}

		route := make(map[string]Route)
		route[""] = Route{}

		multiProducer, err := NewProducer(producers, route, nil)
		assert.Nil(t, err)

		assert.Equal(t, Route{}, multiProducer.RouteOf(""))
	})

	t.Run("Multi producer db", func(t *testing.T) {
		dbs := memorydb.NewProducer("")
		pool := flushable.NewSyncedPool(dbs, []byte("flushID"))

		_, err := pool.GetUnderlying(dbname1)
		assert.Nil(t, err)

		producers := map[TypeName]kvdb.FullDBProducer{
			"": pool,
		}

		route := make(map[string]Route)
		route[""] = Route{Name: dbname1}

		multiProducer, err := NewProducer(producers, route, TablesKey)
		assert.Nil(t, err)

		var flushID []byte
		flushID, err = multiProducer.Initialize([]string{dbname1}, flushID)
		assert.Nil(t, err)
		assert.Equal(t, []byte(nil), flushID)

		assert.Equal(t, Route{Name: dbname1}, multiProducer.RouteOf(""))

		db, err := multiProducer.OpenDB("")
		assert.Nil(t, err)

		err = db.Put([]byte("test"), []byte("test"))
		assert.Nil(t, err)

		records, err := multiProducer.getRecords()
		assert.Nil(t, err)

		for _, v := range records {
			assert.Equal(t, 1, len(v))
		}

		err = db.Close()
		assert.Nil(t, err)

		_, err = multiProducer.OpenDB("")
		assert.Nil(t, err)

		size := multiProducer.NotFlushedSizeEst()
		assert.Equal(t, 349, size)

		err = multiProducer.Flush(nil)
		assert.Nil(t, err)

		size = multiProducer.NotFlushedSizeEst()
		assert.Equal(t, 0, size)
	})

	t.Run("Multi producer 2 db", func(t *testing.T) {
		dbs := memorydb.NewProducer("")
		pool := flushable.NewSyncedPool(dbs, []byte("flushID"))

		_, err := pool.GetUnderlying(dbname1)
		assert.Nil(t, err)
		_, err = pool.GetUnderlying(dbname2)
		assert.Nil(t, err)

		producers := map[TypeName]kvdb.FullDBProducer{
			"": pool,
		}

		route := make(map[string]Route)
		route[""] = Route{Name: dbname1}
		route["%%"] = Route{Name: dbname2}

		multiProducer, err := NewProducer(producers, route, TablesKey)
		assert.Nil(t, err)

		_, err = multiProducer.OpenDB(dbname1)
		assert.Nil(t, err)
	})

	t.Run("Multi producer verify", func(t *testing.T) {
		dbs := memorydb.NewProducer("")
		pool := flushable.NewSyncedPool(dbs, []byte("flushID"))

		_, err := pool.GetUnderlying(dbname1)
		assert.Nil(t, err)

		producers := map[TypeName]kvdb.FullDBProducer{
			"": pool,
		}

		route := make(map[string]Route)
		route[""] = Route{Name: dbname1}

		multiProducer, err := NewProducer(producers, route, TablesKey)
		assert.Nil(t, err)

		db, err := multiProducer.OpenDB("")
		assert.Nil(t, err)

		err = db.Put([]byte("test"), []byte("test"))
		assert.Nil(t, err)

		err = multiProducer.Verify()
		assert.Nil(t, err)

		names := multiProducer.Names()
		assert.Equal(t, []string{dbname1}, names)
	})
}
