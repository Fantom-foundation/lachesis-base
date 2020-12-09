package flushable

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
)

func TestSyncedPool(t *testing.T) {
	require := require.New(t)

	dbs := memorydb.NewProducer("")
	pool := NewSyncedPool(dbs)

	require.NotNil(pool)
}
