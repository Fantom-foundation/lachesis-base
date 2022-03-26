package multidb

import (
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type TableRecord struct {
	Req   string
	Table string
}

func ReadTablesList(store kvdb.Store, tableRecordsKey []byte) (res []TableRecord, err error) {
	b, err := store.Get(tableRecordsKey)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return []TableRecord{}, nil
	}
	err = rlp.DecodeBytes(b, &res)
	return
}

func WriteTablesList(store kvdb.Store, tableRecordsKey []byte, records []TableRecord) error {
	b, err := rlp.EncodeToBytes(records)
	if err != nil {
		return err
	}
	return store.Put(tableRecordsKey, b)
}
