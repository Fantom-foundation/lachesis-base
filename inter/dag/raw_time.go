package dag

import (
	"github.com/Fantom-foundation/go-lachesis/common/bigendian"
)

type (
	// RawTimestamp is an universal raw timestamp.
	// It's not defined how translate this number to a date (e.g. may be stored in seconds or nanoseconds).
	RawTimestamp uint64
)

// Bytes gets the byte representation of the index.
func (t RawTimestamp) Bytes() []byte {
	return bigendian.Uint64ToBytes(uint64(t))
}

// BytesToRawTimestamp converts bytes to timestamp.
func BytesToRawTimestamp(b []byte) RawTimestamp {
	return RawTimestamp(bigendian.BytesToUint64(b))
}
