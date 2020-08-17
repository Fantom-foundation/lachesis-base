package bigendian

import "encoding/binary"

// Uint64ToBytes converts uint64 to bytes.
func Uint64ToBytes(n uint64) []byte {
	var res [8]byte
	binary.BigEndian.PutUint64(res[:], n)
	return res[:]
}

// BytesToUint64 converts uint64 from bytes.
func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Uint32ToBytes converts uint32 to bytes.
func Uint32ToBytes(n uint32) []byte {
	var res [4]byte
	binary.BigEndian.PutUint32(res[:], n)
	return res[:]
}

// BytesToUint32 converts uint32 from bytes.
func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
