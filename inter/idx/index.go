package idx

import (
	"github.com/Fantom-foundation/go-lachesis/common/bigendian"
)

type (
	// Epoch numeration.
	Epoch uint32

	// Event numeration.
	Event uint32

	// Block numeration.
	Block uint64

	// Lamport numeration.
	Lamport uint32

	// Frame numeration.
	Frame uint32

	// Pack numeration.
	Pack uint32

	// StakerID numeration.
	StakerID uint32
)

// Bytes gets the byte representation of the index.
func (e Epoch) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(e))
}

// Bytes gets the byte representation of the index.
func (e Event) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(e))
}

// Bytes gets the byte representation of the index.
func (b Block) Bytes() []byte {
	return bigendian.Uint64ToBytes(uint64(b))
}

// Bytes gets the byte representation of the index.
func (l Lamport) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(l))
}

// Bytes gets the byte representation of the index.
func (p Pack) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(p))
}

// Bytes gets the byte representation of the index.
func (s StakerID) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(s))
}

// Bytes gets the byte representation of the index.
func (f Frame) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(f))
}

// BytesToEpoch converts bytes to epoch index.
func BytesToEpoch(b []byte) Epoch {
	return Epoch(bigendian.BytesToUint32(b))
}

// BytesToEvent converts bytes to event index.
func BytesToEvent(b []byte) Event {
	return Event(bigendian.BytesToUint32(b))
}

// BytesToBlock converts bytes to block index.
func BytesToBlock(b []byte) Block {
	return Block(bigendian.BytesToUint64(b))
}

// BytesToLamport converts bytes to block index.
func BytesToLamport(b []byte) Lamport {
	return Lamport(bigendian.BytesToUint32(b))
}

// BytesToFrame converts bytes to block index.
func BytesToFrame(b []byte) Frame {
	return Frame(bigendian.BytesToUint32(b))
}

// BytesToPack converts bytes to block index.
func BytesToPack(b []byte) Pack {
	return Pack(bigendian.BytesToUint32(b))
}

// BytesToStakerID converts bytes to staker index.
func BytesToStakerID(b []byte) StakerID {
	return StakerID(bigendian.BytesToUint32(b))
}

// MaxLamport return max value
func MaxLamport(x, y Lamport) Lamport {
	if x > y {
		return x
	}
	return y
}
