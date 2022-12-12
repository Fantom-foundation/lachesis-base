package vecfc

import (
	"encoding/binary"
	"math"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

/*
 * Use binary form for optimization, to avoid serialization. As a result, DB cache works as elements cache.
 */

type (
	// LowestAfterSeq is a vector of lowest events (their Seq) which do observe the source event
	LowestAfterSeq []byte
	// HighestBeforeSeq is a vector of highest events (their Seq + MinSeq + Index) which are observed by source event
	HighestBeforeSeq []byte

	// BranchSeq encodes Seq, MinSeq, and LookupKey into 12 bytes
	BranchSeq struct {
		Seq       idx.Event
		MinSeq    idx.Event
		LookupKey idx.Event // key of the event in the vector-engine's EventLookup table
	}
)

// NewLowestAfterSeq creates new LowestAfterSeq vector.
func NewLowestAfterSeq(size idx.Validator) *LowestAfterSeq {
	b := make(LowestAfterSeq, size*4)
	return &b
}

// NewHighestBeforeSeq creates new HighestBeforeSeq vector.
func NewHighestBeforeSeq(size idx.Validator) *HighestBeforeSeq {
	b := make(HighestBeforeSeq, size*12)
	return &b
}

// Get i's position in the byte-encoded vector clock
func (b LowestAfterSeq) Get(i idx.Validator) idx.Event {
	for i >= b.Size() {
		return 0
	}
	return idx.Event(binary.LittleEndian.Uint32(b[i*4 : (i+1)*4]))
}

// Size of the vector clock
func (b LowestAfterSeq) Size() idx.Validator {
	return idx.Validator(len(b)) / 4
}

// Set i's position in the byte-encoded vector clock
func (b *LowestAfterSeq) Set(i idx.Validator, seq idx.Event) {
	for i >= b.Size() {
		// append zeros if exceeds size
		*b = append(*b, []byte{0, 0, 0, 0}...)
	}

	binary.LittleEndian.PutUint32((*b)[i*4:(i+1)*4], uint32(seq))
}

// Size of the vector clock
func (b HighestBeforeSeq) Size() int {
	return len(b) / 12
}

// Get i's position in the byte-encoded vector clock
func (b HighestBeforeSeq) Get(i idx.Validator) BranchSeq {
	for int(i) >= b.Size() {
		return BranchSeq{}
	}
	seq1 := binary.LittleEndian.Uint32(b[i*12 : i*12+4])
	seq2 := binary.LittleEndian.Uint32(b[i*12+4 : i*12+8])
	lk := binary.LittleEndian.Uint32(b[i*12+8 : i*12+12])
	return BranchSeq{
		Seq:       idx.Event(seq1),
		MinSeq:    idx.Event(seq2),
		LookupKey: idx.Event(lk),
	}
}

// Set i's position in the byte-encoded vector clock
func (b *HighestBeforeSeq) Set(i idx.Validator, seq BranchSeq) {
	for int(i) >= b.Size() {
		// append zeros if exceeds size
		*b = append(*b, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
	}
	binary.LittleEndian.PutUint32((*b)[i*12:i*12+4], uint32(seq.Seq))
	binary.LittleEndian.PutUint32((*b)[i*12+4:i*12+8], uint32(seq.MinSeq))
	binary.LittleEndian.PutUint32((*b)[i*12+8:i*12+12], uint32(seq.LookupKey))
}

var (
	// forkDetectedSeq is a special marker of observed fork by a creator
	forkDetectedSeq = BranchSeq{
		Seq:    0,
		MinSeq: idx.Event(math.MaxInt32),
	}
)

// IsForkDetected returns true if observed fork by a creator (in combination of branches)
func (seq BranchSeq) IsForkDetected() bool {
	return seq == forkDetectedSeq
}
