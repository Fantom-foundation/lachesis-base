package cachescale

import (
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

type Func interface {
	I(int) int
	I32(int32) int32
	I64(int64) int64
	U(uint) uint
	U32(uint32) uint32
	U64(uint64) uint64
	F32(float32) float32
	F64(float64) float64
	Events(v idx.Event) idx.Event
	Blocks(v idx.Block) idx.Block
	Frames(v idx.Frame) idx.Frame
}
