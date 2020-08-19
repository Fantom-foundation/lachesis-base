package idx

import (
	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
)

type (
	// Validator numeration.
	Validator uint32
)

// Bytes gets the byte representation of the index.
func (v Validator) Bytes() []byte {
	return bigendian.Uint32ToBytes(uint32(v))
}

// BytesToValidator converts bytes to validator index.
func BytesToValidator(b []byte) Validator {
	return Validator(bigendian.BytesToUint32(b))
}
