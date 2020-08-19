package pos

import (
	"math"
	"math/big"
	"testing"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

func TestNewValidators(t *testing.T) {
	b := NewBuilder()

	assert.NotNil(t, b)
	assert.NotNil(t, b.Build())

	assert.Equal(t, 0, b.Build().Len())
}

func TestValidators_Set(t *testing.T) {
	b := NewBuilder()

	b.Set(1, 1)
	b.Set(2, 2)
	b.Set(3, 3)
	b.Set(4, 4)
	b.Set(5, 5)

	v := b.Build()

	assert.Equal(t, 5, v.Len())
	assert.Equal(t, Weight(15), v.TotalWeight())

	b.Set(1, 10)
	b.Set(3, 30)

	v = b.Build()

	assert.Equal(t, 5, v.Len())
	assert.Equal(t, Weight(51), v.TotalWeight())

	b.Set(2, 0)
	b.Set(5, 0)

	v = b.Build()

	assert.Equal(t, 3, v.Len())
	assert.Equal(t, Weight(44), v.TotalWeight())

	b.Set(4, 0)
	b.Set(3, 0)
	b.Set(1, 0)

	v = b.Build()

	assert.Equal(t, 0, v.Len())
	assert.Equal(t, Weight(0), v.TotalWeight())
}

func TestValidators_Get(t *testing.T) {
	b := NewBuilder()

	b.Set(0, 1)
	b.Set(2, 2)
	b.Set(3, 3)
	b.Set(4, 4)
	b.Set(7, 5)

	v := b.Build()

	assert.Equal(t, Weight(1), v.Get(0))
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(2), v.Get(2))
	assert.Equal(t, Weight(3), v.Get(3))
	assert.Equal(t, Weight(4), v.Get(4))
	assert.Equal(t, Weight(0), v.Get(5))
	assert.Equal(t, Weight(0), v.Get(6))
	assert.Equal(t, Weight(5), v.Get(7))
}

func TestValidators_Iterate(t *testing.T) {
	b := NewBuilder()

	b.Set(1, 1)
	b.Set(2, 2)
	b.Set(3, 3)
	b.Set(4, 4)
	b.Set(5, 5)

	v := b.Build()

	count := 0
	sum := 0

	for _, id := range v.IDs() {
		count++
		sum += int(v.Get(id))
	}

	assert.Equal(t, 5, count)
	assert.Equal(t, 15, sum)
}

func TestValidators_Copy(t *testing.T) {
	b := NewBuilder()

	b.Set(1, 1)
	b.Set(2, 2)
	b.Set(3, 3)
	b.Set(4, 4)
	b.Set(5, 5)

	v := b.Build()
	vv := v.Copy()

	assert.Equal(t, v.values, vv.values)

	assert.NotEqual(t, unsafe.Pointer(&v.values), unsafe.Pointer(&vv.values))
	assert.NotEqual(t, unsafe.Pointer(&v.cache.indexes), unsafe.Pointer(&vv.cache.indexes))
	assert.NotEqual(t, unsafe.Pointer(&v.cache.ids), unsafe.Pointer(&vv.cache.ids))
	assert.NotEqual(t, unsafe.Pointer(&v.cache.weights), unsafe.Pointer(&vv.cache.weights))
}

func maxBig(n uint) *big.Int {
	max := new(big.Int).Lsh(common.Big1, n)
	max.Sub(max, common.Big1)
	return max
}

func TestValidators_Big(t *testing.T) {
	b := NewBigBuilder()

	b.Set(1, big.NewInt(1))
	v := b.Build()
	assert.Equal(t, Weight(1), v.TotalWeight())
	assert.Equal(t, Weight(1), v.Get(1))

	b.Set(2, big.NewInt(math.MaxInt64-1))
	v = b.Build()
	assert.Equal(t, Weight(math.MaxInt64), v.TotalWeight())
	assert.Equal(t, Weight(1), v.Get(1))
	assert.Equal(t, Weight(math.MaxInt64-1), v.Get(2))

	b.Set(3, big.NewInt(1))
	v = b.Build()
	assert.Equal(t, Weight(math.MaxInt64/2), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(math.MaxInt64/2), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))

	b.Set(4, big.NewInt(2))
	v = b.Build()
	assert.Equal(t, Weight(math.MaxInt64/2+1), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(math.MaxInt64/2), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))
	assert.Equal(t, Weight(1), v.Get(4))

	b.Set(5, maxBig(96))
	v = b.Build()
	assert.Equal(t, Weight(0x400000001ffffffe), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(0x1fffffff), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))
	assert.Equal(t, Weight(0), v.Get(4))
	assert.Equal(t, Weight(math.MaxInt64/2), v.Get(5))

	b.Set(1, maxBig(501))
	b.Set(2, maxBig(502))
	b.Set(3, maxBig(503))
	b.Set(4, maxBig(504))
	b.Set(5, maxBig(515))
	v = b.Build()
	assert.Equal(t, Weight(0x400efffffffffffb), v.TotalWeight())
	assert.Equal(t, Weight(0xffffffffffff), v.Get(1))
	assert.Equal(t, Weight(0x1ffffffffffff), v.Get(2))
	assert.Equal(t, Weight(0x3ffffffffffff), v.Get(3))
	assert.Equal(t, Weight(0x7ffffffffffff), v.Get(4))
	assert.Equal(t, Weight(0x3fffffffffffffff), v.Get(5))

	for v := idx.ValidatorID(1); v < 5000; v++ {
		b.Set(v, new(big.Int).Mul(big.NewInt(int64(v)), maxBig(400)))
	}
	v = b.Build()
	assert.Equal(t, Weight(0x5f592dffffffec79), v.TotalWeight())
	assert.Equal(t, Weight(549755813887), v.Get(1))
	assert.Equal(t, Weight(1099511627775), v.Get(2))
	assert.Equal(t, Weight(1649267441663), v.Get(3))
	assert.Equal(t, Weight(1374389534719999), v.Get(2500))
	assert.Equal(t, Weight(2747679557812223), v.Get(4998))
	assert.Equal(t, Weight(2748229313626111), v.Get(4999))
}
