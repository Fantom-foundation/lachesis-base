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

	assert.Equal(t, idx.Validator(0), b.Build().Len())
}

func TestValidators_Set(t *testing.T) {
	b := NewBuilder()

	b.Set(1, 1)
	b.Set(2, 2)
	b.Set(3, 3)
	b.Set(4, 4)
	b.Set(5, 5)

	v := b.Build()

	assert.Equal(t, idx.Validator(5), v.Len())
	assert.Equal(t, Weight(15), v.TotalWeight())

	b.Set(1, 10)
	b.Set(3, 30)

	v = b.Build()

	assert.Equal(t, idx.Validator(5), v.Len())
	assert.Equal(t, Weight(51), v.TotalWeight())

	b.Set(2, 0)
	b.Set(5, 0)

	v = b.Build()

	assert.Equal(t, idx.Validator(3), v.Len())
	assert.Equal(t, Weight(44), v.TotalWeight())

	b.Set(4, 0)
	b.Set(3, 0)
	b.Set(1, 0)

	v = b.Build()

	assert.Equal(t, idx.Validator(0), v.Len())
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
	max := Weight(math.MaxUint32 >> 1)

	b := NewBigBuilder()

	b.Set(1, big.NewInt(1))
	v := b.Build()
	assert.Equal(t, Weight(1), v.TotalWeight())
	assert.Equal(t, Weight(1), v.Get(1))

	b.Set(2, big.NewInt(int64(max)-1))
	v = b.Build()
	assert.Equal(t, Weight(max), v.TotalWeight())
	assert.Equal(t, Weight(1), v.Get(1))
	assert.Equal(t, Weight(max-1), v.Get(2))

	b.Set(3, big.NewInt(1))
	v = b.Build()
	assert.Equal(t, Weight(max/2), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(max/2), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))

	b.Set(4, big.NewInt(2))
	v = b.Build()
	assert.Equal(t, Weight(max/2+1), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(max/2), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))
	assert.Equal(t, Weight(1), v.Get(4))

	b.Set(5, maxBig(60))
	v = b.Build()
	assert.Equal(t, Weight(0x40000000), v.TotalWeight())
	assert.Equal(t, Weight(0), v.Get(1))
	assert.Equal(t, Weight(0x1), v.Get(2))
	assert.Equal(t, Weight(0), v.Get(3))
	assert.Equal(t, Weight(0), v.Get(4))
	assert.Equal(t, Weight(max/2), v.Get(5))

	b.Set(1, maxBig(501))
	b.Set(2, maxBig(502))
	b.Set(3, maxBig(503))
	b.Set(4, maxBig(504))
	b.Set(5, maxBig(515))
	v = b.Build()
	assert.Equal(t, Weight(0x400efffb), v.TotalWeight())
	assert.Equal(t, Weight(0xffff), v.Get(1))
	assert.Equal(t, Weight(0x1ffff), v.Get(2))
	assert.Equal(t, Weight(0x3ffff), v.Get(3))
	assert.Equal(t, Weight(0x7ffff), v.Get(4))
	assert.Equal(t, Weight(0x3fffffff), v.Get(5))

	for v := idx.ValidatorID(1); v <= 5000; v++ {
		b.Set(v, new(big.Int).Mul(big.NewInt(int64(v)), maxBig(400)))
	}
	v = b.Build()
	assert.Equal(t, Weight(0x5f62de78), v.TotalWeight())
	assert.Equal(t, Weight(0x7f), v.Get(1))
	assert.Equal(t, Weight(0xff), v.Get(2))
	assert.Equal(t, Weight(0x17f), v.Get(3))
	assert.Equal(t, Weight(0x4e1ff), v.Get(2500))
	assert.Equal(t, Weight(0x9c37f), v.Get(4999))
	assert.Equal(t, Weight(0x9c3ff), v.Get(5000))
}
