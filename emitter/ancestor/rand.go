package ancestor

import (
	"math/rand"
	"time"

	"github.com/Fantom-foundation/go-lachesis/hash"
)

/*
 * RandomStrategy
 */

// RandomStrategy is used in tests, when vector clock isn't available
type RandomStrategy struct {
	r *rand.Rand
}

func NewRandomStrategy(r *rand.Rand) *RandomStrategy {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &RandomStrategy{
		r: r,
	}
}

func (st *RandomStrategy) Init(myLast *hash.Event) {}

// Find chooses the hash from the specified options
func (st *RandomStrategy) Find(heads hash.Events) hash.Event {
	return heads[st.r.Intn(len(heads))]
}
