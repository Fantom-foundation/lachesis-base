package ancestor

import (
	"math/rand"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
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

// Choose chooses the hash from the specified options
func (st *RandomStrategy) Choose(_ hash.Events, options hash.Events) int {
	return st.r.Intn(len(options))
}
