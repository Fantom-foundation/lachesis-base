package ancestor

import (
	"github.com/Fantom-foundation/go-lachesis/hash"
)

// SearchStrategy defines a criteria used to estimate the "best" subset of parents to emit event with.
type SearchStrategy interface {
	// Init must be called before using the strategy
	Init(selfParent *hash.Event)
	// Find chooses the hash from the specified options
	Find(options hash.Events) hash.Event
}

// FindBestParents returns estimated parents subset, according to provided strategy
// max is max num of parents to link with (including self-parent)
// returns set of parents to link, len(res) <= max
func FindBestParents(max int, options hash.Events, selfParent *hash.Event, strategy SearchStrategy) (*hash.Event, hash.Events) {
	optionsSet := options.Set()
	parents := make(hash.Events, 0, max)
	if selfParent != nil {
		parents = append(parents, *selfParent)
		optionsSet.Erase(*selfParent)
	}

	strategy.Init(selfParent)

	for len(parents) < max && len(optionsSet) > 0 {
		best := strategy.Find(optionsSet.Slice())
		parents = append(parents, best)
		optionsSet.Erase(best)
	}

	return selfParent, parents
}
