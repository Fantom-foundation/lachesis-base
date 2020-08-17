package ancestor

import (
	"sort"

	"github.com/Fantom-foundation/go-lachesis/abft/dagidx"
	"github.com/Fantom-foundation/go-lachesis/hash"
	"github.com/Fantom-foundation/go-lachesis/inter/idx"
	"github.com/Fantom-foundation/go-lachesis/inter/pos"
)

/*
 * CasualityStrategy
 */

// CasualityStrategy uses vector clock to check which parents observe "more" than others
// The strategy uses "observing more" as a search criteria
type CasualityStrategy struct {
	dagi       DagIndex
	template   dagidx.HighestBeforeSeq
	validators *pos.Validators
}

type DagIndex interface {
	GetHighestBeforeSeq(id hash.Event) dagidx.HighestBeforeSeq
}

// NewCasualityStrategy creates new CasualityStrategy with provided vector clock
func NewCasualityStrategy(dagIndex DagIndex, validators *pos.Validators) *CasualityStrategy {
	return &CasualityStrategy{
		dagi:       dagIndex,
		validators: validators,
	}
}

type eventScore struct {
	event hash.Event
	score idx.Event
	vec   dagidx.HighestBeforeSeq
}

// Init must be called before using the strategy
func (st *CasualityStrategy) Init(selfParent *hash.Event) {
	if selfParent != nil {
		// we start searching by comparing with self-parent
		st.template = st.dagi.GetHighestBeforeSeq(*selfParent)
	}
}

// Find chooses the hash from the specified options
func (st *CasualityStrategy) Find(options hash.Events) hash.Event {
	if st.template == nil { // nothing observes
		st.template = st.dagi.GetHighestBeforeSeq(options[0])
	}
	scores := make([]eventScore, 0, 50)

	// estimate score of each option as number of validators it observes higher than provided template
	for _, id := range options {
		score := eventScore{}
		score.event = id
		score.vec = st.dagi.GetHighestBeforeSeq(id)
		for creatorIdx := idx.Validator(0); creatorIdx < idx.Validator(st.validators.Len()); creatorIdx++ {
			my := st.template.Get(creatorIdx)
			his := score.vec.Get(creatorIdx)

			// observes higher
			if his.Seq() > my.Seq() && !my.IsForkDetected() {
				score.score++
			}
			// observes a fork
			if his.IsForkDetected() && !my.IsForkDetected() {
				score.score++
			}
		}
		scores = append(scores, score)
	}

	// take the option with best score
	sort.Slice(scores, func(i, j int) bool {
		a, b := scores[i], scores[j]
		return a.score < b.score
	})
	// memorize its template for next calls
	st.template = scores[0].vec
	return scores[0].event
}
