package ancestor

import (
	"github.com/Fantom-foundation/lachesis-base/hash"
)

type MetricStrategy struct {
	metricFn func(hash.Event) Metric
}

func NewMetricStrategy(metricFn func(hash.Event) Metric) *MetricStrategy {
	return &MetricStrategy{metricFn}
}

// Choose chooses the hash from the specified options
func (st *MetricStrategy) Choose(_ hash.Events, options hash.Events) int {
	var maxI int
	var maxWeight Metric
	// find option with a maximum weight
	for i, opt := range options {
		weight := st.metricFn(opt)
		if maxWeight == 0 || weight > maxWeight {
			maxI = i
			maxWeight = weight
		}
	}
	return maxI
}
