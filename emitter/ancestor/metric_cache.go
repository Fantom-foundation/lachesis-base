package ancestor

import (
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/utils/wlru"
)

type Metric uint64

type MetricCache struct {
	metricFn func(hash.Event) Metric
	cache    *wlru.Cache
}

func NewMetricFnCache(metricFn func(hash.Event) Metric, cacheSize int) *MetricCache {
	cache, _ := wlru.New(uint(cacheSize), cacheSize)
	return &MetricCache{metricFn, cache}
}

func (c *MetricCache) GetMetricOf(id hash.Event) Metric {
	if m, ok := c.cache.Get(id); ok {
		return m.(Metric)
	}
	m := c.metricFn(id)
	c.cache.Add(id, m, 1)
	return m
}
