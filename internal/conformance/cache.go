package conformance

import (
	"context"
	"sync"
)

// MemoryCache is a concurrency-safe in-process verdict cache suitable for the
// standalone proxy and tests. The operator can implement Cache with
// InferenceRoute status storage while reusing the same Checker.
type MemoryCache struct {
	mu      sync.RWMutex
	reports map[CacheKey]Report
}

// NewMemoryCache creates an empty verdict cache.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{reports: make(map[CacheKey]Report)}
}

// Get returns an isolated copy of a cached report.
func (cache *MemoryCache) Get(ctx context.Context, key CacheKey) (Report, bool, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, false, err
	}
	if cache == nil {
		return Report{}, false, nil
	}
	cache.mu.RLock()
	report, ok := cache.reports[key]
	cache.mu.RUnlock()
	if !ok {
		return Report{}, false, nil
	}
	return cloneReport(report), true, nil
}

// Put stores an isolated copy of report.
func (cache *MemoryCache) Put(ctx context.Context, key CacheKey, report Report) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cache == nil {
		return nil
	}
	report.Cached = false
	cache.mu.Lock()
	cache.reports[key] = cloneReport(report)
	cache.mu.Unlock()
	return nil
}
