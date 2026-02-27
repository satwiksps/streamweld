package proxy

import (
	"context"
	"errors"
	"time"
)

func (s *durableService) runHealthChecks(ctx context.Context) {
	probe := func() {
		results, err := s.backends.ProbeAll(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logger.Warn("probe backend pool", "error", err)
			}
			return
		}
		for _, result := range results {
			if len(result.Transition.Bindings) != 0 {
				s.triggerBindings("health", result.ID, result.Transition.Bindings)
			}
		}
	}

	ticker := time.NewTicker(s.config.BackendHealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe()
		}
	}
}
