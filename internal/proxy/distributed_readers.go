package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

const (
	minimumReaderLeaseTTL  = 3 * time.Second
	remoteReaderPollPeriod = 100 * time.Millisecond
)

// acquireDistributedReader registers a remote replica's reader with the
// shared journal. Owner-local readers remain tracked directly by streamRuntime.
func (s *durableService) acquireDistributedReader(
	ctx context.Context,
	id journal.StreamID,
) (func(), error) {
	leases, ok := s.journal.(journal.ReaderLeaseStore)
	if !ok {
		return func() {}, nil
	}
	readerID, err := journal.NewStreamID()
	if err != nil {
		return nil, fmt.Errorf("generate reader lease ID: %w", err)
	}
	ttl := max(minimumReaderLeaseTTL, 3*s.config.ReadinessTimeout)
	if err := leases.AcquireReader(ctx, id, readerID.String(), ttl); err != nil {
		return nil, err
	}

	heartbeatContext, cancelHeartbeat := context.WithCancel(s.rootContext)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				refreshContext, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
				refreshed, refreshErr := leases.RefreshReader(
					refreshContext, id, readerID.String(), ttl,
				)
				cancel()
				if refreshErr != nil {
					s.logger.Warn("refresh distributed reader lease",
						"stream_id", safeLogString(id.String()),
						"reader_id", safeLogString(readerID.String()),
						"error", safeLogError(refreshErr))
					continue
				}
				if !refreshed {
					acquireContext, acquireCancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
					acquireErr := leases.AcquireReader(acquireContext, id, readerID.String(), ttl)
					acquireCancel()
					if acquireErr != nil {
						s.logger.Warn("reacquire distributed reader lease",
							"stream_id", safeLogString(id.String()),
							"reader_id", safeLogString(readerID.String()),
							"error", safeLogError(acquireErr))
					}
				}
			case <-heartbeatContext.Done():
				return
			}
		}
	}()

	var once sync.Once
	release := func() {
		once.Do(func() {
			cancelHeartbeat()
			<-heartbeatDone
			releaseContext, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
			defer cancel()
			if err := leases.ReleaseReader(releaseContext, id, readerID.String()); err != nil {
				s.logger.Warn("release distributed reader lease",
					"stream_id", safeLogString(id.String()),
					"reader_id", safeLogString(readerID.String()),
					"error", safeLogError(err))
			}
		})
	}
	return release, nil
}

func (s *durableService) activeDistributedReaders(id journal.StreamID) (int64, error) {
	leases, ok := s.journal.(journal.ReaderLeaseStore)
	if !ok {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
	defer cancel()
	return leases.ActiveReaders(ctx, id)
}

// tryClaimDistributedOrphan uses the same Redis transaction boundary as
// remote reader acquisition. Its lease is at least three operation deadlines:
// even if the claim response consumes one full deadline, the subsequent
// bounded journal Close cannot outlive the remaining fence.
func (s *durableService) tryClaimDistributedOrphan(
	id journal.StreamID,
) (claimID string, supported bool, claimed bool, err error) {
	claims, ok := s.journal.(journal.OrphanCancellationStore)
	if !ok {
		return "", false, false, nil
	}
	idValue, err := journal.NewStreamID()
	if err != nil {
		return "", true, false, fmt.Errorf("generate orphan cancellation claim ID: %w", err)
	}
	claimID = idValue.String()
	claimTTL := max(minimumReaderLeaseTTL, 3*s.config.ReadinessTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
	defer cancel()
	claimed, err = claims.TryClaimOrphan(ctx, id, claimID, claimTTL)
	return claimID, true, claimed, err
}

func (s *durableService) releaseDistributedOrphanClaim(
	id journal.StreamID,
	claimID string,
) error {
	claims, ok := s.journal.(journal.OrphanCancellationStore)
	if !ok || claimID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
	defer cancel()
	_, err := claims.ReleaseOrphanClaim(ctx, id, claimID)
	return err
}
