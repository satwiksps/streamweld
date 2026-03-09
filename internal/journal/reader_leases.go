package journal

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidReaderID means a distributed reader lease has no identity.
	ErrInvalidReaderID = errors.New("journal: invalid reader ID")
	// ErrInvalidReaderLeaseTTL means a reader heartbeat lifetime is not positive.
	ErrInvalidReaderLeaseTTL = errors.New("journal: invalid reader lease TTL")
	// ErrInvalidOrphanClaimID means an orphan-cancellation claim has no identity.
	ErrInvalidOrphanClaimID = errors.New("journal: invalid orphan claim ID")
	// ErrInvalidOrphanClaimTTL means a cancellation fence lifetime is not positive.
	ErrInvalidOrphanClaimTTL = errors.New("journal: invalid orphan claim TTL")
)

// ReaderLeaseStore is an optional distributed-reader registry. Redis journals
// implement it so the producer-owning proxy can distinguish a true orphan
// from a reader attached through another proxy replica.
type ReaderLeaseStore interface {
	AcquireReader(ctx context.Context, id StreamID, readerID string, ttl time.Duration) error
	RefreshReader(ctx context.Context, id StreamID, readerID string, ttl time.Duration) (bool, error)
	ReleaseReader(ctx context.Context, id StreamID, readerID string) error
	ActiveReaders(ctx context.Context, id StreamID) (int64, error)
}

// OrphanCancellationStore is an optional distributed cancellation fence. A
// successful claim linearizes atomically against AcquireReader: exactly one of
// a new reader lease or orphan cancellation may win while a stream is open.
// ReleaseOrphanClaim only removes a claim owned by claimID, making cleanup of
// an abandoned claim safe in the presence of a newer claimant.
type OrphanCancellationStore interface {
	TryClaimOrphan(ctx context.Context, id StreamID, claimID string, ttl time.Duration) (bool, error)
	ReleaseOrphanClaim(ctx context.Context, id StreamID, claimID string) (bool, error)
}
