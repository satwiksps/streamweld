package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrInvalidIdempotencyKey indicates that an empty raw key was supplied.
	ErrInvalidIdempotencyKey = errors.New("journal: invalid idempotency key")

	// ErrInvalidIdempotencyTTL indicates that a mapping lifetime was not
	// positive.
	ErrInvalidIdempotencyTTL = errors.New("journal: invalid idempotency TTL")

	// ErrIdempotencyReservationLost means a pending key-to-stream reservation
	// expired or was replaced before its journal was opened.
	ErrIdempotencyReservationLost = errors.New("journal: idempotency reservation lost")
)

// IdempotencyDigest is the opaque SHA-256 identity of a client-supplied key.
// Registries retain and operate on this digest after initial resolution so raw
// keys never need to remain in memory for the lifetime of a stream.
type IdempotencyDigest [sha256.Size]byte

// String returns the lowercase hexadecimal digest. It never returns the raw
// idempotency key.
func (digest IdempotencyDigest) String() string {
	return hex.EncodeToString(digest[:])
}

// DigestKey hashes a non-empty raw idempotency key for subsequent refresh or
// removal operations.
func DigestKey(key string) (IdempotencyDigest, error) {
	if key == "" {
		return IdempotencyDigest{}, ErrInvalidIdempotencyKey
	}
	return IdempotencyDigest(sha256.Sum256([]byte(key))), nil
}

// IdempotencyBinding is the atomic result of resolving a raw key.
type IdempotencyBinding struct {
	ID      StreamID
	Digest  IdempotencyDigest
	Created bool
}

// IdempotencyRegistry defines the key-to-stream operations shared by memory
// and future distributed implementations.
type IdempotencyRegistry interface {
	ResolveOrCreate(ctx context.Context, key string, newID StreamID, ttl time.Duration) (IdempotencyBinding, error)
	Refresh(ctx context.Context, digest IdempotencyDigest, ttl time.Duration) (bool, error)
	Remove(ctx context.Context, digest IdempotencyDigest) (bool, error)
	Expire(ctx context.Context) (int, error)
}

// PendingIdempotencyRegistry is an optional distributed-registry extension.
// A newly created binding starts as a short reservation: its creator renews it
// while opening the journal, then Journal.Open atomically promotes it. Release
// is conditional on both digest and stream ID so a stale creator cannot delete
// a newer winner's reservation.
type PendingIdempotencyRegistry interface {
	RefreshPending(ctx context.Context, digest IdempotencyDigest, id StreamID, ttl time.Duration) (bool, error)
	ReleasePending(ctx context.Context, digest IdempotencyDigest, id StreamID) (bool, error)
}

// ConditionalIdempotencyRegistry removes stale ready bindings without racing
// a replacement winner. Implementations must compare both digest and stream ID
// atomically and must not remove a pending reservation.
type ConditionalIdempotencyRegistry interface {
	RemoveIfBound(ctx context.Context, digest IdempotencyDigest, id StreamID) (bool, error)
}

type idempotencyEntry struct {
	id        StreamID
	expiresAt time.Time
}

// MemoryIdempotencyRegistry is a concurrency-safe in-process registry. Its map
// keys are SHA-256 digests; raw client keys are never stored.
type MemoryIdempotencyRegistry struct {
	initialize sync.Once
	gate       chan struct{}
	now        func() time.Time
	entries    map[IdempotencyDigest]idempotencyEntry
}

// NewMemoryIdempotencyRegistry returns an empty registry. A nil clock uses
// time.Now. Supplying a clock makes expiry behavior deterministic in tests.
func NewMemoryIdempotencyRegistry(now func() time.Time) *MemoryIdempotencyRegistry {
	if now == nil {
		now = time.Now
	}
	return &MemoryIdempotencyRegistry{
		now:     now,
		entries: make(map[IdempotencyDigest]idempotencyEntry),
	}
}

// ResolveOrCreate atomically resolves an unexpired key or binds it to newID.
// Resolving an existing key does not extend its lifetime; journal activity
// calls Refresh with the returned digest so the mapping follows stream expiry.
func (r *MemoryIdempotencyRegistry) ResolveOrCreate(
	ctx context.Context,
	key string,
	newID StreamID,
	ttl time.Duration,
) (IdempotencyBinding, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return IdempotencyBinding{}, err
	}
	digest, err := DigestKey(key)
	if err != nil {
		return IdempotencyBinding{}, err
	}
	if err := newID.Validate(); err != nil {
		return IdempotencyBinding{}, fmt.Errorf("bind idempotency key: %w", err)
	}
	if err := validateIdempotencyTTL(ttl); err != nil {
		return IdempotencyBinding{}, err
	}

	if err := r.acquire(ctx); err != nil {
		return IdempotencyBinding{}, err
	}
	defer r.release()
	now := r.now()
	if err := checkIdempotencyContext(ctx); err != nil {
		return IdempotencyBinding{}, err
	}
	if entry, ok := r.entries[digest]; ok {
		if now.Before(entry.expiresAt) {
			return IdempotencyBinding{ID: entry.id, Digest: digest}, nil
		}
		delete(r.entries, digest)
	}

	r.entries[digest] = idempotencyEntry{id: newID, expiresAt: now.Add(ttl)}
	return IdempotencyBinding{ID: newID, Digest: digest, Created: true}, nil
}

// Refresh resets an unexpired mapping's lifetime to ttl from the injected
// clock. It returns false when the digest is missing or already expired.
func (r *MemoryIdempotencyRegistry) Refresh(
	ctx context.Context,
	digest IdempotencyDigest,
	ttl time.Duration,
) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := validateIdempotencyTTL(ttl); err != nil {
		return false, err
	}

	if err := r.acquire(ctx); err != nil {
		return false, err
	}
	defer r.release()
	now := r.now()
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	entry, ok := r.entries[digest]
	if !ok {
		return false, nil
	}
	if !now.Before(entry.expiresAt) {
		delete(r.entries, digest)
		return false, nil
	}
	entry.expiresAt = now.Add(ttl)
	r.entries[digest] = entry
	return true, nil
}

// Remove deletes a mapping by digest and reports whether it existed.
func (r *MemoryIdempotencyRegistry) Remove(ctx context.Context, digest IdempotencyDigest) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := r.acquire(ctx); err != nil {
		return false, err
	}
	defer r.release()
	if _, ok := r.entries[digest]; !ok {
		return false, nil
	}
	delete(r.entries, digest)
	return true, nil
}

// RemoveIfBound atomically removes digest only when it still names id.
func (r *MemoryIdempotencyRegistry) RemoveIfBound(
	ctx context.Context,
	digest IdempotencyDigest,
	id StreamID,
) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := id.Validate(); err != nil {
		return false, fmt.Errorf("conditionally remove idempotency key: %w", err)
	}
	if err := r.acquire(ctx); err != nil {
		return false, err
	}
	defer r.release()
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	entry, ok := r.entries[digest]
	if !ok || entry.id != id {
		return false, nil
	}
	delete(r.entries, digest)
	return true, nil
}

// Expire removes mappings whose lifetime has elapsed and returns their count.
func (r *MemoryIdempotencyRegistry) Expire(ctx context.Context) (int, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return 0, err
	}
	if err := r.acquire(ctx); err != nil {
		return 0, err
	}
	defer r.release()
	now := r.now()
	if err := checkIdempotencyContext(ctx); err != nil {
		return 0, err
	}
	return r.expireLocked(ctx, now)
}

func (r *MemoryIdempotencyRegistry) acquire(ctx context.Context) error {
	if err := checkIdempotencyContext(ctx); err != nil {
		return err
	}
	r.initialize.Do(func() {
		if r.now == nil {
			r.now = time.Now
		}
		if r.entries == nil {
			r.entries = make(map[IdempotencyDigest]idempotencyEntry)
		}
		r.gate = make(chan struct{}, 1)
		r.gate <- struct{}{}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.gate:
	}
	if err := checkIdempotencyContext(ctx); err != nil {
		r.release()
		return err
	}
	return nil
}

func (r *MemoryIdempotencyRegistry) release() {
	r.gate <- struct{}{}
}

func (r *MemoryIdempotencyRegistry) expireLocked(ctx context.Context, now time.Time) (int, error) {
	expired := 0
	for digest, entry := range r.entries {
		if err := ctx.Err(); err != nil {
			return expired, err
		}
		if now.Before(entry.expiresAt) {
			continue
		}
		delete(r.entries, digest)
		expired++
	}
	return expired, nil
}

func validateIdempotencyTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidIdempotencyTTL
	}
	return nil
}

func checkIdempotencyContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

var _ IdempotencyRegistry = (*MemoryIdempotencyRegistry)(nil)
var _ ConditionalIdempotencyRegistry = (*MemoryIdempotencyRegistry)(nil)
