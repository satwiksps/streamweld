package journal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDigestKeyIsDeterministicAndDoesNotExposeRawKey(t *testing.T) {
	t.Parallel()
	const rawKey = "customer-secret-retry-token"
	digest, err := DigestKey(rawKey)
	if err != nil {
		t.Fatal(err)
	}
	want := IdempotencyDigest(sha256.Sum256([]byte(rawKey)))
	if digest != want {
		t.Errorf("DigestKey() = %x, want %x", digest, want)
	}
	if got := digest.String(); len(got) != sha256.Size*2 || strings.Contains(got, rawKey) {
		t.Errorf("digest string exposed raw key or had wrong length: %q", got)
	}
	if _, err := DigestKey(""); !errors.Is(err, ErrInvalidIdempotencyKey) {
		t.Errorf("DigestKey(empty) error = %v, want ErrInvalidIdempotencyKey", err)
	}
}

func TestResolveOrCreateReturnsExistingWithoutRefreshing(t *testing.T) {
	t.Parallel()
	clock := newIdempotencyTestClock(time.Unix(1_700_000_000, 0))
	registry := NewMemoryIdempotencyRegistry(clock.Now)
	firstID := mustIdempotencyID(t, "00000000000000000000000001")
	secondID := mustIdempotencyID(t, "00000000000000000000000002")
	ctx := context.Background()

	first, err := registry.ResolveOrCreate(ctx, "same-key", firstID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.ID != firstID {
		t.Fatalf("first binding = %#v", first)
	}
	clock.Advance(9 * time.Minute)
	existing, err := registry.ResolveOrCreate(ctx, "same-key", secondID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if existing.Created || existing.ID != firstID || existing.Digest != first.Digest {
		t.Fatalf("existing binding = %#v, want first ID/digest", existing)
	}

	// A retry lookup does not extend the mapping beyond the stream's original
	// lifetime. At the exact expiry instant the new candidate wins.
	clock.Advance(time.Minute)
	recreated, err := registry.ResolveOrCreate(ctx, "same-key", secondID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !recreated.Created || recreated.ID != secondID {
		t.Fatalf("recreated binding = %#v", recreated)
	}
}

func TestRefreshTracksStreamLifetimeByDigest(t *testing.T) {
	t.Parallel()
	clock := newIdempotencyTestClock(time.Unix(1_700_000_000, 0))
	registry := NewMemoryIdempotencyRegistry(clock.Now)
	ctx := context.Background()
	firstID := mustIdempotencyID(t, "00000000000000000000000003")
	secondID := mustIdempotencyID(t, "00000000000000000000000004")
	binding, err := registry.ResolveOrCreate(ctx, "refresh-key", firstID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(9 * time.Minute)
	refreshed, err := registry.Refresh(ctx, binding.Digest, 10*time.Minute)
	if err != nil || !refreshed {
		t.Fatalf("Refresh() = (%v, %v), want (true, nil)", refreshed, err)
	}
	clock.Advance(9 * time.Minute)
	existing, err := registry.ResolveOrCreate(ctx, "refresh-key", secondID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if existing.Created || existing.ID != firstID {
		t.Fatalf("binding expired before refreshed stream lifetime: %#v", existing)
	}
	clock.Advance(time.Minute)
	recreated, err := registry.ResolveOrCreate(ctx, "refresh-key", secondID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !recreated.Created || recreated.ID != secondID {
		t.Fatalf("binding did not expire at refreshed deadline: %#v", recreated)
	}
}

func TestRemoveAndExpireOperateOnlyOnDigests(t *testing.T) {
	t.Parallel()
	clock := newIdempotencyTestClock(time.Unix(1_700_000_000, 0))
	registry := NewMemoryIdempotencyRegistry(clock.Now)
	ctx := context.Background()
	id := mustIdempotencyID(t, "00000000000000000000000005")
	first, err := registry.ResolveOrCreate(ctx, "first-key", id, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.ResolveOrCreate(ctx, "second-key", id, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := registry.Remove(ctx, first.Digest)
	if err != nil || !removed {
		t.Fatalf("Remove() = (%v, %v), want (true, nil)", removed, err)
	}
	removed, err = registry.Remove(ctx, first.Digest)
	if err != nil || removed {
		t.Fatalf("second Remove() = (%v, %v), want (false, nil)", removed, err)
	}

	clock.Advance(2 * time.Minute)
	expired, err := registry.Expire(ctx)
	if err != nil || expired != 1 {
		t.Fatalf("Expire() = (%d, %v), want (1, nil)", expired, err)
	}
	refreshed, err := registry.Refresh(ctx, second.Digest, time.Minute)
	if err != nil || refreshed {
		t.Fatalf("Refresh(expired) = (%v, %v), want (false, nil)", refreshed, err)
	}
}

func TestRegistryNeverStoresRawKeys(t *testing.T) {
	t.Parallel()
	const rawKey = "never-retain-this-client-secret"
	registry := NewMemoryIdempotencyRegistry(nil)
	binding, err := registry.ResolveOrCreate(
		context.Background(),
		rawKey,
		mustIdempotencyID(t, "00000000000000000000000006"),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, _ := DigestKey(rawKey)
	if binding.Digest != wantDigest {
		t.Fatalf("binding digest = %x, want %x", binding.Digest, wantDigest)
	}

	if err := registry.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer registry.release()
	if len(registry.entries) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(registry.entries))
	}
	if _, ok := registry.entries[wantDigest]; !ok {
		t.Fatal("entry was not indexed by the SHA-256 digest")
	}
	if snapshot := fmt.Sprintf("%#v", registry.entries); strings.Contains(snapshot, rawKey) {
		t.Fatalf("registry snapshot exposed raw key: %s", snapshot)
	}
}

func TestResolveOrCreateIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	const contenders = 256
	registry := NewMemoryIdempotencyRegistry(func() time.Time { return time.Unix(1_700_000_000, 0) })
	ctx := context.Background()
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(1_700_000_000_000) },
		strings.NewReader(strings.Repeat("\x00", ulidEntropyLen)),
	)
	candidates := make([]StreamID, contenders)
	for index := range candidates {
		id, err := generator.New()
		if err != nil {
			t.Fatal(err)
		}
		candidates[index] = id
	}

	start := make(chan struct{})
	results := make(chan IdempotencyBinding, contenders)
	errorsFound := make(chan error, contenders)
	var wait sync.WaitGroup
	for index := range contenders {
		wait.Add(1)
		go func(candidate StreamID) {
			defer wait.Done()
			<-start
			binding, err := registry.ResolveOrCreate(ctx, "one-shared-key", candidate, time.Minute)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- binding
		}(candidates[index])
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("ResolveOrCreate() error: %v", err)
	}

	created := 0
	var winner StreamID
	var digest IdempotencyDigest
	bindings := make([]IdempotencyBinding, 0, contenders)
	for binding := range results {
		bindings = append(bindings, binding)
		if binding.Created {
			created++
			winner = binding.ID
			digest = binding.Digest
		}
	}
	if created != 1 {
		t.Fatalf("created bindings = %d, want exactly 1", created)
	}
	for _, binding := range bindings {
		if binding.ID != winner || binding.Digest != digest {
			t.Fatalf("concurrent binding = %#v, want ID %q and digest %x", binding, winner, digest)
		}
	}
	resolved, err := registry.ResolveOrCreate(ctx, "one-shared-key", candidates[0], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Created || resolved.ID != winner || resolved.Digest != digest {
		t.Fatalf("resolved winner = %#v, want ID %q and digest %x", resolved, winner, digest)
	}
}

func TestConcurrentRecreationAfterExpiryHasOneWinner(t *testing.T) {
	t.Parallel()
	const contenders = 128
	clock := newIdempotencyTestClock(time.Unix(1_700_000_000, 0))
	registry := NewMemoryIdempotencyRegistry(clock.Now)
	ctx := context.Background()
	original := mustIdempotencyID(t, "00000000000000000000000007")
	if _, err := registry.ResolveOrCreate(ctx, "expired-race", original, time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)

	start := make(chan struct{})
	var created atomic.Int32
	winningIDs := make(chan StreamID, contenders)
	var wait sync.WaitGroup
	for index := 0; index < contenders; index++ {
		candidate := StreamID(fmt.Sprintf("0000000000000000000000%04x", index+8))
		// The hexadecimal candidate alphabet is a subset of Crockford Base32.
		if err := candidate.Validate(); err != nil {
			t.Fatalf("candidate %q invalid: %v", candidate, err)
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			binding, err := registry.ResolveOrCreate(ctx, "expired-race", candidate, time.Minute)
			if err != nil {
				t.Errorf("ResolveOrCreate() error: %v", err)
				return
			}
			if binding.Created {
				created.Add(1)
			}
			winningIDs <- binding.ID
		}()
	}
	close(start)
	wait.Wait()
	close(winningIDs)
	if got := created.Load(); got != 1 {
		t.Fatalf("created bindings after expiry = %d, want 1", got)
	}
	var winner StreamID
	for id := range winningIDs {
		if winner == "" {
			winner = id
		}
		if id != winner {
			t.Fatalf("concurrent callers resolved different IDs: %q and %q", winner, id)
		}
	}
}

func TestRegistryRejectsInvalidArgumentsWithoutLeakingKey(t *testing.T) {
	t.Parallel()
	const rawKey = "sensitive-invalid-key"
	registry := NewMemoryIdempotencyRegistry(nil)
	ctx := context.Background()
	validID := mustIdempotencyID(t, "00000000000000000000000008")

	tests := []struct {
		name string
		call func() error
		want error
	}{
		{
			name: "empty key",
			call: func() error {
				_, err := registry.ResolveOrCreate(ctx, "", validID, time.Minute)
				return err
			},
			want: ErrInvalidIdempotencyKey,
		},
		{
			name: "invalid ID",
			call: func() error {
				_, err := registry.ResolveOrCreate(ctx, rawKey, "bad-id", time.Minute)
				return err
			},
			want: ErrInvalidStreamID,
		},
		{
			name: "invalid resolve TTL",
			call: func() error {
				_, err := registry.ResolveOrCreate(ctx, rawKey, validID, 0)
				return err
			},
			want: ErrInvalidIdempotencyTTL,
		},
		{
			name: "invalid refresh TTL",
			call: func() error {
				_, err := registry.Refresh(ctx, IdempotencyDigest{}, -time.Second)
				return err
			},
			want: ErrInvalidIdempotencyTTL,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), rawKey) {
				t.Fatalf("error exposed raw key: %v", err)
			}
		})
	}
}

func TestRegistryHonorsContext(t *testing.T) {
	t.Parallel()
	registry := NewMemoryIdempotencyRegistry(nil)
	id := mustIdempotencyID(t, "00000000000000000000000009")
	digest, _ := DigestKey("context-key")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{"resolve", func(ctx context.Context) error {
			_, err := registry.ResolveOrCreate(ctx, "context-key", id, time.Minute)
			return err
		}},
		{"refresh", func(ctx context.Context) error {
			_, err := registry.Refresh(ctx, digest, time.Minute)
			return err
		}},
		{"remove", func(ctx context.Context) error {
			_, err := registry.Remove(ctx, digest)
			return err
		}},
		{"expire", func(ctx context.Context) error {
			_, err := registry.Expire(ctx)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name+" canceled", func(t *testing.T) {
			if err := test.call(canceled); !errors.Is(err, context.Canceled) {
				t.Errorf("error = %v, want context.Canceled", err)
			}
		})
		t.Run(test.name+" nil", func(t *testing.T) {
			if err := test.call(nil); !errors.Is(err, ErrInvalidContext) { //nolint:staticcheck // Verify the registry's nil-context contract.
				t.Errorf("error = %v, want ErrInvalidContext", err)
			}
		})
	}
}

func TestRegistryCancellationInterruptsAtomicGateWait(t *testing.T) {
	t.Parallel()
	registry := NewMemoryIdempotencyRegistry(nil)
	if err := registry.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer registry.release()

	ctx, cancel := context.WithCancel(context.Background())
	id := mustIdempotencyID(t, "0000000000000000000000000b")
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, err := registry.ResolveOrCreate(
			ctx,
			"waiting-key",
			id,
			time.Minute,
		)
		result <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("waiting ResolveOrCreate() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled ResolveOrCreate remained blocked on the registry gate")
	}
}

func TestZeroValueRegistryIsUsable(t *testing.T) {
	t.Parallel()
	var registry MemoryIdempotencyRegistry
	binding, err := registry.ResolveOrCreate(
		context.Background(),
		"zero-value",
		mustIdempotencyID(t, "0000000000000000000000000a"),
		time.Minute,
	)
	if err != nil || !binding.Created {
		t.Fatalf("zero-value ResolveOrCreate() = (%#v, %v)", binding, err)
	}
}

func mustIdempotencyID(t *testing.T, value string) StreamID {
	t.Helper()
	id, err := ParseStreamID(value)
	if err != nil {
		t.Fatalf("ParseStreamID(%q): %v", value, err)
	}
	return id
}

type idempotencyTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newIdempotencyTestClock(now time.Time) *idempotencyTestClock {
	return &idempotencyTestClock{now: now}
}

func (c *idempotencyTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *idempotencyTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}
