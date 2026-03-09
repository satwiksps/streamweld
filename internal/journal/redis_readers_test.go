package journal

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisReaderLeasesAreSharedRefreshedAndReleased(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	config := DefaultRedisConfig()
	config.Prefix = "reader-leases"
	storeA, err := NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("NewRedis(A) error = %v", err)
	}
	storeB, err := NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("NewRedis(B) error = %v", err)
	}
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := storeA.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	const ttl = 10 * time.Second
	if err := storeB.AcquireReader(ctx, id, "replica-b-reader", ttl); err != nil {
		t.Fatalf("AcquireReader() error = %v", err)
	}
	if count, err := storeA.ActiveReaders(ctx, id); err != nil || count != 1 {
		t.Fatalf("ActiveReaders() = (%d, %v), want (1, nil)", count, err)
	}
	server.FastForward(6 * time.Second)
	if refreshed, err := storeB.RefreshReader(ctx, id, "replica-b-reader", ttl); err != nil || !refreshed {
		t.Fatalf("RefreshReader() = (%t, %v), want (true, nil)", refreshed, err)
	}
	server.FastForward(6 * time.Second)
	if count, err := storeA.ActiveReaders(ctx, id); err != nil || count != 1 {
		t.Fatalf("ActiveReaders(after refresh) = (%d, %v), want (1, nil)", count, err)
	}
	if err := storeB.ReleaseReader(ctx, id, "replica-b-reader"); err != nil {
		t.Fatalf("ReleaseReader() error = %v", err)
	}
	if count, err := storeA.ActiveReaders(ctx, id); err != nil || count != 0 {
		t.Fatalf("ActiveReaders(after release) = (%d, %v), want (0, nil)", count, err)
	}
	if refreshed, err := storeB.RefreshReader(ctx, id, "replica-b-reader", ttl); err != nil || refreshed {
		t.Fatalf("RefreshReader(after release) = (%t, %v), want (false, nil)", refreshed, err)
	}

	if err := storeA.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRedisReaderLeaseExpiresWithoutRelease(t *testing.T) {
	store, server := newRedisTestStore(t, "reader-lease-expiry")
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.AcquireReader(ctx, id, "crashed-reader", time.Second); err != nil {
		t.Fatalf("AcquireReader() error = %v", err)
	}
	server.FastForward(2100 * time.Millisecond)
	if count, err := store.ActiveReaders(ctx, id); err != nil || count != 0 {
		t.Fatalf("ActiveReaders(after expiry) = (%d, %v), want (0, nil)", count, err)
	}
}

func TestRedisReaderLeaseUsesServerTimeDespiteProxyClockSkew(t *testing.T) {
	store, server := newRedisTestStore(t, "reader-lease-server-time")
	server.SetTime(time.Date(2042, time.July, 8, 9, 10, 11, 0, time.UTC))
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	const ttl = 10 * time.Second
	if err := store.AcquireReader(ctx, id, "skewed-proxy-reader", ttl); err != nil {
		t.Fatalf("AcquireReader() error = %v", err)
	}
	serverNow, err := store.client.Time(ctx).Result()
	if err != nil {
		t.Fatalf("TIME error = %v", err)
	}
	score, err := store.client.ZScore(ctx, store.readersKey(id), "skewed-proxy-reader").Result()
	if err != nil {
		t.Fatalf("ZScore() error = %v", err)
	}
	wantExpiry := serverNow.Add(ttl).UnixMilli()
	if got := int64(score); got < wantExpiry-1 || got > wantExpiry+1 {
		t.Fatalf("reader expiry score = %d, want Redis TIME + TTL near %d", got, wantExpiry)
	}

	// Advance Redis's command clock explicitly. miniredis FastForward advances
	// key expirations, while SetTime is the clock observed by the TIME command
	// inside Lua.
	server.SetTime(serverNow.Add(ttl + time.Millisecond))
	if count, countErr := store.ActiveReaders(ctx, id); countErr != nil || count != 0 {
		t.Fatalf("ActiveReaders(after server TTL) = (%d, %v), want (0, nil)", count, countErr)
	}
}

func TestRedisOrphanClaimReaderWinsLinearization(t *testing.T) {
	store, _ := newRedisTestStore(t, "orphan-claim-reader-wins")
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.AcquireReader(ctx, id, "reader-winner", time.Minute); err != nil {
		t.Fatalf("AcquireReader() error = %v", err)
	}
	if claimed, err := store.TryClaimOrphan(ctx, id, "cancel-loser", time.Minute); err != nil || claimed {
		t.Fatalf("TryClaimOrphan() = (%t, %v), want (false, nil)", claimed, err)
	}
}

func TestRedisOrphanClaimCancellationWinsLinearization(t *testing.T) {
	store, _ := newRedisTestStore(t, "orphan-claim-cancel-wins")
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if claimed, err := store.TryClaimOrphan(ctx, id, "cancel-winner", time.Minute); err != nil || !claimed {
		t.Fatalf("TryClaimOrphan() = (%t, %v), want (true, nil)", claimed, err)
	}
	if err := store.AcquireReader(ctx, id, "late-reader", time.Minute); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("AcquireReader(after claim) error = %v, want ErrTerminalState", err)
	}
	if released, err := store.ReleaseOrphanClaim(ctx, id, "different-claim"); err != nil || released {
		t.Fatalf("ReleaseOrphanClaim(wrong owner) = (%t, %v), want (false, nil)", released, err)
	}
	if err := store.AcquireReader(ctx, id, "still-blocked-reader", time.Minute); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("AcquireReader(after foreign release) error = %v, want ErrTerminalState", err)
	}
	if released, err := store.ReleaseOrphanClaim(ctx, id, "cancel-winner"); err != nil || !released {
		t.Fatalf("ReleaseOrphanClaim() = (%t, %v), want (true, nil)", released, err)
	}
	if err := store.AcquireReader(ctx, id, "reader-after-abandon", time.Minute); err != nil {
		t.Fatalf("AcquireReader(after owned release) error = %v", err)
	}
}

func TestRedisOrphanClaimAndReaderAcquireHaveOneWinner(t *testing.T) {
	store, _ := newRedisTestStore(t, "orphan-claim-race")
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	for iteration := range 20 {
		readerID := "reader-" + time.Unix(0, int64(iteration)).Format("150405.000000000")
		claimID := "claim-" + time.Unix(0, int64(iteration)).Format("150405.000000000")
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		var acquireErr error
		var claimed bool
		var claimErr error
		go func() {
			defer wait.Done()
			<-start
			acquireErr = store.AcquireReader(ctx, id, readerID, time.Minute)
		}()
		go func() {
			defer wait.Done()
			<-start
			claimed, claimErr = store.TryClaimOrphan(ctx, id, claimID, time.Minute)
		}()
		close(start)
		wait.Wait()

		readerWon := acquireErr == nil
		claimWon := claimErr == nil && claimed
		if readerWon == claimWon {
			t.Fatalf(
				"iteration %d: reader=(%t, %v), claim=(%t, %v); want exactly one winner",
				iteration, readerWon, acquireErr, claimed, claimErr,
			)
		}
		if readerWon {
			if claimErr != nil || claimed {
				t.Fatalf("iteration %d: losing claim = (%t, %v), want (false, nil)", iteration, claimed, claimErr)
			}
			if err := store.ReleaseReader(ctx, id, readerID); err != nil {
				t.Fatalf("iteration %d: ReleaseReader() error = %v", iteration, err)
			}
		} else {
			if !errors.Is(acquireErr, ErrTerminalState) {
				t.Fatalf("iteration %d: losing AcquireReader() error = %v, want ErrTerminalState", iteration, acquireErr)
			}
			if released, err := store.ReleaseOrphanClaim(ctx, id, claimID); err != nil || !released {
				t.Fatalf("iteration %d: ReleaseOrphanClaim() = (%t, %v), want (true, nil)", iteration, released, err)
			}
		}
	}
}

func TestRedisOrphanClaimExpiresAfterClaimantCrash(t *testing.T) {
	store, server := newRedisTestStore(t, "orphan-claim-crash")
	id := newRedisTestStreamID(t)
	ctx := context.Background()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if claimed, err := store.TryClaimOrphan(ctx, id, "crashed-owner", time.Second); err != nil || !claimed {
		t.Fatalf("TryClaimOrphan() = (%t, %v), want (true, nil)", claimed, err)
	}
	if err := store.AcquireReader(ctx, id, "blocked-reader", time.Minute); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("AcquireReader(before claim expiry) error = %v, want ErrTerminalState", err)
	}

	server.FastForward(time.Second + time.Millisecond)
	if err := store.AcquireReader(ctx, id, "reader-after-crash", time.Minute); err != nil {
		t.Fatalf("AcquireReader(after claim expiry) error = %v", err)
	}
}
