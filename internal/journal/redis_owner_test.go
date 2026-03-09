package journal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisOwnerDirectoryLivenessAndMetadataPrivacy(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	config := DefaultRedisConfig()
	config.Prefix = "owner-directory"
	storeA, err := NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("NewRedis(A) error = %v", err)
	}
	storeB, err := NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("NewRedis(B) error = %v", err)
	}
	ctx := context.Background()
	owner := OwnerRecord{ReplicaID: "replica-a", RelayURL: "https://relay-a.internal:8443"}
	if err := storeA.HeartbeatOwner(ctx, owner, time.Second); err != nil {
		t.Fatalf("HeartbeatOwner() error = %v", err)
	}
	id := newRedisTestStreamID(t)
	if err := storeA.Open(ctx, id, Meta{
		Model: "model", BackendID: "backend", Endpoint: "/v1/chat/completions",
		Request: json.RawMessage(`{"model":"model"}`), Owner: &owner,
	}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	located, err := storeB.LocateOwner(ctx, id)
	if err != nil || located != owner {
		t.Fatalf("LocateOwner() = (%+v, %v), want %+v", located, err, owner)
	}

	iterator, err := storeB.Read(ctx, id, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	entries, readErr := collectRedisIterator(iterator)
	if readErr != nil || len(entries) != 1 {
		t.Fatalf("Read() = (%#v, %v), want one open entry", entries, readErr)
	}
	state, err := storeB.State(ctx, id)
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal(StreamState) error = %v", err)
	}
	publicBytes := string(entries[0].Payload) + string(stateJSON)
	if strings.Contains(publicBytes, owner.ReplicaID) || strings.Contains(publicBytes, owner.RelayURL) {
		t.Fatalf("owner metadata leaked through public journal data: %s", publicBytes)
	}

	server.FastForward(2 * time.Second)
	if _, err := storeB.LocateOwner(ctx, id); !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatalf("LocateOwner() after presence expiry error = %v, want ErrOwnerUnavailable", err)
	}
	changed := OwnerRecord{ReplicaID: owner.ReplicaID, RelayURL: "https://different.internal:8443"}
	if err := storeA.HeartbeatOwner(ctx, changed, time.Second); err != nil {
		t.Fatalf("HeartbeatOwner(changed) error = %v", err)
	}
	if _, err := storeB.LocateOwner(ctx, id); !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatalf("LocateOwner() with mismatched presence error = %v, want ErrOwnerUnavailable", err)
	}
}

func TestRedisOwnerDirectoryRejectsMissingOwnerAndInvalidHeartbeat(t *testing.T) {
	t.Parallel()
	store, _ := newRedisTestStore(t, "owner-missing")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := store.LocateOwner(ctx, id); !errors.Is(err, ErrOwnerNotRecorded) {
		t.Fatalf("LocateOwner() error = %v, want ErrOwnerNotRecorded", err)
	}
	owner := OwnerRecord{ReplicaID: "replica", RelayURL: "https://relay.internal"}
	if err := store.HeartbeatOwner(ctx, owner, 0); err == nil {
		t.Fatal("HeartbeatOwner() accepted zero TTL")
	}
	// Deliberately exercise the package's documented nil-context rejection.
	//nolint:staticcheck
	if err := store.HeartbeatOwner(nil, owner, time.Second); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("HeartbeatOwner(nil) error = %v, want ErrInvalidContext", err)
	}
}
