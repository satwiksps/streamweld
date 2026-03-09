package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisMutationsRetryAmbiguousExecutedResultOnce(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	base := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	client := &executeThenErrorRedisClient{UniversalClient: base}
	t.Cleanup(func() { _ = base.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-ambiguous-retry"
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)

	client.arm()
	binding, err := store.ResolveOrCreate(ctx, "ambiguous-key", id, config.TTL)
	if err != nil {
		t.Fatalf("ResolveOrCreate() retry error: %v", err)
	}
	if !binding.Created || binding.ID != id {
		t.Fatalf("ResolveOrCreate() binding = %+v, want newly created %s", binding, id)
	}

	client.arm()
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() retry error: %v", err)
	}
	client.arm()
	seq, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"data":"one"}`)})
	if err != nil || seq != 2 {
		t.Fatalf("Append() retry = (%d, %v), want (2, nil)", seq, err)
	}
	client.arm()
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() retry error: %v", err)
	}
	if got, err := base.XLen(ctx, store.eventsKey(id)).Result(); err != nil || got != 3 {
		t.Fatalf("XLEN after retried lifecycle = (%d, %v), want (3, nil)", got, err)
	}

	degradedID := newRedisTestStreamID(t)
	if err := store.Open(ctx, degradedID, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open(degraded) error: %v", err)
	}
	client.arm()
	if err := store.MarkDegraded(ctx, degradedID); err != nil {
		t.Fatalf("MarkDegraded() retry error: %v", err)
	}
	if got, err := base.XLen(ctx, store.eventsKey(degradedID)).Result(); err != nil || got != 1 {
		t.Fatalf("XLEN after retried degradation = (%d, %v), want (1, nil)", got, err)
	}

	second, err := store.ResolveOrCreate(ctx, "ambiguous-key", newRedisTestStreamID(t), config.TTL)
	if err != nil {
		t.Fatalf("second ResolveOrCreate() error: %v", err)
	}
	if second.Created || second.ID != id {
		t.Fatalf("second ResolveOrCreate() = %+v, want existing %s", second, id)
	}
}

func TestRedisMutationNonceMetadataStaysBounded(t *testing.T) {
	t.Parallel()
	store, _ := newRedisTestStore(t, "test-bounded-nonce")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("first Append() error: %v", err)
	}
	stateKey := store.streamKeys(id)[0]
	fieldsAfterFirst, err := store.client.HLen(ctx, stateKey).Result()
	if err != nil {
		t.Fatalf("HLEN after first append error: %v", err)
	}
	for index := 0; index < 100; index++ {
		if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("Append(%d) error: %v", index, err)
		}
	}
	fieldsAfterMany, err := store.client.HLen(ctx, stateKey).Result()
	if err != nil {
		t.Fatalf("HLEN after many appends error: %v", err)
	}
	if fieldsAfterMany != fieldsAfterFirst {
		t.Fatalf("state fields grew from %d to %d after repeated appends", fieldsAfterFirst, fieldsAfterMany)
	}
}

func TestRedisActiveAndTerminalMutationsRefreshRetentionAtomically(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-retention-refresh"
	config.TTL = time.Minute
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	binding, err := store.ResolveOrCreate(ctx, "retained-key", id, 10*time.Second)
	if err != nil {
		t.Fatalf("ResolveOrCreate() error: %v", err)
	}
	if err := store.Open(ctx, binding.ID, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	keys := store.streamKeys(id)
	mappingKey := store.idempotencyKey(binding.Digest)
	requireRedisTTL(t, server, keys[0], config.TTL)
	requireRedisTTL(t, server, keys[1], config.TTL)
	requireRedisTTL(t, server, keys[2], 2*config.TTL)
	requireRedisTTL(t, server, mappingKey, config.TTL)

	server.FastForward(40 * time.Second)
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append() error: %v", err)
	}
	requireRedisTTL(t, server, keys[0], config.TTL)
	requireRedisTTL(t, server, keys[1], config.TTL)
	requireRedisTTL(t, server, keys[2], 2*config.TTL)
	requireRedisTTL(t, server, mappingKey, config.TTL)

	server.FastForward(40 * time.Second)
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	requireRedisTTL(t, server, mappingKey, config.TTL)
	server.FastForward(30 * time.Second)
	if !server.Exists(mappingKey) || !server.Exists(keys[0]) || !server.Exists(keys[1]) {
		t.Fatal("terminal journal or idempotency mapping expired on its pre-Close clock")
	}
}

func TestRedisOpenRejectsOrphanEventsKey(t *testing.T) {
	t.Parallel()
	store, server := newRedisTestStore(t, "test-open-collision")
	id := newRedisTestStreamID(t)
	if err := server.Set(store.eventsKey(id), "orphan"); err != nil {
		t.Fatalf("seed orphan events key: %v", err)
	}
	err := store.Open(context.Background(), id, Meta{Model: "model", BackendID: "backend"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Open() collision error = %v, want ErrAlreadyExists", err)
	}
	if server.Exists(store.streamKeys(id)[0]) {
		t.Fatal("Open() created metadata despite an orphan events-key collision")
	}
}

func TestRedisReplayRejectsLostOrDiscontinuousEvents(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		corrupt func(*testing.T, *Redis, *miniredis.Miniredis, StreamID)
	}{
		{
			name: "events key missing",
			corrupt: func(_ *testing.T, store *Redis, server *miniredis.Miniredis, id StreamID) {
				server.Del(store.eventsKey(id))
			},
		},
		{
			name: "interior event missing",
			corrupt: func(t *testing.T, store *Redis, _ *miniredis.Miniredis, id StreamID) {
				if removed, err := store.client.XDel(context.Background(), store.eventsKey(id), "2-0").Result(); err != nil || removed != 1 {
					t.Fatalf("XDEL = (%d, %v), want (1, nil)", removed, err)
				}
			},
		},
		{
			name: "metadata ahead of events",
			corrupt: func(_ *testing.T, store *Redis, server *miniredis.Miniredis, id StreamID) {
				server.HSet(store.streamKeys(id)[0], "last_seq", "4")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, server := newRedisTestStore(t, "test-replay-loss-"+strings.ReplaceAll(test.name, " ", "-"))
			ctx := context.Background()
			id := newRedisTestStreamID(t)
			if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
				t.Fatalf("Open() error: %v", err)
			}
			for index := 0; index < 2; index++ {
				if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
					t.Fatalf("Append(%d) error: %v", index, err)
				}
			}
			test.corrupt(t, store, server, id)
			if _, err := store.State(ctx, id); !errors.Is(err, ErrOffsetExpired) {
				t.Fatalf("State() error = %v, want ErrOffsetExpired", err)
			}
			if _, err := store.Read(ctx, id, 0); !errors.Is(err, ErrOffsetExpired) {
				t.Fatalf("Read() error = %v, want ErrOffsetExpired", err)
			}
			if channel, cancel, err := store.Tail(ctx, id, 0); channel != nil || cancel != nil || !errors.Is(err, ErrOffsetExpired) {
				t.Fatalf("Tail() = (%v, cancel=%t, %v), want upfront ErrOffsetExpired", channel, cancel != nil, err)
			}
		})
	}
}

func TestRedisKeysSharePrefixClusterHashTag(t *testing.T) {
	t.Parallel()
	store, _ := newRedisTestStore(t, "tenant-slot")
	id := newRedisTestStreamID(t)
	digest, err := DigestKey("cluster-key")
	if err != nil {
		t.Fatalf("DigestKey() error: %v", err)
	}
	keys := append(
		store.openKeys(id),
		store.idempotencyKey(digest),
		store.mutationReceiptKey(id, "0123456789abcdef"),
		store.readersKey(id),
		store.ownerPresenceKey("replica-a"),
	)
	wantTag := redisTestHashTag(keys[0])
	if wantTag == "" {
		t.Fatalf("key %q has no Redis Cluster hash tag", keys[0])
	}
	for _, key := range keys[1:] {
		if got := redisTestHashTag(key); got != wantTag {
			t.Fatalf("key %q hashes by %q, want shared tag %q", key, got, wantTag)
		}
	}
}

func TestRedisOwnerAssociationIsPrivateAndPresenceBound(t *testing.T) {
	t.Parallel()
	store, server := newRedisTestStore(t, "test-owner-association")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	owner := OwnerRecord{ReplicaID: "replica-private", RelayURL: "https://relay.internal.test"}
	if err := store.HeartbeatOwner(ctx, owner, time.Minute); err != nil {
		t.Fatalf("HeartbeatOwner() error: %v", err)
	}
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend", Owner: &owner}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	located, err := store.LocateOwner(ctx, id)
	if err != nil || located != owner {
		t.Fatalf("LocateOwner() = (%+v, %v), want (%+v, nil)", located, err, owner)
	}
	iterator, err := store.Read(ctx, id, 0)
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	entries, iteratorErr := collectRedisIterator(iterator)
	if iteratorErr != nil || len(entries) != 1 {
		t.Fatalf("Read() = (%+v, %v), want one open entry", entries, iteratorErr)
	}
	state, err := store.State(ctx, id)
	if err != nil {
		t.Fatalf("State() error: %v", err)
	}
	publicJSON, err := json.Marshal(struct {
		Entry Entry       `json:"entry"`
		State StreamState `json:"state"`
	}{Entry: entries[0], State: state})
	if err != nil {
		t.Fatalf("marshal public journal response: %v", err)
	}
	if strings.Contains(string(publicJSON), owner.ReplicaID) || strings.Contains(string(publicJSON), owner.RelayURL) {
		t.Fatalf("public journal JSON leaked private owner metadata: %s", publicJSON)
	}
	server.FastForward(time.Minute)
	if _, err := store.LocateOwner(ctx, id); !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatalf("LocateOwner() after presence expiry error = %v, want ErrOwnerUnavailable", err)
	}
}

func TestRetryRedisOperationIsExplicitAndConservative(t *testing.T) {
	t.Parallel()
	attempts := 0
	result, err := retryRedisOperation(context.Background(), func() (string, error) {
		attempts++
		if attempts == 1 {
			return "", ambiguousRedisResultError{}
		}
		return "ok", nil
	})
	if err != nil || result != "ok" || attempts != 2 {
		t.Fatalf("transient retry = (%q, %v, attempts=%d), want (ok, nil, 2)", result, err, attempts)
	}
	attempts = 0
	_, err = retryRedisOperation(context.Background(), func() (string, error) {
		attempts++
		return "", redislib.Nil
	})
	if !errors.Is(err, redislib.Nil) || attempts != 1 {
		t.Fatalf("semantic result retry = (%v, attempts=%d), want (redis.Nil, 1)", err, attempts)
	}
}

type executeThenErrorRedisClient struct {
	redislib.UniversalClient
	mu    sync.Mutex
	armed bool
}

func (c *executeThenErrorRedisClient) arm() {
	c.mu.Lock()
	c.armed = true
	c.mu.Unlock()
}

func (c *executeThenErrorRedisClient) Eval(
	ctx context.Context,
	script string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := c.UniversalClient.Eval(ctx, script, keys, args...)
	c.failSuccessfulCommand(command)
	return command
}

func (c *executeThenErrorRedisClient) EvalSha(
	ctx context.Context,
	sha1 string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := c.UniversalClient.EvalSha(ctx, sha1, keys, args...)
	c.failSuccessfulCommand(command)
	return command
}

func (c *executeThenErrorRedisClient) failSuccessfulCommand(command *redislib.Cmd) {
	if command.Err() != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.armed {
		c.armed = false
		command.SetErr(ambiguousRedisResultError{})
	}
}

type ambiguousRedisResultError struct{}

func (ambiguousRedisResultError) Error() string   { return "connection lost after Redis execution" }
func (ambiguousRedisResultError) Timeout() bool   { return false }
func (ambiguousRedisResultError) Temporary() bool { return true }

var _ net.Error = ambiguousRedisResultError{}

func requireRedisTTL(t *testing.T, server *miniredis.Miniredis, key string, want time.Duration) {
	t.Helper()
	if got := server.TTL(key); got != want {
		t.Fatalf("TTL(%q) = %s, want %s", key, got, want)
	}
}

func redisTestHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return ""
	}
	return key[start+1 : start+1+end]
}

func (c *executeThenErrorRedisClient) String() string {
	return fmt.Sprintf("executeThenErrorRedisClient(%T)", c.UniversalClient)
}
