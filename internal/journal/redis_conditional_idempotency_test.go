package journal

import (
	"context"
	"testing"
)

func TestRedisRemoveIfBoundNeverDeletesPendingOrReplacementMapping(t *testing.T) {
	t.Parallel()
	store, server := newRedisTestStore(t, "test-conditional-remove")
	ctx := context.Background()
	pendingID := newRedisTestStreamID(t)
	binding, err := store.ResolveOrCreate(ctx, "conditional-key", pendingID, store.config.TTL)
	if err != nil {
		t.Fatalf("ResolveOrCreate() error: %v", err)
	}
	key := store.idempotencyKey(binding.Digest)
	if removed, err := store.RemoveIfBound(ctx, binding.Digest, pendingID); err != nil || removed {
		t.Fatalf("RemoveIfBound(pending) = (%t, %v), want (false, nil)", removed, err)
	}
	if !server.Exists(key) {
		t.Fatal("RemoveIfBound deleted a pending reservation")
	}

	replacementID := newRedisTestStreamID(t)
	if err := server.Set(key, "r|"+replacementID.String()); err != nil {
		t.Fatalf("seed replacement mapping: %v", err)
	}
	if removed, err := store.RemoveIfBound(ctx, binding.Digest, pendingID); err != nil || removed {
		t.Fatalf("RemoveIfBound(stale ID) = (%t, %v), want (false, nil)", removed, err)
	}
	if got, getErr := server.Get(key); getErr != nil || got != "r|"+replacementID.String() {
		t.Fatalf("replacement mapping = (%q, %v), want preserved replacement", got, getErr)
	}
	if removed, err := store.RemoveIfBound(ctx, binding.Digest, replacementID); err != nil || !removed {
		t.Fatalf("RemoveIfBound(winner) = (%t, %v), want (true, nil)", removed, err)
	}
	if server.Exists(key) {
		t.Fatal("RemoveIfBound left the matching promoted mapping")
	}
}
