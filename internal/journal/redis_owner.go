package journal

import (
	"context"
	"errors"
	"fmt"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

var _ OwnerDirectory = (*Redis)(nil)

// LocateOwner resolves immutable per-stream routing metadata only while the
// owning replica's matching presence lease remains live.
func (r *Redis) LocateOwner(ctx context.Context, id StreamID) (OwnerRecord, error) {
	if err := checkContext(ctx); err != nil {
		return OwnerRecord{}, err
	}
	if err := id.Validate(); err != nil {
		return OwnerRecord{}, fmt.Errorf("locate stream owner: %w", err)
	}
	keys := r.streamKeys(id)
	var stateCommand *redislib.MapStringStringCmd
	var tombstoneCommand *redislib.IntCmd
	_, err := r.client.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		stateCommand = pipe.HGetAll(ctx, keys[0])
		tombstoneCommand = pipe.Exists(ctx, keys[2])
		return nil
	})
	if err != nil {
		return OwnerRecord{}, fmt.Errorf("locate Redis stream owner %s: %w", id, err)
	}
	fields := stateCommand.Val()
	if len(fields) == 0 {
		if tombstoneCommand.Val() != 0 {
			return OwnerRecord{}, fmt.Errorf("%w: %s", ErrExpired, id)
		}
		return OwnerRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	owner := OwnerRecord{
		ReplicaID: fields["owner_replica_id"],
		RelayURL:  fields["owner_relay_url"],
	}
	if owner.ReplicaID == "" || owner.RelayURL == "" {
		return OwnerRecord{}, fmt.Errorf("%w: %s", ErrOwnerNotRecorded, id)
	}
	if err := owner.Validate(); err != nil {
		return OwnerRecord{}, fmt.Errorf("%w: stream %s has invalid owner metadata: %w", ErrOwnerNotRecorded, id, err)
	}
	presence, err := r.client.Get(ctx, r.ownerPresenceKey(owner.ReplicaID)).Result()
	if errors.Is(err, redislib.Nil) {
		return OwnerRecord{}, fmt.Errorf("%w: replica %s", ErrOwnerUnavailable, owner.ReplicaID)
	}
	if err != nil {
		return OwnerRecord{}, fmt.Errorf("locate Redis owner presence for %s: %w", id, err)
	}
	if presence != owner.RelayURL {
		return OwnerRecord{}, fmt.Errorf("%w: replica %s presence does not match its recorded relay", ErrOwnerUnavailable, owner.ReplicaID)
	}
	return owner, nil
}

// HeartbeatOwner creates or refreshes a replica presence lease.
func (r *Redis) HeartbeatOwner(
	ctx context.Context,
	owner OwnerRecord,
	ttl time.Duration,
) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("heartbeat stream owner: %w", err)
	}
	if ttl <= 0 {
		return errors.New("owner presence TTL must be positive")
	}
	if err := r.client.Set(
		ctx,
		r.ownerPresenceKey(owner.ReplicaID),
		owner.RelayURL,
		ttl,
	).Err(); err != nil {
		return fmt.Errorf("heartbeat Redis owner %s: %w", owner.ReplicaID, err)
	}
	return nil
}

func (r *Redis) ownerPresenceKey(replicaID string) string {
	return r.redisNamespace() + ":replica:" + replicaID + ":presence"
}
