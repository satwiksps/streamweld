package journal

import (
	"context"
	"fmt"
	"strings"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

var (
	redisAcquireReaderScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('STREAMWELD_EXPIRED')
  end
  return redis.error_reply('STREAMWELD_NOT_FOUND')
end
if redis.call('HGET', KEYS[1], 'status') == 'open' and
   redis.call('EXISTS', KEYS[4]) == 1 then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
return 1
`)
	redisRefreshReaderScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
if redis.call('HGET', KEYS[1], 'status') == 'open' and
   redis.call('EXISTS', KEYS[4]) == 1 then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZSCORE', KEYS[2], ARGV[2]) == false then
  return 0
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
return 1
`)
	redisCountReadersScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
return redis.call('ZCARD', KEYS[2])
`)
	redisClaimOrphanScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('STREAMWELD_EXPIRED')
  end
  return redis.error_reply('STREAMWELD_NOT_FOUND')
end
if redis.call('HGET', KEYS[1], 'status') ~= 'open' then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local existing = redis.call('GET', KEYS[4])
if existing then
  if existing == ARGV[1] then
    redis.call('PEXPIRE', KEYS[4], ARGV[2])
    return 1
  end
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZCARD', KEYS[2]) ~= 0 then
  return 0
end
redis.call('SET', KEYS[4], ARGV[1], 'PX', ARGV[2])
return 1
`)
	redisReleaseOrphanScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
if redis.call('GET', KEYS[4]) ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[4])
return 1
`)
)

var _ ReaderLeaseStore = (*Redis)(nil)
var _ OrphanCancellationStore = (*Redis)(nil)

// AcquireReader registers or refreshes one cross-replica reader heartbeat.
func (r *Redis) AcquireReader(ctx context.Context, id StreamID, readerID string, ttl time.Duration) error {
	if err := validateReaderLease(ctx, id, readerID, ttl); err != nil {
		return err
	}
	keys := r.readerKeys(id)
	_, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisAcquireReaderScript.Run(ctx, r.client, keys,
			durationMillis(ttl), readerID, durationMillis(2*ttl),
		).Int64()
	})
	return r.mapError("acquire reader lease", id, err)
}

// RefreshReader extends an existing reader heartbeat without recreating one
// that was explicitly released.
func (r *Redis) RefreshReader(
	ctx context.Context,
	id StreamID,
	readerID string,
	ttl time.Duration,
) (bool, error) {
	if err := validateReaderLease(ctx, id, readerID, ttl); err != nil {
		return false, err
	}
	result, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisRefreshReaderScript.Run(ctx, r.client, r.readerKeys(id),
			durationMillis(ttl), readerID, durationMillis(2*ttl),
		).Int64()
	})
	if err != nil {
		return false, r.mapError("refresh reader lease", id, err)
	}
	return result == 1, nil
}

// ReleaseReader removes one reader heartbeat. It is idempotent.
func (r *Redis) ReleaseReader(ctx context.Context, id StreamID, readerID string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("release reader lease: %w", err)
	}
	if strings.TrimSpace(readerID) == "" {
		return ErrInvalidReaderID
	}
	if err := r.client.ZRem(ctx, r.readersKey(id), readerID).Err(); err != nil {
		return fmt.Errorf("release Redis reader lease for %s: %w", id, err)
	}
	return nil
}

// ActiveReaders returns the number of unexpired cross-replica reader leases.
func (r *Redis) ActiveReaders(ctx context.Context, id StreamID) (int64, error) {
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if err := id.Validate(); err != nil {
		return 0, fmt.Errorf("count reader leases: %w", err)
	}
	count, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisCountReadersScript.Run(ctx, r.client, r.readerKeys(id)).Int64()
	})
	if err != nil {
		return 0, r.mapError("count reader leases", id, err)
	}
	return count, nil
}

// TryClaimOrphan atomically claims cancellation only when no unexpired remote
// reader lease exists. Redis TIME supplies the sole lease clock, so proxy clock
// skew cannot cause premature pruning or extend a dead reader.
func (r *Redis) TryClaimOrphan(
	ctx context.Context,
	id StreamID,
	claimID string,
	ttl time.Duration,
) (bool, error) {
	if err := validateOrphanClaim(ctx, id, claimID, ttl); err != nil {
		return false, err
	}
	result, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisClaimOrphanScript.Run(
			ctx, r.client, r.readerKeys(id), claimID, durationMillis(ttl),
		).Int64()
	})
	if err != nil {
		return false, r.mapError("claim orphan cancellation", id, err)
	}
	return result == 1, nil
}

// ReleaseOrphanClaim removes only the caller's claim. It is safe to retry and
// cannot clear a claim installed by another owner attempt.
func (r *Redis) ReleaseOrphanClaim(
	ctx context.Context,
	id StreamID,
	claimID string,
) (bool, error) {
	if err := validateOrphanClaimIdentity(ctx, id, claimID); err != nil {
		return false, err
	}
	result, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisReleaseOrphanScript.Run(ctx, r.client, r.readerKeys(id), claimID).Int64()
	})
	if err != nil {
		return false, r.mapError("release orphan cancellation claim", id, err)
	}
	return result == 1, nil
}

func (r *Redis) readerKeys(id StreamID) []string {
	stream := r.streamKeys(id)
	return []string{stream[0], r.readersKey(id), stream[2], r.orphanClaimKey(id)}
}

func (r *Redis) readersKey(id StreamID) string {
	return r.streamKey(id, "readers")
}

func (r *Redis) orphanClaimKey(id StreamID) string {
	return r.streamKey(id, "orphan-claim")
}

func validateReaderLease(ctx context.Context, id StreamID, readerID string, ttl time.Duration) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("reader lease: %w", err)
	}
	if strings.TrimSpace(readerID) == "" {
		return ErrInvalidReaderID
	}
	if strings.ContainsAny(readerID, "\r\n\x00") {
		return ErrInvalidReaderID
	}
	if ttl <= 0 {
		return ErrInvalidReaderLeaseTTL
	}
	return nil
}

func validateOrphanClaim(
	ctx context.Context,
	id StreamID,
	claimID string,
	ttl time.Duration,
) error {
	if err := validateOrphanClaimIdentity(ctx, id, claimID); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidOrphanClaimTTL
	}
	return nil
}

func validateOrphanClaimIdentity(ctx context.Context, id StreamID, claimID string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("orphan cancellation claim: %w", err)
	}
	if strings.TrimSpace(claimID) == "" || strings.ContainsAny(claimID, "\r\n\x00") {
		return ErrInvalidOrphanClaimID
	}
	return nil
}
