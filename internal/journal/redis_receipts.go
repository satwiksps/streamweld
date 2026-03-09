package journal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

const redisReceiptReconcileTimeout = 250 * time.Millisecond

// Mutation receipts are separate TTL-scoped keys instead of fields on the
// stream state hash. A receipt remains addressable by its operation nonce even
// when another replica commits later, which makes an execute-then-disconnect
// retry return the original result. Each receipt has a fixed value and expires
// with the journal; the state hash therefore stays bounded.
var redisAppendReceiptScript = redislib.NewScript(`
local receipt = redis.call('GET', KEYS[4])
if receipt then
  if string.sub(receipt, 1, 7) ~= 'append:' then
    return redis.error_reply('STREAMWELD_NONCE_CONFLICT')
  end
  if redis.call('HGET', KEYS[1], 'status') == 'open' then
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
    redis.call('PEXPIRE', KEYS[2], ARGV[4])
    redis.call('SET', KEYS[3], '1', 'PX', ARGV[5])
    local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
    if idempotency then
      redis.call('PEXPIRE', idempotency, ARGV[4])
    end
  end
  redis.call('PEXPIRE', KEYS[4], ARGV[7])
  return {string.sub(receipt, 8), ARGV[6]}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('STREAMWELD_EXPIRED')
  end
  return redis.error_reply('STREAMWELD_NOT_FOUND')
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  return redis.error_reply('STREAMWELD_OFFSET_EXPIRED')
end
if redis.call('HGET', KEYS[1], 'degraded') == '1' then
  return redis.error_reply('STREAMWELD_DEGRADED')
end
if redis.call('HGET', KEYS[1], 'status') ~= 'open' then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local seq = tonumber(redis.call('HGET', KEYS[1], 'last_seq')) + 1
redis.call('XADD', KEYS[2], tostring(seq) .. '-0', 'kind', ARGV[2], 'payload', ARGV[3], 'ts', ARGV[1])
redis.call('HSET', KEYS[1],
  'last_seq', tostring(seq),
  'updated_at', ARGV[1])
redis.call('SET', KEYS[4], 'append:' .. tostring(seq), 'PX', ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[5])
local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
if idempotency then
  redis.call('PEXPIRE', idempotency, ARGV[4])
end
return {tostring(seq), ARGV[6]}
`)

var redisCloseReceiptScript = redislib.NewScript(`
local receipt = redis.call('GET', KEYS[4])
if receipt then
  if string.sub(receipt, 1, 6) ~= 'close:' then
    return redis.error_reply('STREAMWELD_NONCE_CONFLICT')
  end
  if redis.call('HGET', KEYS[1], 'status') == 'open' then
    redis.call('PEXPIRE', KEYS[1], ARGV[6])
    redis.call('PEXPIRE', KEYS[2], ARGV[6])
    redis.call('SET', KEYS[3], '1', 'PX', ARGV[7])
    local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
    if idempotency then
      redis.call('PEXPIRE', idempotency, ARGV[6])
    end
  end
  redis.call('PEXPIRE', KEYS[4], ARGV[9])
  return {string.sub(receipt, 7), ARGV[8]}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('STREAMWELD_EXPIRED')
  end
  return redis.error_reply('STREAMWELD_NOT_FOUND')
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  return redis.error_reply('STREAMWELD_OFFSET_EXPIRED')
end
if redis.call('HGET', KEYS[1], 'degraded') == '1' then
  return redis.error_reply('STREAMWELD_DEGRADED')
end
if redis.call('HGET', KEYS[1], 'status') ~= 'open' then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local seq = tonumber(redis.call('HGET', KEYS[1], 'last_seq')) + 1
redis.call('XADD', KEYS[2], tostring(seq) .. '-0', 'kind', ARGV[2], 'payload', ARGV[3], 'ts', ARGV[1])
redis.call('HSET', KEYS[1],
  'last_seq', tostring(seq),
  'updated_at', ARGV[1],
  'status', ARGV[4],
  'resumable', ARGV[5])
redis.call('SET', KEYS[4], 'close:' .. tostring(seq), 'PX', ARGV[9])
redis.call('PEXPIRE', KEYS[1], ARGV[6])
redis.call('PEXPIRE', KEYS[2], ARGV[6])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[7])
local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
if idempotency then
  redis.call('PEXPIRE', idempotency, ARGV[6])
end
return {tostring(seq), ARGV[8]}
`)

var redisDegradeReceiptScript = redislib.NewScript(`
local receipt = redis.call('GET', KEYS[4])
if receipt then
  if string.sub(receipt, 1, 8) ~= 'degrade:' then
    return redis.error_reply('STREAMWELD_NONCE_CONFLICT')
  end
  if redis.call('HGET', KEYS[1], 'status') == 'open' then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    redis.call('PEXPIRE', KEYS[2], ARGV[2])
    redis.call('SET', KEYS[3], '1', 'PX', ARGV[3])
    local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
    if idempotency then
      redis.call('PEXPIRE', idempotency, ARGV[2])
    end
  end
  redis.call('PEXPIRE', KEYS[4], ARGV[5])
  return {string.sub(receipt, 9), ARGV[4]}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  if redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('STREAMWELD_EXPIRED')
  end
  return redis.error_reply('STREAMWELD_NOT_FOUND')
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  return redis.error_reply('STREAMWELD_OFFSET_EXPIRED')
end
if redis.call('HGET', KEYS[1], 'status') ~= 'open' then
  return redis.error_reply('STREAMWELD_TERMINAL')
end
local changed = 0
if redis.call('HGET', KEYS[1], 'degraded') ~= '1' then
  redis.call('HSET', KEYS[1], 'degraded', '1', 'updated_at', ARGV[1])
  changed = 1
end
redis.call('SET', KEYS[4], 'degrade:' .. tostring(changed), 'PX', ARGV[5])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[3])
local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
if idempotency then
  redis.call('PEXPIRE', idempotency, ARGV[2])
end
return {tostring(changed), ARGV[4]}
`)

func (r *Redis) mutationKeys(id StreamID, nonce string) []string {
	return append(r.streamKeys(id), r.mutationReceiptKey(id, nonce))
}

func (r *Redis) mutationReceiptKey(id StreamID, nonce string) string {
	return r.streamKey(id, "receipt:"+nonce)
}

// reconcileMutationReceipt performs a read-only lookup with a fresh bounded
// context after an ambiguous command result. It never re-executes a mutation
// after caller cancellation; it only recovers the result of a Lua commit that
// already stored its receipt.
func (r *Redis) reconcileMutationReceipt(
	ctx context.Context,
	id StreamID,
	nonce string,
	operation string,
	operationErr error,
) (uint64, bool, error) {
	if !redisMutationResultMayBeAmbiguous(operationErr) {
		return 0, false, nil
	}
	reconcileContext, cancel := redisReconcileContext(ctx)
	defer cancel()
	value, err := r.client.Get(reconcileContext, r.mutationReceiptKey(id, nonce)).Result()
	if errors.Is(err, redislib.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read %s mutation receipt: %w", operation, err)
	}
	prefix := operation + ":"
	if !strings.HasPrefix(value, prefix) {
		return 0, false, fmt.Errorf(
			"%w: stream %s %s receipt has an invalid operation",
			ErrInvalidEntry,
			id,
			operation,
		)
	}
	result, err := strconv.ParseUint(strings.TrimPrefix(value, prefix), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"%w: stream %s %s receipt has an invalid result",
			ErrInvalidEntry,
			id,
			operation,
		)
	}
	return result, true, nil
}

func (r *Redis) reconcileOpenReceipt(
	ctx context.Context,
	id StreamID,
	nonce string,
	operationErr error,
) (bool, error) {
	if !redisMutationResultMayBeAmbiguous(operationErr) {
		return false, nil
	}
	reconcileContext, cancel := redisReconcileContext(ctx)
	defer cancel()
	stored, err := r.client.HGet(reconcileContext, r.streamKeys(id)[0], "open_op_nonce").Result()
	if errors.Is(err, redislib.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read open mutation receipt: %w", err)
	}
	if stored != nonce {
		return false, fmt.Errorf(
			"%w: stream %s open receipt nonce conflict",
			ErrInvalidEntry,
			id,
		)
	}
	return true, nil
}

func redisReconcileContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), redisReceiptReconcileTimeout)
}

func redisMutationResultMayBeAmbiguous(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return retryableRedisOperationError(context.Background(), err)
}
