package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

const (
	defaultRedisPrefix     = "streamweld"
	defaultRedisReadBlock  = 250 * time.Millisecond
	defaultRedisPendingTTL = 5 * time.Second
	defaultRedisReaderLag  = int64(1 << 20)
	defaultRedisReceiptTTL = 30 * time.Second
	redisRetryBackoff      = 5 * time.Millisecond
)

var (
	// ErrInvalidRedisConfig reports a malformed Redis journal setting.
	ErrInvalidRedisConfig = errors.New("journal: invalid Redis configuration")

	redisOpenScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  local prior = redis.call('HGET', KEYS[1], 'open_op_nonce')
  if prior == ARGV[11] then
    if redis.call('EXISTS', KEYS[2]) == 0 then
      return redis.error_reply('STREAMWELD_OFFSET_EXPIRED')
    end
    redis.call('PEXPIRE', KEYS[1], ARGV[9])
    redis.call('PEXPIRE', KEYS[2], ARGV[9])
    redis.call('SET', KEYS[3], '1', 'PX', ARGV[10])
    local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
    if idempotency then
      redis.call('PEXPIRE', idempotency, ARGV[9])
    end
    return {'1', ARGV[11]}
  end
  return redis.error_reply('STREAMWELD_ALREADY_EXISTS')
end
if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
  return redis.error_reply('STREAMWELD_ALREADY_EXISTS')
end
local association = redis.call('GET', KEYS[4])
local idempotency = false
if association then
  local reservation = redis.call('GET', association)
  if reservation then
    local first = string.find(reservation, '|', 1, true)
    if first then
      local tag = string.sub(reservation, 1, first - 1)
      local reserved_id = tag
      local pending = true
      if tag == 'p' or tag == 'r' then
        local second = string.find(reservation, '|', first + 1, true)
        if second then
          reserved_id = string.sub(reservation, first + 1, second - 1)
        else
          reserved_id = string.sub(reservation, first + 1)
        end
        pending = tag == 'p'
      end
      if pending and reserved_id == ARGV[15] then
        idempotency = association
      end
    end
  end
end
if ARGV[16] == '1' and (not idempotency or idempotency ~= ARGV[17]) then
  return redis.error_reply('STREAMWELD_RESERVATION_LOST')
end
redis.call('XADD', KEYS[2], '1-0', 'kind', 'open', 'payload', ARGV[8], 'ts', ARGV[1])
redis.call('HSET', KEYS[1],
  'model', ARGV[2],
  'has_model_version', ARGV[3],
  'model_version', ARGV[4],
  'origin_backend', ARGV[5],
  'endpoint', ARGV[6],
  'request', ARGV[7],
  'created_at', ARGV[1],
  'updated_at', ARGV[1],
  'earliest_seq', '1',
  'last_seq', '1',
  'status', 'open',
  'resumable', '1',
  'degraded', '0',
  'open_op_nonce', ARGV[11])
if ARGV[12] == '1' then
  redis.call('HSET', KEYS[1],
    'owner_replica_id', ARGV[13],
    'owner_relay_url', ARGV[14])
end
if idempotency then
  redis.call('HSET', KEYS[1], 'idempotency_key', idempotency)
  redis.call('SET', idempotency, 'r|' .. ARGV[15], 'PX', ARGV[9])
end
redis.call('DEL', KEYS[4])
redis.call('PEXPIRE', KEYS[1], ARGV[9])
redis.call('PEXPIRE', KEYS[2], ARGV[9])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[10])
return {'1', ARGV[11]}
`)

	redisTouchScript = redislib.NewScript(`
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
redis.call('PEXPIRE', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[2], ARGV[1])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[2])
local idempotency = redis.call('HGET', KEYS[1], 'idempotency_key')
if idempotency then
  redis.call('PEXPIRE', idempotency, ARGV[1])
end
return 1
`)

	redisResolveIdempotencyScript = redislib.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing then
  local first = string.find(existing, '|', 1, true)
  if not first then
    return redis.error_reply('STREAMWELD_IDEMPOTENCY_CORRUPT')
  end
  local tag = string.sub(existing, 1, first - 1)
  local id = tag
  local creator = string.sub(existing, first + 1)
  local association = false
  local pending = true
  if tag == 'p' or tag == 'r' then
    local second = string.find(existing, '|', first + 1, true)
    if not second then
      if tag == 'r' then
        id = string.sub(existing, first + 1)
        pending = false
      else
        return redis.error_reply('STREAMWELD_IDEMPOTENCY_CORRUPT')
      end
    else
      id = string.sub(existing, first + 1, second - 1)
      pending = tag == 'p'
      local third = string.find(existing, '|', second + 1, true)
      if third then
        creator = string.sub(existing, second + 1, third - 1)
        association = string.sub(existing, third + 1)
      else
        creator = string.sub(existing, second + 1)
      end
    end
  end
  local created = 0
  if pending and creator == ARGV[3] then
    created = 1
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    if association and redis.call('GET', association) == KEYS[1] then
      redis.call('PEXPIRE', association, ARGV[2])
    end
  end
  return {id, created, ARGV[3]}
end
redis.call('SET', KEYS[1], 'p|' .. ARGV[1] .. '|' .. ARGV[3] .. '|' .. KEYS[2], 'PX', ARGV[2])
redis.call('SET', KEYS[2], KEYS[1], 'PX', ARGV[2])
return {ARGV[1], 1, ARGV[3]}
`)

	redisRefreshIdempotencyScript = redislib.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`)

	redisRefreshPendingIdempotencyScript = redislib.NewScript(`
local reservation = redis.call('GET', KEYS[1])
if not reservation or redis.call('GET', KEYS[2]) ~= KEYS[1] then
  return 0
end
local expected = 'p|' .. ARGV[1] .. '|'
local legacy = ARGV[1] .. '|'
if string.sub(reservation, 1, string.len(expected)) ~= expected and
   string.sub(reservation, 1, string.len(legacy)) ~= legacy then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`)

	redisReleasePendingIdempotencyScript = redislib.NewScript(`
local reservation = redis.call('GET', KEYS[1])
if not reservation or redis.call('GET', KEYS[2]) ~= KEYS[1] then
  return 0
end
local expected = 'p|' .. ARGV[1] .. '|'
local legacy = ARGV[1] .. '|'
if string.sub(reservation, 1, string.len(expected)) ~= expected and
   string.sub(reservation, 1, string.len(legacy)) ~= legacy then
  return 0
end
redis.call('DEL', KEYS[1], KEYS[2])
return 1
`)
)

// RedisConfig controls a Redis Streams journal. Prefix namespaces all keys and
// must not contain Redis Cluster hash-tag delimiters. ReadBlock bounds how long
// a tail reader waits before checking terminal and degraded state.
type RedisConfig struct {
	TTL               time.Duration
	Prefix            string
	ReadBlock         time.Duration
	ReaderMaxLagBytes int64

	// MutationReceiptTTL bounds the cardinality of execute-then-disconnect
	// receipts. It must cover the Redis client's command timeout and is capped
	// at TTL because a receipt cannot outlive the journal result it describes.
	MutationReceiptTTL time.Duration
}

// DefaultRedisConfig returns production defaults for Redis persistence.
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		TTL:                DefaultTTL,
		Prefix:             defaultRedisPrefix,
		ReadBlock:          defaultRedisReadBlock,
		ReaderMaxLagBytes:  defaultRedisReaderLag,
		MutationReceiptTTL: defaultRedisReceiptTTL,
	}
}

// Redis persists journals as Redis Streams and idempotency bindings as
// expiring string keys. It is safe to share across goroutines and proxy
// replicas. The caller retains ownership of Client and must close it.
type Redis struct {
	client redislib.UniversalClient
	config RedisConfig
	nonce  func() (string, error)
}

var (
	_ Journal                        = (*Redis)(nil)
	_ IdempotencyRegistry            = (*Redis)(nil)
	_ PendingIdempotencyRegistry     = (*Redis)(nil)
	_ ConditionalIdempotencyRegistry = (*Redis)(nil)
	_ DegradationMarker              = (*Redis)(nil)
	_ ActiveJournalLease             = (*Redis)(nil)
)

// NewRedis validates config and returns a Redis-backed journal. Construction
// deliberately performs no network request so Redis can recover after proxy
// startup and an unavailable Redis does not prevent the process from serving
// degraded pass-through traffic.
func NewRedis(client redislib.UniversalClient, config RedisConfig) (*Redis, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client cannot be nil", ErrInvalidRedisConfig)
	}
	if config.TTL == 0 {
		config.TTL = DefaultTTL
	}
	if config.Prefix == "" {
		config.Prefix = defaultRedisPrefix
	}
	if config.ReadBlock == 0 {
		config.ReadBlock = defaultRedisReadBlock
	}
	if config.ReaderMaxLagBytes == 0 {
		config.ReaderMaxLagBytes = defaultRedisReaderLag
	}
	if config.MutationReceiptTTL == 0 {
		config.MutationReceiptTTL = defaultRedisReceiptTTL
	}
	// A receipt cannot usefully outlive the journal result it describes.
	if config.MutationReceiptTTL > config.TTL {
		config.MutationReceiptTTL = config.TTL
	}
	var problems []error
	if config.TTL <= 0 {
		problems = append(problems, errors.New("TTL must be positive"))
	}
	if strings.TrimSpace(config.Prefix) == "" {
		problems = append(problems, errors.New("prefix cannot be blank"))
	}
	if strings.ContainsAny(config.Prefix, "{}\r\n\x00") {
		problems = append(problems, errors.New("prefix cannot contain braces, line breaks, or NUL"))
	}
	if config.ReadBlock <= 0 {
		problems = append(problems, errors.New("read block must be positive"))
	}
	if config.ReaderMaxLagBytes <= 0 {
		problems = append(problems, errors.New("reader maximum lag bytes must be positive"))
	}
	if config.MutationReceiptTTL <= 0 {
		problems = append(problems, errors.New("mutation receipt TTL must be positive"))
	}
	if len(problems) != 0 {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRedisConfig, errors.Join(problems...))
	}
	return &Redis{client: client, config: config, nonce: newRedisOperationNonce}, nil
}

// Open atomically creates stream metadata, the sequence-one open entry, and
// the existence tombstone.
func (r *Redis) Open(ctx context.Context, id StreamID, meta Meta) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	ownerPresent, ownerReplicaID, ownerRelayURL := "0", "", ""
	if meta.Owner != nil {
		if err := meta.Owner.Validate(); err != nil {
			return fmt.Errorf("open stream owner: %w", err)
		}
		ownerPresent = "1"
		ownerReplicaID = meta.Owner.ReplicaID
		ownerRelayURL = meta.Owner.RelayURL
	}
	payload, err := json.Marshal(struct {
		StreamID     StreamID `json:"stream_id"`
		Model        string   `json:"model"`
		ModelVersion *string  `json:"model_version"`
		BackendID    string   `json:"backend_id"`
	}{
		StreamID:     id,
		Model:        meta.Model,
		ModelVersion: meta.ModelVersion,
		BackendID:    meta.BackendID,
	})
	if err != nil {
		return fmt.Errorf("marshal open entry: %w", err)
	}
	modelVersion, hasModelVersion := "", "0"
	if meta.ModelVersion != nil {
		modelVersion, hasModelVersion = *meta.ModelVersion, "1"
	}
	expectsReservation, reservationKey := "0", ""
	if meta.Idempotency != nil {
		expectsReservation = "1"
		reservationKey = r.idempotencyKey(*meta.Idempotency)
	}
	nonce, err := r.operationNonce()
	if err != nil {
		return fmt.Errorf("open stream %s: %w", id, err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := retryRedisOperation(ctx, func() ([]any, error) {
		return redisOpenScript.Run(ctx, r.client, r.openKeys(id),
			now,
			meta.Model,
			hasModelVersion,
			modelVersion,
			meta.BackendID,
			meta.Endpoint,
			string(meta.Request),
			string(payload),
			durationMillis(r.config.TTL),
			durationMillis(2*r.config.TTL),
			nonce,
			ownerPresent,
			ownerReplicaID,
			ownerRelayURL,
			id.String(),
			expectsReservation,
			reservationKey,
		).Slice()
	})
	if err != nil {
		if reconciled, reconcileErr := r.reconcileOpenReceipt(ctx, id, nonce, err); reconcileErr != nil {
			err = errors.Join(err, reconcileErr)
		} else if reconciled {
			return nil
		}
		return r.mapError("open", id, err)
	}
	_, err = parseRedisNonceResult("open", result, nonce)
	return err
}

// Append atomically allocates a sequence and writes one non-terminal entry.
func (r *Redis) Append(ctx context.Context, id StreamID, entry Entry) (uint64, error) {
	if err := checkContext(ctx); err != nil {
		return 0, err
	}
	if err := id.Validate(); err != nil {
		return 0, fmt.Errorf("append stream: %w", err)
	}
	if !entry.Kind.appendable() {
		return 0, fmt.Errorf("%w: kind %q cannot be appended", ErrInvalidEntry, entry.Kind)
	}
	if entry.Err != nil {
		return 0, fmt.Errorf("%w: persisted entries cannot carry delivery errors", ErrInvalidEntry)
	}
	if err := validatePayload(entry.Payload); err != nil {
		return 0, err
	}
	nonce, err := r.operationNonce()
	if err != nil {
		return 0, fmt.Errorf("append stream %s: %w", id, err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := retryRedisOperation(ctx, func() ([]any, error) {
		return redisAppendReceiptScript.Run(
			ctx,
			r.client,
			r.mutationKeys(id, nonce),
			now,
			string(entry.Kind),
			string(entry.Payload),
			durationMillis(r.config.TTL),
			durationMillis(2*r.config.TTL),
			nonce,
			durationMillis(r.config.MutationReceiptTTL),
		).Slice()
	})
	if err != nil {
		if reconciledSeq, reconciled, reconcileErr := r.reconcileMutationReceipt(
			ctx,
			id,
			nonce,
			"append",
			err,
		); reconcileErr != nil {
			err = errors.Join(err, reconcileErr)
		} else if reconciled {
			return reconciledSeq, nil
		}
		return 0, r.mapError("append", id, err)
	}
	seq, err := parseRedisNonceResult("append", result, nonce)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// Read returns a finite atomic snapshot. A degraded journal yields every
// committed entry followed by ErrOffsetExpired so callers never infer that a
// sequence gap is resumable.
func (r *Redis) Read(ctx context.Context, id StreamID, fromSeq uint64) (iter.Seq2[Entry, error], error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	state, degraded, err := r.boundedRedisSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if !state.Resumable {
		return nil, fmt.Errorf("%w: %s", ErrNotResumable, id)
	}
	if err := validateRedisCursor(state, fromSeq); err != nil {
		return nil, err
	}
	if degraded && fromSeq == state.LastSeq {
		return nil, fmt.Errorf("%w: stream %s contains an unjournaled gap", ErrOffsetExpired, id)
	}
	throughSeq := state.LastSeq
	return func(yield func(Entry, error) bool) {
		_, completed, replayErr := r.replayRedisRange(
			ctx,
			id,
			fromSeq,
			throughSeq,
			redisStateReplayPageEntries,
			func(entry Entry) bool { return yield(cloneEntry(entry), nil) },
		)
		if replayErr != nil {
			yield(Entry{}, replayErr)
			return
		}
		if !completed {
			return
		}
		if degraded {
			yield(Entry{}, fmt.Errorf("%w: stream %s lost journal continuity", ErrOffsetExpired, id))
		}
	}, nil
}

// Tail replays retained entries and follows future XADD commits with XREAD
// BLOCK. Each call owns an independent Redis read and therefore fans out across
// processes without producer-side bookkeeping.
func (r *Redis) Tail(ctx context.Context, id StreamID, fromSeq uint64) (<-chan Entry, func(), error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, err
	}
	state, degraded, err := r.boundedRedisSnapshot(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if !state.Resumable {
		return nil, nil, fmt.Errorf("%w: %s", ErrNotResumable, id)
	}
	if err := validateRedisCursor(state, fromSeq); err != nil {
		return nil, nil, err
	}
	if degraded && fromSeq == state.LastSeq {
		return nil, nil, fmt.Errorf("%w: stream %s contains an unjournaled gap", ErrOffsetExpired, id)
	}

	tailContext, cancelContext := context.WithCancel(ctx)
	source := make(chan Entry)
	// Normal entries use an unbuffered handoff so the delivery pump accounts
	// for every pending byte until the client actually accepts the entry.
	out := make(chan Entry)
	var once sync.Once
	cancel := func() { once.Do(cancelContext) }
	go r.runTail(tailContext, id, fromSeq, state, degraded, source)
	go runRedisTailDelivery(
		tailContext,
		cancel,
		id,
		r.config.ReaderMaxLagBytes,
		source,
		out,
	)
	return out, cancel, nil
}

// State returns an immutable point-in-time snapshot reconstructed from the
// stream metadata and entries in one Redis transaction.
func (r *Redis) State(ctx context.Context, id StreamID) (StreamState, error) {
	if err := checkContext(ctx); err != nil {
		return StreamState{}, err
	}
	state, _, err := r.boundedRedisSnapshot(ctx, id)
	if err != nil {
		return StreamState{}, err
	}
	_, completed, err := r.replayRedisRange(
		ctx,
		id,
		state.EarliestSeq-1,
		state.LastSeq,
		redisStateReplayPageEntries,
		func(entry Entry) bool {
			applyRedisStateEntry(&state, entry)
			return true
		},
	)
	if err != nil {
		return StreamState{}, err
	}
	if !completed {
		return StreamState{}, fmt.Errorf("read Redis stream %s: incomplete state replay", id)
	}
	return state, nil
}

// Close atomically commits the winning terminal entry, transitions state, and
// starts journal and tombstone retention clocks.
func (r *Redis) Close(ctx context.Context, id StreamID, terminal Entry) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("close stream: %w", err)
	}
	if !terminal.Kind.terminal() {
		return fmt.Errorf("%w: kind %q is not terminal", ErrInvalidEntry, terminal.Kind)
	}
	if terminal.Err != nil {
		return fmt.Errorf("%w: persisted entries cannot carry delivery errors", ErrInvalidEntry)
	}
	if err := validatePayload(terminal.Payload); err != nil {
		return err
	}
	resumable := "1"
	if terminal.Kind == KindStopped {
		resumable = "0"
	}
	nonce, err := r.operationNonce()
	if err != nil {
		return fmt.Errorf("close stream %s: %w", id, err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := retryRedisOperation(ctx, func() ([]any, error) {
		return redisCloseReceiptScript.Run(ctx, r.client, r.mutationKeys(id, nonce),
			now,
			string(terminal.Kind),
			string(terminal.Payload),
			string(statusForTerminal(terminal.Kind)),
			resumable,
			durationMillis(r.config.TTL),
			durationMillis(2*r.config.TTL),
			nonce,
			durationMillis(r.config.MutationReceiptTTL),
		).Slice()
	})
	if err != nil {
		if _, reconciled, reconcileErr := r.reconcileMutationReceipt(
			ctx,
			id,
			nonce,
			"close",
			err,
		); reconcileErr != nil {
			err = errors.Join(err, reconcileErr)
		} else if reconciled {
			return nil
		}
		return r.mapError("close", id, err)
	}
	_, err = parseRedisNonceResult("close", result, nonce)
	return err
}

// MarkDegraded atomically freezes sequence assignment after a durability gap.
// Existing entries remain readable, but replay ends with ErrOffsetExpired.
func (r *Redis) MarkDegraded(ctx context.Context, id StreamID) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("mark stream degraded: %w", err)
	}
	nonce, err := r.operationNonce()
	if err != nil {
		return fmt.Errorf("mark stream %s degraded: %w", id, err)
	}
	result, err := retryRedisOperation(ctx, func() ([]any, error) {
		return redisDegradeReceiptScript.Run(
			ctx,
			r.client,
			r.mutationKeys(id, nonce),
			time.Now().UTC().Format(time.RFC3339Nano),
			durationMillis(r.config.TTL),
			durationMillis(2*r.config.TTL),
			nonce,
			durationMillis(r.config.MutationReceiptTTL),
		).Slice()
	})
	if err != nil {
		if _, reconciled, reconcileErr := r.reconcileMutationReceipt(
			ctx,
			id,
			nonce,
			"degrade",
			err,
		); reconcileErr != nil {
			err = errors.Join(err, reconcileErr)
		} else if reconciled {
			return nil
		}
		return r.mapError("mark degraded", id, err)
	}
	_, err = parseRedisNonceResult("mark degraded", result, nonce)
	return err
}

// Touch atomically renews every Redis key required by an active journal. It
// deliberately refuses terminal streams so a racing lease tick cannot extend
// their retention window after Close.
func (r *Redis) Touch(ctx context.Context, id StreamID) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("touch stream: %w", err)
	}
	_, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisTouchScript.Run(
			ctx,
			r.client,
			r.streamKeys(id),
			durationMillis(r.config.TTL),
			durationMillis(2*r.config.TTL),
		).Int64()
	})
	if err != nil {
		return r.mapError("touch", id, err)
	}
	return nil
}

// ResolveOrCreate atomically resolves an unexpired key or creates a shared
// digest-to-stream binding. The raw key is hashed before Redis sees it.
func (r *Redis) ResolveOrCreate(
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
	nonce, err := r.operationNonce()
	if err != nil {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: %w", err)
	}
	result, err := retryRedisOperation(ctx, func() ([]any, error) {
		return redisResolveIdempotencyScript.Run(
			ctx,
			r.client,
			[]string{r.idempotencyKey(digest), r.idempotencyAssociationKey(newID)},
			newID.String(),
			durationMillis(redisPendingTTL(ttl)),
			nonce,
		).Slice()
	})
	if err != nil {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: %w", err)
	}
	if len(result) != 3 {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: unexpected result length %d", len(result))
	}
	id, err := ParseStreamID(fmt.Sprint(result[0]))
	if err != nil {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: invalid stored stream ID: %w", err)
	}
	created, err := redisInteger(result[1])
	if err != nil {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: %w", err)
	}
	if returnedNonce := fmt.Sprint(result[2]); returnedNonce != nonce {
		return IdempotencyBinding{}, fmt.Errorf("resolve Redis idempotency key: operation nonce mismatch")
	}
	return IdempotencyBinding{ID: id, Digest: digest, Created: created == 1}, nil
}

// RefreshPending renews a still-pending reservation only when it remains bound
// to id. A promoted mapping or a replacement winner is never modified.
func (r *Redis) RefreshPending(
	ctx context.Context,
	digest IdempotencyDigest,
	id StreamID,
	ttl time.Duration,
) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := id.Validate(); err != nil {
		return false, fmt.Errorf("refresh pending idempotency key: %w", err)
	}
	if err := validateIdempotencyTTL(ttl); err != nil {
		return false, err
	}
	result, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisRefreshPendingIdempotencyScript.Run(
			ctx,
			r.client,
			[]string{r.idempotencyKey(digest), r.idempotencyAssociationKey(id)},
			id.String(),
			durationMillis(redisPendingTTL(ttl)),
		).Int64()
	})
	if err != nil {
		return false, fmt.Errorf("refresh pending Redis idempotency key: %w", err)
	}
	return result == 1, nil
}

// ReleasePending conditionally removes this creator's unpromoted reservation.
func (r *Redis) ReleasePending(
	ctx context.Context,
	digest IdempotencyDigest,
	id StreamID,
) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := id.Validate(); err != nil {
		return false, fmt.Errorf("release pending idempotency key: %w", err)
	}
	result, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisReleasePendingIdempotencyScript.Run(
			ctx,
			r.client,
			[]string{r.idempotencyKey(digest), r.idempotencyAssociationKey(id)},
			id.String(),
		).Int64()
	})
	if err != nil {
		return false, fmt.Errorf("release pending Redis idempotency key: %w", err)
	}
	return result == 1, nil
}

// Refresh extends an existing idempotency mapping.
func (r *Redis) Refresh(ctx context.Context, digest IdempotencyDigest, ttl time.Duration) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := validateIdempotencyTTL(ttl); err != nil {
		return false, err
	}
	result, err := redisRefreshIdempotencyScript.Run(
		ctx,
		r.client,
		[]string{r.idempotencyKey(digest)},
		durationMillis(ttl),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("refresh Redis idempotency key: %w", err)
	}
	return result == 1, nil
}

// Remove deletes one idempotency mapping by digest.
func (r *Redis) Remove(ctx context.Context, digest IdempotencyDigest) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	removed, err := r.client.Del(ctx, r.idempotencyKey(digest)).Result()
	if err != nil {
		return false, fmt.Errorf("remove Redis idempotency key: %w", err)
	}
	return removed != 0, nil
}

// Expire is a no-op because Redis expires bindings server-side. It remains part
// of the shared registry interface so callers can use memory and Redis alike.
func (r *Redis) Expire(ctx context.Context) (int, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (r *Redis) runTail(
	ctx context.Context,
	id StreamID,
	fromSeq uint64,
	initial StreamState,
	degraded bool,
	out chan<- Entry,
) {
	defer close(out)
	state := initial
	cursor, completed, replayErr := r.replayRedisRange(
		ctx,
		id,
		fromSeq,
		initial.LastSeq,
		1,
		func(entry Entry) bool {
			select {
			case out <- entry:
				return true
			case <-ctx.Done():
				return false
			}
		},
	)
	if replayErr != nil {
		r.sendTailError(ctx, out, replayErr)
		return
	}
	if !completed {
		return
	}
	if degraded {
		r.sendTailError(ctx, out, fmt.Errorf("%w: stream %s lost journal continuity", ErrOffsetExpired, id))
		return
	}
	if state.Status != StatusOpen {
		return
	}
	for {
		if degraded && cursor >= state.LastSeq {
			r.sendTailError(ctx, out, fmt.Errorf("%w: stream %s lost journal continuity", ErrOffsetExpired, id))
			return
		}
		if state.Status != StatusOpen && cursor >= state.LastSeq {
			return
		}

		streams, err := r.client.XRead(ctx, &redislib.XReadArgs{
			Streams: []string{r.eventsKey(id), redisStreamID(cursor)},
			// Decode at most one live entry before handing it to the byte-aware
			// delivery pump. A larger batch would retain decoded payloads outside
			// ReaderMaxLagBytes while a client is stalled.
			Count: 1,
			Block: r.config.ReadBlock,
		}).Result()
		if err != nil && !errors.Is(err, redislib.Nil) {
			if ctx.Err() == nil {
				if strings.Contains(strings.ToUpper(err.Error()), "WRONGTYPE") {
					r.sendTailError(ctx, out, fmt.Errorf(
						"%w: stream %s events cannot be replayed",
						ErrOffsetExpired,
						id,
					))
				} else {
					r.sendTailError(ctx, out, fmt.Errorf("tail Redis stream %s: %w", id, err))
				}
			}
			return
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				entry, decodeErr := decodeRedisEntry(message)
				if decodeErr != nil {
					r.sendTailError(ctx, out, decodeErr)
					return
				}
				if entry.Seq <= cursor {
					continue
				}
				if cursor == ^uint64(0) || entry.Seq != cursor+1 || message.ID != redisStreamID(entry.Seq) {
					r.sendTailError(ctx, out, fmt.Errorf(
						"%w: stream %s expected sequence %d and received %d",
						ErrOffsetExpired,
						id,
						cursor+1,
						entry.Seq,
					))
					return
				}
				select {
				case out <- entry:
					cursor = entry.Seq
				case <-ctx.Done():
					return
				}
				if entry.Kind.terminal() {
					return
				}
			}
		}
		if len(streams) != 0 {
			continue
		}

		fields, tombstoneExists, eventsExist, headerErr := r.header(ctx, id)
		if headerErr != nil {
			if ctx.Err() == nil {
				r.sendTailError(ctx, out, headerErr)
			}
			return
		}
		state, degraded, headerErr = r.decodeState(
			id,
			fields,
			tombstoneExists,
			eventsExist,
			nil,
			false,
		)
		if headerErr != nil {
			r.sendTailError(ctx, out, headerErr)
			return
		}
		if state.LastSeq > cursor {
			if cursor == ^uint64(0) {
				r.sendTailError(ctx, out, fmt.Errorf("%w: stream %s sequence overflow", ErrOffsetExpired, id))
				return
			}
			next, rangeErr := r.client.XRangeN(
				ctx,
				r.eventsKey(id),
				redisStreamID(cursor+1),
				"+",
				1,
			).Result()
			if rangeErr != nil {
				r.sendTailError(ctx, out, fmt.Errorf(
					"%w: stream %s events cannot be replayed: %w",
					ErrOffsetExpired,
					id,
					rangeErr,
				))
				return
			}
			if len(next) == 0 {
				r.sendTailError(ctx, out, fmt.Errorf(
					"%w: stream %s has committed sequence %d after cursor %d but no retained event",
					ErrOffsetExpired,
					id,
					state.LastSeq,
					cursor,
				))
				return
			}
			entry, decodeErr := decodeRedisEntry(next[0])
			if decodeErr != nil || entry.Seq != cursor+1 || next[0].ID != redisStreamID(entry.Seq) {
				r.sendTailError(ctx, out, fmt.Errorf(
					"%w: stream %s expected sequence %d",
					ErrOffsetExpired,
					id,
					cursor+1,
				))
				return
			}
			select {
			case out <- entry:
				cursor = entry.Seq
			case <-ctx.Done():
				return
			}
			if entry.Kind.terminal() {
				return
			}
		}
	}
}

func (r *Redis) sendTailError(ctx context.Context, out chan<- Entry, err error) {
	select {
	case out <- Entry{Err: err}:
	case <-ctx.Done():
	}
}

func (r *Redis) header(ctx context.Context, id StreamID) (map[string]string, bool, bool, error) {
	if err := id.Validate(); err != nil {
		return nil, false, false, fmt.Errorf("read stream: %w", err)
	}
	keys := r.streamKeys(id)
	commands, err := r.client.Pipelined(ctx, func(pipe redislib.Pipeliner) error {
		pipe.HGetAll(ctx, keys[0])
		pipe.Exists(ctx, keys[2])
		pipe.Exists(ctx, keys[1])
		return nil
	})
	if err != nil {
		return nil, false, false, fmt.Errorf("read Redis stream %s: %w", id, err)
	}
	fields, ok := commands[0].(*redislib.MapStringStringCmd)
	if !ok {
		return nil, false, false, fmt.Errorf("read Redis stream %s: unexpected state response", id)
	}
	tombstone, ok := commands[1].(*redislib.IntCmd)
	if !ok {
		return nil, false, false, fmt.Errorf("read Redis stream %s: unexpected tombstone response", id)
	}
	events, ok := commands[2].(*redislib.IntCmd)
	if !ok {
		return nil, false, false, fmt.Errorf("read Redis stream %s: unexpected events response", id)
	}
	return fields.Val(), tombstone.Val() != 0, events.Val() != 0, nil
}

func (r *Redis) decodeState(
	id StreamID,
	fields map[string]string,
	tombstoneExists bool,
	eventsExist bool,
	messages []redislib.XMessage,
	validateReplay bool,
) (StreamState, bool, error) {
	if len(fields) == 0 {
		if tombstoneExists {
			return StreamState{}, false, fmt.Errorf("%w: %s", ErrExpired, id)
		}
		return StreamState{}, false, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !eventsExist {
		return StreamState{}, false, fmt.Errorf(
			"%w: stream %s metadata exists but its events are unavailable",
			ErrOffsetExpired,
			id,
		)
	}
	createdAt, err := parseRedisTime(fields["created_at"])
	if err != nil {
		return StreamState{}, false, fmt.Errorf("decode Redis stream %s created time: %w", id, err)
	}
	updatedAt, err := parseRedisTime(fields["updated_at"])
	if err != nil {
		return StreamState{}, false, fmt.Errorf("decode Redis stream %s updated time: %w", id, err)
	}
	earliest, err := strconv.ParseUint(fields["earliest_seq"], 10, 64)
	if err != nil {
		return StreamState{}, false, fmt.Errorf("decode Redis stream %s earliest sequence: %w", id, err)
	}
	last, err := strconv.ParseUint(fields["last_seq"], 10, 64)
	if err != nil {
		return StreamState{}, false, fmt.Errorf("decode Redis stream %s last sequence: %w", id, err)
	}
	state := StreamState{
		StreamID:       id,
		Status:         StreamStatus(fields["status"]),
		Resumable:      fields["resumable"] == "1",
		Model:          fields["model"],
		OriginBackend:  fields["origin_backend"],
		CurrentBackend: fields["origin_backend"],
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		EarliestSeq:    earliest,
		LastSeq:        last,
		Migrations:     make([]Migration, 0),
	}
	if fields["has_model_version"] == "1" {
		modelVersion := fields["model_version"]
		state.ModelVersion = cloneString(&modelVersion)
	}
	if validateReplay && len(messages) == 0 {
		return StreamState{}, false, fmt.Errorf(
			"%w: stream %s has metadata through sequence %d but no retained events",
			ErrOffsetExpired,
			id,
			last,
		)
	}
	expectedSequence := earliest
	for index, message := range messages {
		entry, decodeErr := decodeRedisEntry(message)
		if decodeErr != nil {
			return StreamState{}, false, decodeErr
		}
		if validateReplay && entry.Seq != expectedSequence {
			return StreamState{}, false, fmt.Errorf(
				"%w: stream %s expected retained sequence %d and found %d",
				ErrOffsetExpired,
				id,
				expectedSequence,
				entry.Seq,
			)
		}
		if validateReplay && message.ID != redisStreamID(entry.Seq) {
			return StreamState{}, false, fmt.Errorf(
				"%w: stream %s event sequence %d has a noncanonical Redis ID",
				ErrOffsetExpired,
				id,
				entry.Seq,
			)
		}
		if validateReplay && index == 0 && earliest == 1 && entry.Kind != KindOpen {
			return StreamState{}, false, fmt.Errorf(
				"%w: stream %s sequence 1 is not the open event",
				ErrOffsetExpired,
				id,
			)
		}
		applyRedisStateEntry(&state, entry)
		if validateReplay {
			if expectedSequence == ^uint64(0) && index != len(messages)-1 {
				return StreamState{}, false, fmt.Errorf("%w: stream %s sequence overflow", ErrOffsetExpired, id)
			}
			expectedSequence++
		}
	}
	if validateReplay && messages[len(messages)-1].ID != redisStreamID(last) {
		return StreamState{}, false, fmt.Errorf(
			"%w: stream %s metadata ends at sequence %d but retained events do not",
			ErrOffsetExpired,
			id,
			last,
		)
	}
	return state, fields["degraded"] == "1", nil
}

func applyRedisStateEntry(state *StreamState, entry Entry) {
	if entry.Kind == KindMigration {
		var payload migrationPayload
		if json.Unmarshal(entry.Payload, &payload) == nil {
			state.Migrations = append(state.Migrations, Migration{
				Seq:                 entry.Seq,
				FromBackend:         payload.FromBackend,
				ToBackend:           payload.ToBackend,
				Reason:              payload.Reason,
				RescuedTokens:       payload.RescuedTokens,
				TokenCountEstimated: payload.TokenCountEstimated,
				Attempt:             payload.Attempt,
			})
			state.CurrentBackend = payload.ToBackend
		}
	}
	var payload usagePayload
	if json.Unmarshal(entry.Payload, &payload) == nil && payload.Usage != nil {
		state.Usage = *payload.Usage
	}
	if entry.Kind.terminal() {
		state.Terminal = &TerminalState{
			Seq:     entry.Seq,
			TS:      entry.TS,
			Kind:    entry.Kind,
			Payload: bytes.Clone(entry.Payload),
		}
	}
}

func decodeRedisEntry(message redislib.XMessage) (Entry, error) {
	parts := strings.SplitN(message.ID, "-", 2)
	if len(parts) != 2 {
		return Entry{}, fmt.Errorf("decode Redis entry: invalid stream ID %q", message.ID)
	}
	seq, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || seq == 0 {
		return Entry{}, fmt.Errorf("decode Redis entry sequence %q: %w", parts[0], err)
	}
	kind := EntryKind(redisValue(message.Values, "kind"))
	if kind != KindOpen && !kind.appendable() && !kind.terminal() {
		return Entry{}, fmt.Errorf("decode Redis entry %d: %w: unknown kind %q", seq, ErrInvalidEntry, kind)
	}
	payload := json.RawMessage(redisValue(message.Values, "payload"))
	if err := validatePayload(payload); err != nil {
		return Entry{}, fmt.Errorf("decode Redis entry %d: %w", seq, err)
	}
	ts, err := parseRedisTime(redisValue(message.Values, "ts"))
	if err != nil {
		return Entry{}, fmt.Errorf("decode Redis entry %d time: %w", seq, err)
	}
	return Entry{Seq: seq, TS: ts, Kind: kind, Payload: bytes.Clone(payload)}, nil
}

func redisValue(values map[string]any, key string) string {
	value, ok := values[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func parseRedisTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func validateRedisCursor(state StreamState, fromSeq uint64) error {
	if fromSeq > state.LastSeq {
		return fmt.Errorf("%w: cursor %d exceeds last sequence %d", ErrCursorAhead, fromSeq, state.LastSeq)
	}
	if state.EarliestSeq > 1 && fromSeq < state.EarliestSeq-1 {
		return fmt.Errorf("%w: cursor %d precedes earliest sequence %d", ErrOffsetExpired, fromSeq, state.EarliestSeq)
	}
	return nil
}

func (r *Redis) mapError(operation string, id StreamID, err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "STREAMWELD_ALREADY_EXISTS"):
		return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
	case strings.Contains(message, "STREAMWELD_NOT_FOUND"):
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	case strings.Contains(message, "STREAMWELD_EXPIRED"):
		return fmt.Errorf("%w: %s", ErrExpired, id)
	case strings.Contains(message, "STREAMWELD_OFFSET_EXPIRED"):
		return fmt.Errorf("%w: stream %s lost journal continuity", ErrOffsetExpired, id)
	case strings.Contains(message, "STREAMWELD_DEGRADED"):
		return fmt.Errorf("%w: stream %s lost journal continuity", ErrDegraded, id)
	case strings.Contains(message, "STREAMWELD_TERMINAL"):
		return fmt.Errorf("%w: stream %s is terminal", ErrTerminalState, id)
	case strings.Contains(message, "STREAMWELD_NONCE_CONFLICT"):
		return fmt.Errorf("%w: stream %s operation nonce conflict", ErrInvalidEntry, id)
	case strings.Contains(message, "STREAMWELD_RESERVATION_LOST"):
		return fmt.Errorf("%w: %s", ErrIdempotencyReservationLost, id)
	default:
		return fmt.Errorf("%s Redis stream %s: %w", operation, id, err)
	}
}

func (r *Redis) streamKeys(id StreamID) []string {
	return []string{
		r.streamKey(id, "state"),
		r.streamKey(id, "events"),
		r.streamKey(id, "exists"),
	}
}

func (r *Redis) openKeys(id StreamID) []string {
	return append(r.streamKeys(id), r.idempotencyAssociationKey(id))
}

func (r *Redis) streamKey(id StreamID, suffix string) string {
	return r.redisNamespace() + ":stream:" + id.String() + ":" + suffix
}

func (r *Redis) redisNamespace() string {
	// A prefix-wide hash tag lets stream and idempotency keys participate in
	// one Lua transaction when the client connects to Redis Cluster.
	return r.config.Prefix + ":{" + r.config.Prefix + "}"
}

func (r *Redis) eventsKey(id StreamID) string {
	return r.streamKeys(id)[1]
}

func (r *Redis) idempotencyKey(digest IdempotencyDigest) string {
	return r.redisNamespace() + ":idempotency:" + digest.String()
}

func (r *Redis) idempotencyAssociationKey(id StreamID) string {
	return r.streamKey(id, "idempotency")
}

func newRedisOperationNonce() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate Redis operation nonce: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func (r *Redis) operationNonce() (string, error) {
	if r.nonce == nil {
		return "", errors.New("Redis operation nonce source is nil")
	}
	nonce, err := r.nonce()
	if err != nil {
		return "", err
	}
	if nonce == "" {
		return "", errors.New("Redis operation nonce is empty")
	}
	return nonce, nil
}

// retryRedisOperation retries at most once, and only for connection and Redis
// availability errors. Callers must reuse an operation nonce so an ambiguous
// execute-then-disconnect result is returned instead of applied twice.
func retryRedisOperation[T any](ctx context.Context, operation func() (T, error)) (T, error) {
	result, err := operation()
	if err == nil || !retryableRedisOperationError(ctx, err) {
		return result, err
	}
	timer := time.NewTimer(redisRetryBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-timer.C:
	}
	return operation()
}

func retryableRedisOperationError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	message := strings.ToUpper(strings.TrimSpace(err.Error()))
	for _, prefix := range []string{"LOADING ", "TRYAGAIN ", "CLUSTERDOWN ", "MASTERDOWN ", "READONLY ", "NOREPLICAS "} {
		if strings.HasPrefix(message, prefix) {
			return true
		}
	}
	return false
}

func parseRedisNonceResult(operation string, result []any, nonce string) (uint64, error) {
	if len(result) != 2 {
		return 0, fmt.Errorf("%s Redis operation: unexpected result length %d", operation, len(result))
	}
	sequence, err := strconv.ParseUint(fmt.Sprint(result[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s Redis operation: invalid result: %w", operation, err)
	}
	if returnedNonce := fmt.Sprint(result[1]); returnedNonce != nonce {
		return 0, fmt.Errorf("%s Redis operation: operation nonce mismatch", operation)
	}
	return sequence, nil
}

func redisStreamID(seq uint64) string {
	return strconv.FormatUint(seq, 10) + "-0"
}

func durationMillis(duration time.Duration) int64 {
	milliseconds := duration.Milliseconds()
	if milliseconds == 0 {
		return 1
	}
	return milliseconds
}

func redisPendingTTL(ttl time.Duration) time.Duration {
	if ttl < defaultRedisPendingTTL {
		return ttl
	}
	return defaultRedisPendingTTL
}

func redisInteger(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected integer result %T", value)
	}
}
