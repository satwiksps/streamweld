package journal

import (
	"context"
	"fmt"

	redislib "github.com/redis/go-redis/v9"
)

var redisRemoveIfBoundScript = redislib.NewScript(`
local expected = 'r|' .. ARGV[1]
if redis.call('GET', KEYS[1]) ~= expected then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
`)

// RemoveIfBound atomically removes only a promoted mapping that still points
// at id. Pending reservations and replacement winners are never deleted.
func (r *Redis) RemoveIfBound(
	ctx context.Context,
	digest IdempotencyDigest,
	id StreamID,
) (bool, error) {
	if err := checkIdempotencyContext(ctx); err != nil {
		return false, err
	}
	if err := id.Validate(); err != nil {
		return false, fmt.Errorf("conditionally remove Redis idempotency key: %w", err)
	}
	removed, err := retryRedisOperation(ctx, func() (int64, error) {
		return redisRemoveIfBoundScript.Run(
			ctx,
			r.client,
			[]string{r.idempotencyKey(digest)},
			id.String(),
		).Int64()
	})
	if err != nil {
		return false, fmt.Errorf("conditionally remove Redis idempotency key: %w", err)
	}
	return removed == 1, nil
}
