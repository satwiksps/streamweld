package journal

import (
	"context"
	"encoding/json"
	"fmt"
)

type redisTailPendingEntry struct {
	entry Entry
	bytes int64
}

// runRedisTailDelivery decouples Redis reads from a client reader. The source
// can continue draining XREAD into a byte-bounded queue while the public Tail
// channel is blocked. Once the queue exceeds its budget, only that reader is
// evicted and receives ErrReaderLagged.
func runRedisTailDelivery(
	ctx context.Context,
	cancel context.CancelFunc,
	id StreamID,
	maxLagBytes int64,
	source <-chan Entry,
	out chan Entry,
) {
	defer close(out)
	defer cancel()

	pending := make([]redisTailPendingEntry, 0)
	pendingBytes := int64(0)
	for source != nil || len(pending) != 0 {
		// Prefer a ready reader before accepting more source data. This makes
		// the one-envelope oversize allowance useful to a ready client instead
		// of randomly evicting it when the next Redis entry is also ready.
		if len(pending) != 0 {
			select {
			case out <- pending[0].entry:
				pendingBytes -= pending[0].bytes
				pending[0] = redisTailPendingEntry{}
				pending = pending[1:]
				continue
			default:
			}
		}

		var destination chan Entry
		var next Entry
		if len(pending) != 0 {
			destination = out
			next = pending[0].entry
		}

		select {
		case <-ctx.Done():
			return
		case entry, open := <-source:
			if !open {
				source = nil
				continue
			}
			size := redisTailEntrySize(entry)
			fits := size <= maxLagBytes && pendingBytes <= maxLagBytes-size
			if entry.Err == nil && len(pending) != 0 && !fits {
				// One complete envelope may exceed the configured payload-sized
				// budget because its persisted envelope adds metadata. It is only
				// allowed as the sole pending entry; any additional backlog evicts
				// this reader without affecting Redis writers or other readers.
				select {
				case out <- Entry{Err: fmt.Errorf(
					"%w: stream %s exceeded %d pending bytes",
					ErrReaderLagged,
					id,
					maxLagBytes,
				)}:
				case <-ctx.Done():
				}
				return
			}
			pending = append(pending, redisTailPendingEntry{entry: entry, bytes: size})
			pendingBytes += size
		case destination <- next:
			pendingBytes -= pending[0].bytes
			pending[0] = redisTailPendingEntry{}
			pending = pending[1:]
		}
	}
}

func redisTailEntrySize(entry Entry) int64 {
	if entry.Err != nil {
		return 0
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		// Entries reaching a Redis tail were already decoded and validated,
		// so this is defensive. Treat an impossible encoding failure as a
		// maximally large entry and evict the affected reader.
		return int64(^uint64(0) >> 1)
	}
	return int64(len(encoded))
}
