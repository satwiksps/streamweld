package journal

import (
	"context"
	"fmt"

	redislib "github.com/redis/go-redis/v9"
)

const redisStateReplayPageEntries int64 = 8

// boundedRedisSnapshot captures the replay boundary and validates the retained
// stream with constant-size Redis replies. It intentionally reads only the
// first and last event plus XLEN; callers page the interior after this atomic
// attach instead of materializing XRANGE - +.
func (r *Redis) boundedRedisSnapshot(ctx context.Context, id StreamID) (StreamState, bool, error) {
	if err := id.Validate(); err != nil {
		return StreamState{}, false, fmt.Errorf("read stream: %w", err)
	}
	keys := r.streamKeys(id)
	var stateCommand *redislib.MapStringStringCmd
	var tombstoneCommand *redislib.IntCmd
	var eventsCommand *redislib.IntCmd
	var lengthCommand *redislib.IntCmd
	var firstCommand *redislib.XMessageSliceCmd
	var lastCommand *redislib.XMessageSliceCmd
	_, err := r.client.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		stateCommand = pipe.HGetAll(ctx, keys[0])
		tombstoneCommand = pipe.Exists(ctx, keys[2])
		eventsCommand = pipe.Exists(ctx, keys[1])
		lengthCommand = pipe.XLen(ctx, keys[1])
		firstCommand = pipe.XRangeN(ctx, keys[1], "-", "+", 1)
		lastCommand = pipe.XRevRangeN(ctx, keys[1], "+", "-", 1)
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return StreamState{}, false, ctx.Err()
		}
		if stateCommand != nil && len(stateCommand.Val()) != 0 {
			return StreamState{}, false, fmt.Errorf(
				"%w: stream %s events cannot be replayed: %w",
				ErrOffsetExpired,
				id,
				err,
			)
		}
		return StreamState{}, false, fmt.Errorf("inspect Redis stream %s: %w", id, err)
	}
	state, degraded, err := r.decodeState(
		id,
		stateCommand.Val(),
		tombstoneCommand.Val() != 0,
		eventsCommand.Val() != 0,
		nil,
		false,
	)
	if err != nil {
		return StreamState{}, false, err
	}
	if err := validateRedisRetainedBounds(
		state,
		degraded,
		lengthCommand.Val(),
		firstCommand.Val(),
		lastCommand.Val(),
	); err != nil {
		return StreamState{}, false, err
	}
	return state, degraded, nil
}

func validateRedisRetainedBounds(
	state StreamState,
	degraded bool,
	length int64,
	first []redislib.XMessage,
	last []redislib.XMessage,
) error {
	if state.EarliestSeq == 0 || state.LastSeq < state.EarliestSeq {
		return fmt.Errorf(
			"%w: stream %s has invalid retained bounds %d..%d",
			ErrOffsetExpired,
			state.StreamID,
			state.EarliestSeq,
			state.LastSeq,
		)
	}
	expectedLength := state.LastSeq - state.EarliestSeq + 1
	if length < 0 || uint64(length) != expectedLength || len(first) != 1 || len(last) != 1 {
		return fmt.Errorf(
			"%w: stream %s metadata covers %d events but Redis retains %d",
			ErrOffsetExpired,
			state.StreamID,
			expectedLength,
			length,
		)
	}
	firstEntry, err := decodeRedisEntry(first[0])
	if err != nil || firstEntry.Seq != state.EarliestSeq || first[0].ID != redisStreamID(firstEntry.Seq) {
		return fmt.Errorf(
			"%w: stream %s has an invalid first retained event",
			ErrOffsetExpired,
			state.StreamID,
		)
	}
	lastEntry, err := decodeRedisEntry(last[0])
	if err != nil || lastEntry.Seq != state.LastSeq || last[0].ID != redisStreamID(lastEntry.Seq) {
		return fmt.Errorf(
			"%w: stream %s has an invalid last retained event",
			ErrOffsetExpired,
			state.StreamID,
		)
	}
	if state.EarliestSeq == 1 && firstEntry.Kind != KindOpen {
		return fmt.Errorf(
			"%w: stream %s sequence 1 is not the open event",
			ErrOffsetExpired,
			state.StreamID,
		)
	}
	switch state.Status {
	case StatusOpen:
		if lastEntry.Kind.terminal() {
			return fmt.Errorf(
				"%w: stream %s is open but its last event is terminal",
				ErrOffsetExpired,
				state.StreamID,
			)
		}
	case StatusDone, StatusError, StatusStopped:
		if !lastEntry.Kind.terminal() || statusForTerminal(lastEntry.Kind) != state.Status {
			return fmt.Errorf(
				"%w: stream %s terminal metadata and event disagree",
				ErrOffsetExpired,
				state.StreamID,
			)
		}
	default:
		return fmt.Errorf(
			"%w: stream %s has invalid status %q",
			ErrOffsetExpired,
			state.StreamID,
			state.Status,
		)
	}
	if degraded && state.Status != StatusOpen {
		return fmt.Errorf(
			"%w: stream %s is both degraded and terminal",
			ErrOffsetExpired,
			state.StreamID,
		)
	}
	return nil
}

// replayRedisRange pages a fixed call-time sequence interval in canonical
// order. completed is false only when yield deliberately stops early.
func (r *Redis) replayRedisRange(
	ctx context.Context,
	id StreamID,
	fromSeq uint64,
	throughSeq uint64,
	pageEntries int64,
	yield func(Entry) bool,
) (cursor uint64, completed bool, err error) {
	cursor = fromSeq
	for cursor < throughSeq {
		if err := checkContext(ctx); err != nil {
			return cursor, false, err
		}
		expected := cursor + 1
		messages, rangeErr := r.client.XRangeN(
			ctx,
			r.eventsKey(id),
			redisStreamID(expected),
			redisStreamID(throughSeq),
			pageEntries,
		).Result()
		if rangeErr != nil {
			if ctx.Err() != nil {
				return cursor, false, ctx.Err()
			}
			return cursor, false, fmt.Errorf(
				"%w: stream %s events cannot be replayed: %w",
				ErrOffsetExpired,
				id,
				rangeErr,
			)
		}
		if len(messages) == 0 {
			return cursor, false, fmt.Errorf(
				"%w: stream %s expected sequence %d",
				ErrOffsetExpired,
				id,
				expected,
			)
		}
		for _, message := range messages {
			if err := checkContext(ctx); err != nil {
				return cursor, false, err
			}
			entry, decodeErr := decodeRedisEntry(message)
			if decodeErr != nil || entry.Seq != expected || message.ID != redisStreamID(expected) {
				return cursor, false, fmt.Errorf(
					"%w: stream %s expected sequence %d",
					ErrOffsetExpired,
					id,
					expected,
				)
			}
			if entry.Kind.terminal() && entry.Seq != throughSeq {
				return cursor, false, fmt.Errorf(
					"%w: stream %s became terminal before replay boundary %d",
					ErrOffsetExpired,
					id,
					throughSeq,
				)
			}
			cursor = entry.Seq
			expected++
			if !yield(entry) {
				return cursor, false, nil
			}
		}
	}
	return cursor, true, nil
}
