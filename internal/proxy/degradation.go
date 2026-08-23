package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

const (
	degradationMarkerRetryMin = 100 * time.Millisecond
	degradationMarkerRetryMax = 2 * time.Second
)

// degradedFrame is either a shadow of an already committed journal entry or
// an unsequenced event emitted after sequence allocation has stopped. Keeping
// committed shadows in each attached reader's bounded queue lets a reader
// switch from a failed Tail to the process-local relay without skipping an
// entry which committed immediately before the degradation boundary.
type degradedFrame struct {
	entry       *journal.Entry
	event       sse.Event
	terminal    bool
	verboseOnly bool
	size        int64
}

type degradedFeed struct {
	mu sync.Mutex

	maxLagBytes int64
	activated   chan struct{}
	active      bool
	boundary    uint64
	closed      bool

	nextSubscriberID uint64
	subscribers      map[uint64]*degradedSubscriber
}

type degradedSubscriber struct {
	required bool
	queue    []degradedFrame
	bytes    int64
	ackedSeq uint64
	changed  chan struct{}
	evicted  chan struct{}
	lagged   bool
}

type degradedSubscription struct {
	feed *degradedFeed
	id   uint64
	once sync.Once
}

func newDegradedFeed(maxLagBytes int64) *degradedFeed {
	return &degradedFeed{
		maxLagBytes: maxLagBytes,
		activated:   make(chan struct{}),
		subscribers: make(map[uint64]*degradedSubscriber),
	}
}

// subscribe registers a reader before degradation. The required bit records
// which reader owns the initial request for diagnostics, but every reader has
// the same hard lag budget: a slow network consumer must never block producer
// progress. A normally draining initial reader still receives the complete
// generation.
func (feed *degradedFeed) subscribe(required bool) (*degradedSubscription, bool) {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.active || feed.closed {
		return nil, false
	}
	feed.nextSubscriberID++
	id := feed.nextSubscriberID
	feed.subscribers[id] = &degradedSubscriber{
		required: required,
		changed:  make(chan struct{}),
		evicted:  make(chan struct{}),
	}
	return &degradedSubscription{feed: feed, id: id}, true
}

func (feed *degradedFeed) status() (boundary uint64, active bool) {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.boundary, feed.active
}

func (feed *degradedFeed) subscriberCount() int {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return len(feed.subscribers)
}

func (feed *degradedFeed) queuedBytes() int64 {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	var total int64
	for _, subscriber := range feed.subscribers {
		total += subscriber.bytes
	}
	return total
}

func (feed *degradedFeed) publishCommitted(ctx context.Context, entry journal.Entry) error {
	cloned := entry
	cloned.Payload = bytes.Clone(entry.Payload)
	frame := degradedFrame{entry: &cloned, size: degradedEntrySize(cloned)}
	return feed.broadcast(ctx, frame)
}

func (feed *degradedFeed) activate(ctx context.Context, boundary uint64, frames ...degradedFrame) error {
	feed.mu.Lock()
	if feed.closed {
		feed.mu.Unlock()
		return nil
	}
	if !feed.active {
		feed.active = true
		feed.boundary = boundary
		close(feed.activated)
	}
	feed.mu.Unlock()
	for _, frame := range frames {
		if err := feed.broadcast(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (feed *degradedFeed) append(ctx context.Context, frames ...degradedFrame) error {
	feed.mu.Lock()
	active := feed.active && !feed.closed
	feed.mu.Unlock()
	if !active {
		return nil
	}
	for _, frame := range frames {
		if err := feed.broadcast(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (feed *degradedFeed) close() {
	feed.mu.Lock()
	if feed.closed {
		feed.mu.Unlock()
		return
	}
	feed.closed = true
	for _, subscriber := range feed.subscribers {
		feed.signalLocked(subscriber)
	}
	feed.mu.Unlock()
}

func (feed *degradedFeed) broadcast(ctx context.Context, frame degradedFrame) error {
	if ctx == nil {
		return journal.ErrInvalidContext
	}
	if frame.size <= 0 {
		frame.size = degradedEventSize(frame)
	}

	feed.mu.Lock()
	if feed.closed || len(feed.subscribers) == 0 {
		feed.mu.Unlock()
		return nil
	}
	readers := make([]uint64, 0, len(feed.subscribers))
	for id, subscriber := range feed.subscribers {
		_ = subscriber.required
		readers = append(readers, id)
	}
	feed.mu.Unlock()

	// Every enqueue is nonblocking. Readers which exceed their independent
	// budget receive ErrReaderLagged while the producer continues.
	for _, id := range readers {
		feed.enqueueSecondary(id, frame)
	}
	return nil
}

func (feed *degradedFeed) enqueueSecondary(id uint64, frame degradedFrame) {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	subscriber, ok := feed.subscribers[id]
	if !ok || subscriber.lagged || feed.closed || committedFrameAcknowledged(subscriber, frame) {
		return
	}
	if subscriber.bytes != 0 && subscriber.bytes+frame.size > feed.maxLagBytes {
		subscriber.queue = nil
		subscriber.bytes = 0
		subscriber.lagged = true
		close(subscriber.evicted)
		feed.signalLocked(subscriber)
		return
	}
	subscriber.queue = append(subscriber.queue, frame)
	subscriber.bytes += frame.size
	feed.signalLocked(subscriber)
}

func committedFrameAcknowledged(subscriber *degradedSubscriber, frame degradedFrame) bool {
	return frame.entry != nil && frame.entry.Seq <= subscriber.ackedSeq
}

func (feed *degradedFeed) signalLocked(subscriber *degradedSubscriber) {
	close(subscriber.changed)
	subscriber.changed = make(chan struct{})
}

func (subscription *degradedSubscription) activated() <-chan struct{} {
	if subscription == nil || subscription.feed == nil {
		return neverSignal()
	}
	return subscription.feed.activated
}

func (subscription *degradedSubscription) evicted() <-chan struct{} {
	if subscription == nil || subscription.feed == nil {
		return neverSignal()
	}
	subscription.feed.mu.Lock()
	defer subscription.feed.mu.Unlock()
	subscriber, ok := subscription.feed.subscribers[subscription.id]
	if !ok {
		return closedSignal()
	}
	return subscriber.evicted
}

func (subscription *degradedSubscription) acknowledge(seq uint64) {
	if subscription == nil || subscription.feed == nil || seq == 0 {
		return
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	subscriber, ok := feed.subscribers[subscription.id]
	if !ok || seq <= subscriber.ackedSeq {
		return
	}
	subscriber.ackedSeq = seq
	for len(subscriber.queue) != 0 {
		frame := subscriber.queue[0]
		if frame.entry == nil || frame.entry.Seq > seq {
			break
		}
		subscriber.queue[0] = degradedFrame{}
		subscriber.queue = subscriber.queue[1:]
		subscriber.bytes -= frame.size
	}
	if len(subscriber.queue) == 0 {
		subscriber.queue = nil
	}
	feed.signalLocked(subscriber)
}

func (subscription *degradedSubscription) next(ctx context.Context) (degradedFrame, bool, error) {
	if subscription == nil || subscription.feed == nil || ctx == nil {
		return degradedFrame{}, false, journal.ErrInvalidContext
	}
	feed := subscription.feed
	for {
		feed.mu.Lock()
		subscriber, ok := feed.subscribers[subscription.id]
		if !ok {
			feed.mu.Unlock()
			return degradedFrame{}, false, nil
		}
		if subscriber.lagged {
			feed.mu.Unlock()
			return degradedFrame{}, false, journal.ErrReaderLagged
		}
		if len(subscriber.queue) != 0 {
			frame := subscriber.queue[0]
			subscriber.queue[0] = degradedFrame{}
			subscriber.queue = subscriber.queue[1:]
			if len(subscriber.queue) == 0 {
				subscriber.queue = nil
			}
			subscriber.bytes -= frame.size
			feed.signalLocked(subscriber)
			feed.mu.Unlock()
			return frame, true, nil
		}
		if feed.closed {
			feed.mu.Unlock()
			return degradedFrame{}, false, nil
		}
		changed := subscriber.changed
		feed.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return degradedFrame{}, false, ctx.Err()
		}
	}
}

func (subscription *degradedSubscription) unsubscribe() {
	if subscription == nil || subscription.feed == nil {
		return
	}
	subscription.once.Do(func() {
		feed := subscription.feed
		feed.mu.Lock()
		if subscriber, ok := feed.subscribers[subscription.id]; ok {
			delete(feed.subscribers, subscription.id)
			feed.signalLocked(subscriber)
		}
		feed.mu.Unlock()
	})
}

func degradedEntrySize(entry journal.Entry) int64 {
	return int64(len(entry.Payload)) + 128
}

func degradedEventSize(frame degradedFrame) int64 {
	return int64(len(frame.event.Data)+len(frame.event.Type)+len(frame.event.ID)) + 64
}

var (
	neverSignalChannel  = make(chan struct{})
	closedSignalChannel = func() chan struct{} { result := make(chan struct{}); close(result); return result }()
)

func neverSignal() <-chan struct{}  { return neverSignalChannel }
func closedSignal() <-chan struct{} { return closedSignalChannel }

func (s *durableService) journalDegradedValue() int64 {
	return s.journalDegraded.Load()
}

func degradedChunkFrame(event sse.Event) degradedFrame {
	event.ID = ""
	event.HasID = false
	event.Data = bytes.Clone(event.Data)
	frame := degradedFrame{event: event}
	frame.size = degradedEventSize(frame)
	return frame
}

func degradedEntryFrame(entry journal.Entry) (degradedFrame, error) {
	mapping, ok := journalSSEMappings[entry.Kind]
	if !ok {
		return degradedFrame{}, fmt.Errorf("%w: unsupported degraded entry kind %q", ErrInvalidJournalEntry, entry.Kind)
	}
	payload, err := downstreamPayload(journal.Entry{Seq: 1, Kind: entry.Kind, Payload: entry.Payload})
	if err != nil {
		return degradedFrame{}, err
	}
	frame := degradedFrame{
		event: sse.Event{
			Data:    bytes.Clone(payload),
			Type:    mapping.eventType,
			HasData: true,
			HasType: mapping.eventType != "",
		},
		terminal:    mapping.terminal,
		verboseOnly: mapping.verboseOnly,
	}
	frame.size = degradedEventSize(frame)
	return frame, nil
}

func degradedWarningFrame() degradedFrame {
	payload, _ := json.Marshal(struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	}{
		Code:    "journal_degraded",
		Message: "stream journaling failed; subsequent events are not resumable",
		Details: map[string]any{},
	})
	frame := degradedFrame{event: sse.Event{
		Type:    streamWarningEvent,
		Data:    payload,
		HasType: true,
		HasData: true,
	}}
	frame.size = degradedEventSize(frame)
	return frame
}

func degradedTerminalFrame(kind journal.EntryKind, payload []byte) (degradedFrame, error) {
	if kind == journal.KindDone {
		frame := degradedFrame{
			event:    sse.Event{Data: []byte(doneSentinelData), HasData: true},
			terminal: true,
		}
		frame.size = degradedEventSize(frame)
		return frame, nil
	}
	return degradedEntryFrame(journal.Entry{Kind: kind, Payload: payload})
}

// enterJournalDegradedLocked is called with writeMu held. That serialization
// establishes the committed boundary and orders the warning before the failed
// event and all later producer output.
func (r *streamRuntime) enterJournalDegradedLocked(cause error, failed ...degradedFrame) {
	r.mu.Lock()
	if r.degraded {
		r.mu.Unlock()
		_ = r.degradedFeed.append(r.context, failed...)
		return
	}
	r.degraded = true
	boundary := r.lastSeq
	r.mu.Unlock()

	r.service.journalDegraded.Store(1)
	r.service.telemetry.JournalDegraded(r.telemetryLabels(), r.id.String(), true)
	r.service.logger.Error("journal unavailable after stream open; degrading stream",
		"stream_id", r.id,
		"error", cause,
	)
	r.startJournalDegradedLease()
	frames := make([]degradedFrame, 0, len(failed)+1)
	frames = append(frames, degradedWarningFrame())
	frames = append(frames, failed...)
	if err := r.degradedFeed.activate(r.context, boundary, frames...); err != nil && !errors.Is(err, context.Canceled) {
		r.service.logger.Warn("relay degraded stream", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) finishDegradedLocked(
	kind journal.EntryKind,
	payload []byte,
	stopResult *stopResponse,
) error {
	frame, err := degradedTerminalFrame(kind, payload)
	if err != nil {
		r.failTerminalTransition()
		return err
	}
	r.signalJournalDegradedTerminal()
	r.mu.Lock()
	r.terminal = kind
	r.terminating = false
	r.stopResult = stopResult
	lease := r.currentLease
	r.currentLease = nil
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	close(r.done)
	r.activeDone.Do(r.service.active.Done)
	r.mu.Unlock()
	if lease != nil {
		lease.Release()
	}
	r.recordTerminal(kind)
	// The producer context was canceled to interrupt the current upstream
	// attempt, so use the service lifetime while completing the attached reader.
	if appendErr := r.degradedFeed.append(r.service.rootContext, frame); appendErr != nil && !errors.Is(appendErr, context.Canceled) {
		r.service.logger.Warn("relay degraded terminal event", "stream_id", r.id, "error", appendErr)
	}
	r.degradedFeed.close()
	r.refreshIdempotency()
	r.scheduleRetentionExpiry()
	return nil
}

func (r *streamRuntime) startJournalDegradedLease() {
	if _, ok := r.service.journal.(journal.DegradationMarker); !ok {
		return
	}
	r.degradationMarkerOnce.Do(func() {
		r.service.degradationLeases.Add(1)
		go func() {
			defer r.service.degradationLeases.Done()
			r.runJournalDegradedLease()
		}()
	})
}

func (r *streamRuntime) signalJournalDegradedTerminal() {
	if _, ok := r.service.journal.(journal.DegradationMarker); !ok {
		return
	}
	r.startJournalDegradedLease()
	r.degradationTerminalOnce.Do(func() { close(r.degradationTerminal) })
}

func (r *streamRuntime) runJournalDegradedLease() {
	marker := r.service.journal.(journal.DegradationMarker)
	retry := degradationMarkerRetryMin
	refresh := r.policy.JournalTTL / 2
	if refresh <= 0 {
		refresh = time.Millisecond
	}
	terminalSignal := (<-chan struct{})(r.degradationTerminal)
	terminal := false
	var terminalDeadline time.Time

	for {
		err := r.tryMarkJournalDegraded(marker)
		if terminal {
			r.degradationAttemptOnce.Do(func() { close(r.degradationTerminalAttempt) })
		}
		if errors.Is(err, journal.ErrTerminalState) {
			r.degradationAttemptOnce.Do(func() { close(r.degradationTerminalAttempt) })
			return
		}
		if err == nil && terminal {
			return
		}
		if terminal && !terminalDeadline.IsZero() && time.Now().After(terminalDeadline) {
			return
		}

		delay := refresh
		if err != nil {
			delay = retry
			retry *= 2
			if retry > degradationMarkerRetryMax {
				retry = degradationMarkerRetryMax
			}
		} else {
			retry = degradationMarkerRetryMin
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-terminalSignal:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			terminal = true
			terminalSignal = nil
			terminalDeadline = time.Now().Add(r.policy.JournalTTL)
		case <-r.service.rootContext.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			r.degradationAttemptOnce.Do(func() { close(r.degradationTerminalAttempt) })
			return
		}
	}
}

func (r *streamRuntime) tryMarkJournalDegraded(marker journal.DegradationMarker) error {
	timeout := r.service.config.ReadinessTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(r.service.rootContext, timeout)
	defer cancel()
	err := marker.MarkDegraded(ctx, r.id)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		// The transition itself is logged once by enterJournalDegradedLocked;
		// retry failures intentionally do not flood logs.
		return err
	}
	return err
}
