package sse

import (
	"errors"
	"time"
)

var (
	// ErrInvalidUTF8 indicates that an event stream or an event passed to an
	// Encoder contains bytes that are not valid UTF-8.
	ErrInvalidUTF8 = errors.New("sse: invalid UTF-8")

	// ErrIncompleteEvent indicates that the input ended after a frame began but
	// before its terminating blank line. Such errors also wrap the underlying
	// I/O error (io.ErrUnexpectedEOF for an ordinary EOF).
	ErrIncompleteEvent = errors.New("sse: incomplete event")

	// ErrEventTooLarge indicates that a frame exceeded the Decoder's configured
	// maximum size before its terminating blank line.
	ErrEventTooLarge = errors.New("sse: event exceeds maximum size")

	// ErrInvalidEvent indicates that an Event cannot be represented as SSE.
	ErrInvalidEvent = errors.New("sse: invalid event")
)

// Event is one blank-line-terminated SSE frame.
//
// HasData, HasType, HasID, and HasRetry distinguish an absent field from a
// present field whose value is empty or zero. A Decoder always sets these flags
// precisely. Encoder also treats a non-zero corresponding value as present,
// making hand-built events concise while retaining lossless round trips for
// empty fields.
//
// Data contains all data fields joined with a single '\n', as required by the
// SSE wire format. Comments are retained in their original order. Repeated
// event, id, and valid retry fields use their last value.
type Event struct {
	Data     []byte
	Type     string
	ID       string
	Retry    time.Duration
	Comments []string

	HasData  bool
	HasType  bool
	HasID    bool
	HasRetry bool
}
