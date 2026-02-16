package migrate

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

var (
	// ErrInvalidUTF8 indicates invalid or terminally incomplete content bytes.
	ErrInvalidUTF8 = errors.New("migrate: invalid UTF-8")
	// ErrInvalidSeamWindow indicates a non-positive byte window.
	ErrInvalidSeamWindow = errors.New("migrate: invalid seam window")
	// ErrUTF8BufferComplete indicates a write after the leading window became
	// available or Finish was called.
	ErrUTF8BufferComplete = errors.New("migrate: UTF-8 window buffer is complete")
)

// UTF8WindowBuffer accumulates leading continuation bytes until the configured
// window can be split at a UTF-8 boundary. If the byte boundary crosses a rune,
// Write waits for the complete rune and Split places that rune in Remainder.
type UTF8WindowBuffer struct {
	window  int
	data    []byte
	heldLen int
	ready   bool
}

// NewUTF8WindowBuffer returns an empty leading-window buffer.
func NewUTF8WindowBuffer(window int) (*UTF8WindowBuffer, error) {
	if window <= 0 {
		return nil, fmt.Errorf("%w: bytes must be positive", ErrInvalidSeamWindow)
	}
	return &UTF8WindowBuffer{window: window}, nil
}

// Write adds an arbitrary byte fragment. It returns true once Split is ready.
// An incomplete UTF-8 suffix is retained until a later Write or Finish.
func (buffer *UTF8WindowBuffer) Write(fragment []byte) (bool, error) {
	if buffer == nil || buffer.window <= 0 {
		return false, ErrInvalidSeamWindow
	}
	if buffer.ready {
		return true, ErrUTF8BufferComplete
	}
	previousLength := len(buffer.data)
	buffer.data = append(buffer.data, fragment...)
	completeLength, invalid := completeUTF8Prefix(buffer.data)
	if invalid {
		buffer.data = buffer.data[:previousLength]
		return false, ErrInvalidUTF8
	}
	if completeLength < buffer.window {
		return false, nil
	}
	buffer.heldLen = utf8BoundaryAtOrBefore(buffer.data, buffer.window)
	buffer.ready = true
	return true, nil
}

// Finish marks a shorter terminated attempt ready. A trailing partial rune is
// invalid because no later content can complete it.
func (buffer *UTF8WindowBuffer) Finish() error {
	if buffer == nil || buffer.window <= 0 {
		return ErrInvalidSeamWindow
	}
	completeLength, invalid := completeUTF8Prefix(buffer.data)
	if invalid || completeLength != len(buffer.data) {
		return ErrInvalidUTF8
	}
	if buffer.ready {
		return nil
	}
	buffer.heldLen = len(buffer.data)
	buffer.ready = true
	return nil
}

// Split returns independent copies of the held inspection window and bytes
// already read beyond it. ok is false until Write reports ready or Finish
// succeeds.
func (buffer *UTF8WindowBuffer) Split() (held, remainder []byte, ok bool) {
	if buffer == nil || !buffer.ready {
		return nil, nil, false
	}
	return append([]byte(nil), buffer.data[:buffer.heldLen]...),
		append([]byte(nil), buffer.data[buffer.heldLen:]...), true
}

func completeUTF8Prefix(data []byte) (int, bool) {
	position := 0
	for position < len(data) {
		_, size := utf8.DecodeRune(data[position:])
		if size == 1 && data[position] >= utf8.RuneSelf {
			if !utf8.FullRune(data[position:]) {
				return position, false
			}
			return position, true
		}
		position += size
	}
	return position, false
}

func utf8BoundaryAtOrBefore(data []byte, boundary int) int {
	if boundary >= len(data) {
		return len(data)
	}
	for boundary > 0 && !utf8.RuneStart(data[boundary]) {
		boundary--
	}
	return boundary
}
