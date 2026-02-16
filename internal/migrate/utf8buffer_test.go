package migrate

import (
	"bytes"
	"errors"
	"testing"
	"unicode/utf8"
)

func TestUTF8WindowBufferSplitsAtExactBoundary(t *testing.T) {
	t.Parallel()

	buffer, err := NewUTF8WindowBuffer(4)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer() error = %v", err)
	}
	if ready, err := buffer.Write([]byte("ab")); err != nil || ready {
		t.Fatalf("Write(ab) = (%v, %v), want (false, nil)", ready, err)
	}
	if _, _, ok := buffer.Split(); ok {
		t.Fatal("Split() ready before window")
	}
	if ready, err := buffer.Write([]byte("cdef")); err != nil || !ready {
		t.Fatalf("Write(cdef) = (%v, %v), want (true, nil)", ready, err)
	}
	held, remainder, ok := buffer.Split()
	if !ok || string(held) != "abcd" || string(remainder) != "ef" {
		t.Fatalf("Split() = (%q, %q, %v)", held, remainder, ok)
	}
	held[0] = 'X'
	remainder[0] = 'Y'
	heldAgain, remainderAgain, _ := buffer.Split()
	if string(heldAgain) != "abcd" || string(remainderAgain) != "ef" {
		t.Fatalf("Split() exposed internal storage: %q %q", heldAgain, remainderAgain)
	}
}

func TestUTF8WindowBufferWaitsThroughRuneAndSplitsBeforeIt(t *testing.T) {
	t.Parallel()

	content := []byte("a雪z")
	buffer, err := NewUTF8WindowBuffer(2)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer() error = %v", err)
	}
	for index, fragment := range [][]byte{content[:2], content[2:3]} {
		if ready, writeErr := buffer.Write(fragment); writeErr != nil || ready {
			t.Fatalf("Write(fragment %d) = (%v, %v), want incomplete", index, ready, writeErr)
		}
	}
	if ready, writeErr := buffer.Write(content[3:]); writeErr != nil || !ready {
		t.Fatalf("Write(final rune bytes) = (%v, %v), want ready", ready, writeErr)
	}
	held, remainder, ok := buffer.Split()
	if !ok || string(held) != "a" || string(remainder) != "雪z" {
		t.Fatalf("Split() = (%q, %q, %v), want (a, 雪z, true)", held, remainder, ok)
	}
	if !utf8.Valid(held) || !utf8.Valid(remainder) {
		t.Fatalf("Split() produced invalid UTF-8: %x %x", held, remainder)
	}
}

func TestUTF8WindowBufferFinishShortAttempt(t *testing.T) {
	t.Parallel()

	buffer, err := NewUTF8WindowBuffer(64)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer() error = %v", err)
	}
	if ready, writeErr := buffer.Write([]byte("short 雪")); writeErr != nil || ready {
		t.Fatalf("Write() = (%v, %v), want incomplete window", ready, writeErr)
	}
	if err := buffer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	held, remainder, ok := buffer.Split()
	if !ok || string(held) != "short 雪" || len(remainder) != 0 {
		t.Fatalf("Split() = (%q, %q, %v)", held, remainder, ok)
	}

	empty, err := NewUTF8WindowBuffer(64)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer(empty) error = %v", err)
	}
	if err := empty.Finish(); err != nil {
		t.Fatalf("empty Finish() error = %v", err)
	}
	held, remainder, ok = empty.Split()
	if !ok || held != nil || remainder != nil {
		t.Fatalf("empty Split() = (%#v, %#v, %v)", held, remainder, ok)
	}
}

func TestUTF8WindowBufferRejectsInvalidAndTerminalPartialUTF8(t *testing.T) {
	t.Parallel()

	if _, err := NewUTF8WindowBuffer(0); !errors.Is(err, ErrInvalidSeamWindow) {
		t.Fatalf("NewUTF8WindowBuffer(0) error = %v", err)
	}
	var nilBuffer *UTF8WindowBuffer
	if _, err := nilBuffer.Write([]byte("x")); !errors.Is(err, ErrInvalidSeamWindow) {
		t.Fatalf("nil Write() error = %v", err)
	}

	buffer, err := NewUTF8WindowBuffer(4)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer() error = %v", err)
	}
	if ready, writeErr := buffer.Write([]byte{0xff}); ready || !errors.Is(writeErr, ErrInvalidUTF8) {
		t.Fatalf("Write(invalid) = (%v, %v)", ready, writeErr)
	}
	if ready, writeErr := buffer.Write([]byte("good")); !ready || writeErr != nil {
		t.Fatalf("Write(after rejected bytes) = (%v, %v)", ready, writeErr)
	}
	if _, err := buffer.Write([]byte("later")); !errors.Is(err, ErrUTF8BufferComplete) {
		t.Fatalf("Write(after ready) error = %v", err)
	}

	readyWithPartial, err := NewUTF8WindowBuffer(1)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer(ready partial) error = %v", err)
	}
	if ready, writeErr := readyWithPartial.Write([]byte{'a', 0xe9}); writeErr != nil || !ready {
		t.Fatalf("Write(ready plus partial) = (%v, %v)", ready, writeErr)
	}
	if err := readyWithPartial.Finish(); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("Finish(ready plus partial) error = %v, want ErrInvalidUTF8", err)
	}

	partial, err := NewUTF8WindowBuffer(64)
	if err != nil {
		t.Fatalf("NewUTF8WindowBuffer(partial) error = %v", err)
	}
	if _, err := partial.Write([]byte{0xe9, 0x9b}); err != nil {
		t.Fatalf("Write(partial rune) error = %v", err)
	}
	if err := partial.Finish(); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("Finish(partial rune) error = %v, want ErrInvalidUTF8", err)
	}
}

func FuzzUTF8WindowBufferPreservesValidContent(f *testing.F) {
	f.Add("", uint8(1), uint8(0))
	f.Add("ascii content", uint8(4), uint8(3))
	f.Add("a雪z", uint8(2), uint8(2))
	f.Add("雪雪雪", uint8(5), uint8(1))
	f.Fuzz(func(t *testing.T, content string, rawWindow, rawSplit uint8) {
		if !utf8.ValidString(content) {
			return
		}
		window := 1 + int(rawWindow%64)
		data := []byte(content)
		split := 0
		if len(data) != 0 {
			split = int(rawSplit) % (len(data) + 1)
		}
		buffer, err := NewUTF8WindowBuffer(window)
		if err != nil {
			t.Fatalf("NewUTF8WindowBuffer() error = %v", err)
		}
		ready, err := buffer.Write(data[:split])
		if err != nil {
			t.Fatalf("Write(first) error = %v", err)
		}
		unwritten := data[split:]
		if !ready {
			ready, err = buffer.Write(unwritten)
			if err != nil {
				t.Fatalf("Write(second) error = %v", err)
			}
			unwritten = nil
		}
		if !ready {
			if err := buffer.Finish(); err != nil {
				t.Fatalf("Finish() error = %v", err)
			}
		}
		held, remainder, ok := buffer.Split()
		if !ok {
			t.Fatal("Split() not ready")
		}
		reconstructed := append(append(append([]byte(nil), held...), remainder...), unwritten...)
		if !bytes.Equal(reconstructed, data) {
			t.Fatalf("reconstructed = %x, want %x", reconstructed, data)
		}
		if len(held) > window || !utf8.Valid(held) {
			t.Fatalf("held = %x exceeds window %d or is invalid", held, window)
		}
		if len(remainder) != 0 && !utf8.RuneStart(remainder[0]) {
			t.Fatalf("remainder starts inside rune: %x", remainder)
		}
	})
}
