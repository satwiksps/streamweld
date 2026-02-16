package migrate

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"
)

// SeamResult is the safe leading-content rewrite for one continuation attempt.
type SeamResult struct {
	Content      []byte
	OverlapBytes int
	Anomaly      bool
}

// ReconcileSeam strips the longest UTF-8-aligned suffix/prefix overlap within
// window. Anomaly reports a possible restart: zero overlap, non-terminal
// accumulated text, and an uppercase leading continuation rune.
func ReconcileSeam(accumulated, continuation []byte, window int) (SeamResult, error) {
	if window <= 0 {
		return SeamResult{}, ErrInvalidSeamWindow
	}
	if !utf8.Valid(accumulated) || !utf8.Valid(continuation) {
		return SeamResult{}, ErrInvalidUTF8
	}
	heldLength := len(continuation)
	if heldLength > window {
		heldLength = utf8BoundaryAtOrBefore(continuation, window)
	}
	held := continuation[:heldLength]
	overlap := longestUTF8Overlap(accumulated, held)
	content := append([]byte(nil), continuation[overlap:]...)
	return SeamResult{
		Content:      content,
		OverlapBytes: overlap,
		Anomaly:      overlap == 0 && seamAnomaly(accumulated, continuation),
	}, nil
}

func longestUTF8Overlap(accumulated, held []byte) int {
	maximum := min(len(accumulated), len(held))
	for length := maximum; length > 0; length-- {
		if !utf8Boundary(accumulated, len(accumulated)-length) || !utf8Boundary(held, length) {
			continue
		}
		if bytes.Equal(accumulated[len(accumulated)-length:], held[:length]) {
			return length
		}
	}
	return 0
}

func utf8Boundary(value []byte, offset int) bool {
	return offset == 0 || offset == len(value) || utf8.RuneStart(value[offset])
}

func seamAnomaly(accumulated, continuation []byte) bool {
	left := strings.TrimSpace(string(accumulated))
	right := strings.TrimSpace(string(continuation))
	if left == "" || right == "" || accumulatedEndsTerminal(left) {
		return false
	}
	first, _ := utf8.DecodeRuneInString(right)
	return unicode.IsUpper(first)
}

func accumulatedEndsTerminal(value string) bool {
	for value != "" {
		last, size := utf8.DecodeLastRuneInString(value)
		if !isClosingQuoteOrBracket(last) {
			switch last {
			case '.', '!', '?', '…':
				return true
			default:
				return false
			}
		}
		value = strings.TrimSpace(value[:len(value)-size])
	}
	return false
}

func isClosingQuoteOrBracket(value rune) bool {
	switch value {
	case '"', '\'', ')', ']', '}', '»', '›', '’', '”', '〉', '》', '」', '』', '】', '）', '］', '｝':
		return true
	default:
		return false
	}
}
