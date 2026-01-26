package journal

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	// StreamIDLength is the canonical encoded ULID length.
	StreamIDLength = 26
	ulidByteLength = 16
	ulidTimeBytes  = 6
	ulidEntropyLen = 10
	maxULIDTime    = uint64(1<<48 - 1)
)

const crockfordLower = "0123456789abcdefghjkmnpqrstvwxyz"

var (
	// ErrInvalidStreamID indicates that a value is not a canonical lowercase
	// ULID suitable for use as a Streamweld stream identifier.
	ErrInvalidStreamID = errors.New("journal: invalid stream ID")

	// ErrStreamIDEntropyExhausted indicates that all 80-bit monotonic entropy
	// values were consumed before the generator's clock advanced.
	ErrStreamIDEntropyExhausted = errors.New("journal: stream ID entropy exhausted")
)

// StreamID is a canonical lowercase ULID identifying one durable stream.
type StreamID string

// String returns the canonical encoded identifier.
func (id StreamID) String() string {
	return string(id)
}

// Validate verifies that id is a canonical lowercase 26-character ULID.
func (id StreamID) Validate() error {
	_, err := decodeStreamID(string(id))
	return err
}

// Timestamp returns the millisecond-resolution creation time encoded in id.
func (id StreamID) Timestamp() (time.Time, error) {
	raw, err := decodeStreamID(string(id))
	if err != nil {
		return time.Time{}, err
	}
	var milliseconds uint64
	for index := 0; index < ulidTimeBytes; index++ {
		milliseconds = milliseconds<<8 | uint64(raw[index])
	}
	return time.UnixMilli(int64(milliseconds)).UTC(), nil
}

// ParseStreamID validates value and returns its canonical StreamID form.
func ParseStreamID(value string) (StreamID, error) {
	id := StreamID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

// IDGenerator generates cryptographically random, concurrency-safe monotonic
// StreamIDs. When the clock repeats or moves backwards, the most recent
// timestamp is retained and its 80-bit entropy is incremented.
type IDGenerator struct {
	mu          sync.Mutex
	now         func() time.Time
	entropy     io.Reader
	initialized bool
	lastMillis  uint64
	lastEntropy [ulidEntropyLen]byte
}

// NewIDGenerator returns a generator using the supplied clock and entropy
// source. A nil clock uses time.Now; a nil entropy source uses crypto/rand.
func NewIDGenerator(now func() time.Time, entropy io.Reader) *IDGenerator {
	if now == nil {
		now = time.Now
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	return &IDGenerator{now: now, entropy: entropy}
}

var defaultIDGenerator = NewIDGenerator(time.Now, rand.Reader)

// NewStreamID returns a new process-wide monotonic StreamID.
func NewStreamID() (StreamID, error) {
	return defaultIDGenerator.New()
}

// New returns the next StreamID from the generator.
func (g *IDGenerator) New() (StreamID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.now == nil {
		g.now = time.Now
	}
	if g.entropy == nil {
		g.entropy = rand.Reader
	}

	nowMillis := g.now().UnixMilli()
	if nowMillis < 0 || uint64(nowMillis) > maxULIDTime {
		return "", fmt.Errorf("%w: timestamp is outside the 48-bit ULID range", ErrInvalidStreamID)
	}
	milliseconds := uint64(nowMillis)

	var entropy [ulidEntropyLen]byte
	if !g.initialized || milliseconds > g.lastMillis {
		if _, err := io.ReadFull(g.entropy, entropy[:]); err != nil {
			return "", fmt.Errorf("read stream ID entropy: %w", err)
		}
	} else {
		milliseconds = g.lastMillis
		entropy = g.lastEntropy
		if !incrementEntropy(&entropy) {
			return "", ErrStreamIDEntropyExhausted
		}
	}

	g.initialized = true
	g.lastMillis = milliseconds
	g.lastEntropy = entropy

	var raw [ulidByteLength]byte
	for index := ulidTimeBytes - 1; index >= 0; index-- {
		raw[index] = byte(milliseconds)
		milliseconds >>= 8
	}
	copy(raw[ulidTimeBytes:], entropy[:])
	return StreamID(encodeULID(raw)), nil
}

func incrementEntropy(entropy *[ulidEntropyLen]byte) bool {
	for index := len(entropy) - 1; index >= 0; index-- {
		entropy[index]++
		if entropy[index] != 0 {
			return true
		}
	}
	return false
}

func encodeULID(raw [ulidByteLength]byte) string {
	var encoded [StreamIDLength]byte
	for charIndex := range encoded {
		var value byte
		for bitOffset := 0; bitOffset < 5; bitOffset++ {
			value <<= 1
			inputBit := charIndex*5 + bitOffset - 2 // ULID has two leading zero padding bits.
			if inputBit < 0 {
				continue
			}
			value |= raw[inputBit/8] >> (7 - inputBit%8) & 1
		}
		encoded[charIndex] = crockfordLower[value]
	}
	return string(encoded[:])
}

func decodeStreamID(value string) ([ulidByteLength]byte, error) {
	var raw [ulidByteLength]byte
	if len(value) != StreamIDLength {
		return raw, fmt.Errorf("%w: length must be %d", ErrInvalidStreamID, StreamIDLength)
	}

	for charIndex := 0; charIndex < len(value); charIndex++ {
		decoded := strings.IndexByte(crockfordLower, value[charIndex])
		if decoded < 0 {
			return raw, fmt.Errorf("%w: character at position %d is not lowercase Crockford Base32", ErrInvalidStreamID, charIndex)
		}
		if charIndex == 0 && decoded > 7 {
			return raw, fmt.Errorf("%w: first character exceeds the 128-bit ULID range", ErrInvalidStreamID)
		}

		for bitOffset := 0; bitOffset < 5; bitOffset++ {
			encodedBit := charIndex*5 + bitOffset
			if encodedBit < 2 {
				continue
			}
			if decoded&(1<<(4-bitOffset)) == 0 {
				continue
			}
			outputBit := encodedBit - 2
			raw[outputBit/8] |= 1 << (7 - outputBit%8)
		}
	}
	return raw, nil
}
