package journal

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseStreamIDKnownULID(t *testing.T) {
	t.Parallel()
	const value = "01arz3ndektsv4rrffq69g5fav"
	id, err := ParseStreamID(value)
	if err != nil {
		t.Fatalf("ParseStreamID() error: %v", err)
	}
	if id.String() != value {
		t.Errorf("String() = %q, want %q", id, value)
	}
	timestamp, err := id.Timestamp()
	if err != nil {
		t.Fatalf("Timestamp() error: %v", err)
	}
	if got, want := timestamp.UnixMilli(), int64(1469922850259); got != want {
		t.Errorf("Timestamp() = %d, want %d", got, want)
	}
}

func TestGenerateKnownULIDVector(t *testing.T) {
	t.Parallel()
	entropy := []byte{0xd6, 0x76, 0x4c, 0x61, 0xef, 0xb9, 0x93, 0x02, 0xbd, 0x5b}
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(1469922850259) },
		bytes.NewReader(entropy),
	)
	id, err := generator.New()
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if got, want := id.String(), "01arz3ndektsv4rrffq69g5fav"; got != want {
		t.Errorf("New() = %q, want %q", got, want)
	}
}

func TestStreamIDValidationRejectsNoncanonicalValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"short", "01arz3ndektsv4rrffq69g5fa"},
		{"long", "01arz3ndektsv4rrffq69g5fav0"},
		{"uppercase", "01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{"ambiguous i", "01arz3ndektsv4rrffq69g5fai"},
		{"ambiguous l", "01arz3ndektsv4rrffq69g5fal"},
		{"ambiguous o", "01arz3ndektsv4rrffq69g5fao"},
		{"ambiguous u", "01arz3ndektsv4rrffq69g5fau"},
		{"punctuation", "01arz3ndektsv4rrffq69g5fa-"},
		{"non ASCII", "01arz3ndektsv4rrffq69g5faé"},
		{"overflow", "8zzzzzzzzzzzzzzzzzzzzzzzzz"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, err := ParseStreamID(test.value)
			if !errors.Is(err, ErrInvalidStreamID) {
				t.Fatalf("ParseStreamID(%q) = (%q, %v), want ErrInvalidStreamID", test.value, id, err)
			}
			if err := StreamID(test.value).Validate(); !errors.Is(err, ErrInvalidStreamID) {
				t.Fatalf("Validate(%q) error = %v, want ErrInvalidStreamID", test.value, err)
			}
			if _, err := StreamID(test.value).Timestamp(); !errors.Is(err, ErrInvalidStreamID) {
				t.Fatalf("Timestamp(%q) error = %v, want ErrInvalidStreamID", test.value, err)
			}
		})
	}
}

func TestGeneratorMonotonicWithinSameMillisecond(t *testing.T) {
	t.Parallel()
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(0) },
		bytes.NewReader(make([]byte, ulidEntropyLen)),
	)
	previous := ""
	for index := 0; index < 1_000; index++ {
		id, err := generator.New()
		if err != nil {
			t.Fatalf("New() %d error: %v", index, err)
		}
		if err := id.Validate(); err != nil {
			t.Fatalf("New() %d returned invalid ID %q: %v", index, id, err)
		}
		if previous != "" && id.String() <= previous {
			t.Fatalf("ID %d = %q is not greater than %q", index, id, previous)
		}
		previous = id.String()
	}
}

func TestGeneratorPreventsDeterministicEntropyCollision(t *testing.T) {
	t.Parallel()
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(0) },
		bytes.NewReader(make([]byte, ulidEntropyLen)),
	)
	first, err := generator.New()
	if err != nil {
		t.Fatal(err)
	}
	second, err := generator.New()
	if err != nil {
		t.Fatal(err)
	}
	if first != StreamID(strings.Repeat("0", StreamIDLength)) {
		t.Errorf("first zero-vector ID = %q", first)
	}
	if second == first || second.String() != strings.Repeat("0", StreamIDLength-1)+"1" {
		t.Errorf("second monotonic ID = %q after %q", second, first)
	}
}

func TestGeneratorClampsClockRollback(t *testing.T) {
	t.Parallel()
	clock := &sequenceTimeSource{milliseconds: []int64{100, 99, 101}}
	generator := NewIDGenerator(clock.Now, bytes.NewReader(make([]byte, 2*ulidEntropyLen)))
	var ids []StreamID
	for range 3 {
		id, err := generator.New()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if ids[0] >= ids[1] || ids[1] >= ids[2] {
		t.Fatalf("rollback IDs are not monotonic: %q", ids)
	}
	for index, want := range []int64{100, 100, 101} {
		got, err := ids[index].Timestamp()
		if err != nil {
			t.Fatal(err)
		}
		if got.UnixMilli() != want {
			t.Errorf("ID %d timestamp = %d, want %d", index, got.UnixMilli(), want)
		}
	}
}

func TestGeneratorConcurrentUniqueness(t *testing.T) {
	t.Parallel()
	const (
		workers   = 32
		perWorker = 256
	)
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(1234) },
		bytes.NewReader(make([]byte, ulidEntropyLen)),
	)
	results := make(chan StreamID, workers*perWorker)
	errorsFound := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range perWorker {
				id, err := generator.New()
				if err != nil {
					errorsFound <- err
					return
				}
				results <- id
			}
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent New() error: %v", err)
	}

	ids := make([]string, 0, workers*perWorker)
	seen := make(map[StreamID]struct{}, workers*perWorker)
	for id := range results {
		if err := id.Validate(); err != nil {
			t.Fatalf("invalid concurrent ID %q: %v", id, err)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate concurrent ID %q", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id.String())
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("generated %d unique IDs, want %d", len(seen), workers*perWorker)
	}
	sort.Strings(ids)
	for index := 1; index < len(ids); index++ {
		if ids[index] <= ids[index-1] {
			t.Fatalf("sorted IDs not strictly increasing at %d", index)
		}
	}
}

func TestGeneratorEntropyAndRangeErrors(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("entropy unavailable")
	generator := NewIDGenerator(func() time.Time { return time.UnixMilli(1) }, failingEntropyReader{err: sentinel})
	if _, err := generator.New(); !errors.Is(err, sentinel) {
		t.Errorf("entropy error = %v, want sentinel", err)
	}

	for _, milliseconds := range []int64{-1, int64(maxULIDTime + 1)} {
		generator := NewIDGenerator(func() time.Time { return time.UnixMilli(milliseconds) }, bytes.NewReader(make([]byte, ulidEntropyLen)))
		if _, err := generator.New(); !errors.Is(err, ErrInvalidStreamID) {
			t.Errorf("timestamp %d error = %v, want ErrInvalidStreamID", milliseconds, err)
		}
	}
}

func TestGeneratorReportsMonotonicEntropyExhaustion(t *testing.T) {
	t.Parallel()
	generator := NewIDGenerator(
		func() time.Time { return time.UnixMilli(1) },
		bytes.NewReader(bytes.Repeat([]byte{0xff}, ulidEntropyLen)),
	)
	if _, err := generator.New(); err != nil {
		t.Fatalf("first New() error: %v", err)
	}
	if _, err := generator.New(); !errors.Is(err, ErrStreamIDEntropyExhausted) {
		t.Fatalf("second New() error = %v, want ErrStreamIDEntropyExhausted", err)
	}
}

func TestNewStreamIDUsesValidLowercaseCanonicalForm(t *testing.T) {
	t.Parallel()
	ids := make(map[StreamID]struct{})
	for range 128 {
		id, err := NewStreamID()
		if err != nil {
			t.Fatal(err)
		}
		if err := id.Validate(); err != nil {
			t.Fatalf("NewStreamID() = %q: %v", id, err)
		}
		if id.String() != strings.ToLower(id.String()) {
			t.Errorf("NewStreamID() was not lowercase: %q", id)
		}
		if _, duplicate := ids[id]; duplicate {
			t.Fatalf("NewStreamID() collision: %q", id)
		}
		ids[id] = struct{}{}
	}
}

type sequenceTimeSource struct {
	mu           sync.Mutex
	milliseconds []int64
	index        int
}

func (s *sequenceTimeSource) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := min(s.index, len(s.milliseconds)-1)
	s.index++
	return time.UnixMilli(s.milliseconds[index])
}

type failingEntropyReader struct {
	err error
}

func (r failingEntropyReader) Read([]byte) (int, error) {
	return 0, r.err
}
