package journal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestMemoryDegradationSealsSequenceAndTerminatesPrefixReplay(t *testing.T) {
	config := DefaultConfig()
	config.MaxTotalBytes = 1 << 20
	config.ReaderMaxLagBytes = 1 << 20
	backend, err := NewMemory(config)
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	id, err := ParseStreamID("01arz3ndektsv4rrffq69g5fav")
	if err != nil {
		t.Fatalf("ParseStreamID() error = %v", err)
	}
	ctx := context.Background()
	if err := backend.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := backend.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"data":"hello"}`)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := backend.MarkDegraded(ctx, id); err != nil {
		t.Fatalf("MarkDegraded() error = %v", err)
	}
	if err := backend.MarkDegraded(ctx, id); err != nil {
		t.Fatalf("idempotent MarkDegraded() error = %v", err)
	}
	if _, err := backend.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"data":"gap"}`)}); !errors.Is(err, ErrDegraded) {
		t.Fatalf("Append(after gap) error = %v, want ErrDegraded", err)
	}
	if err := backend.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrDegraded) {
		t.Fatalf("Close(after gap) error = %v, want ErrDegraded", err)
	}

	sequence, err := backend.Read(ctx, id, 0)
	if err != nil {
		t.Fatalf("Read(prefix) error = %v", err)
	}
	var sequences []uint64
	var terminalErr error
	sequence(func(entry Entry, err error) bool {
		if err != nil {
			terminalErr = err
			return false
		}
		sequences = append(sequences, entry.Seq)
		return true
	})
	if len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("Read(prefix) sequences = %v, want [1 2]", sequences)
	}
	if !errors.Is(terminalErr, ErrOffsetExpired) {
		t.Fatalf("Read(prefix) terminal error = %v, want ErrOffsetExpired", terminalErr)
	}
	if _, err := backend.Read(ctx, id, 2); !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("Read(at gap) error = %v, want ErrOffsetExpired", err)
	}

	tail, cancel, err := backend.Tail(ctx, id, 0)
	if err != nil {
		t.Fatalf("Tail(prefix) error = %v", err)
	}
	defer cancel()
	var tailSequences []uint64
	for entry := range tail {
		if entry.Err != nil {
			terminalErr = entry.Err
			break
		}
		tailSequences = append(tailSequences, entry.Seq)
	}
	if len(tailSequences) != 2 || tailSequences[0] != 1 || tailSequences[1] != 2 {
		t.Fatalf("Tail(prefix) sequences = %v, want [1 2]", tailSequences)
	}
	if !errors.Is(terminalErr, ErrOffsetExpired) {
		t.Fatalf("Tail(prefix) terminal error = %v, want ErrOffsetExpired", terminalErr)
	}
	if _, _, err := backend.Tail(ctx, id, 2); !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("Tail(at gap) error = %v, want ErrOffsetExpired", err)
	}
}
