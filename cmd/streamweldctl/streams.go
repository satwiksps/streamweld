package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

const (
	defaultStreamsTimeout = 10 * time.Second
	maxStreamsResponse    = 1 << 20
)

type streamsErrorEnvelope struct {
	Error struct {
		Type     string `json:"type"`
		Code     string `json:"code"`
		Message  string `json:"message"`
		StreamID string `json:"stream_id"`
	} `json:"error"`
}

func runStreams(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("streamweldctl streams", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Streamweld proxy base URL (typically reached with kubectl port-forward)")
	timeout := flags.Duration("timeout", defaultStreamsTimeout, "deadline for the state request")
	jsonOutput := flags.Bool("json", false, "emit the complete stream state as JSON")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: streamweldctl streams [--endpoint URL] [--json] STREAM_ID")
		_, _ = fmt.Fprintln(stderr)
		_, _ = fmt.Fprintln(stderr, "Inspect the retained state, usage, and migration history for one durable stream.")
		_, _ = fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl streams: exactly one stream ID is required")
		flags.Usage()
		return 2
	}
	if *timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl streams: --timeout must be positive")
		return 2
	}
	baseURL, err := parseHTTPEndpoint(*endpoint)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: %v\n", err)
		return 2
	}
	id, err := journal.ParseStreamID(flags.Arg(0))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "streamweldctl streams: STREAM_ID must be a canonical lowercase ULID")
		return 2
	}

	target := strings.TrimRight(baseURL.String(), "/") + "/v1/streams/" + url.PathEscape(id.String())
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "streamweldctl streams: construct request failed")
		return 1
	}
	request.Header.Set("Accept", "application/json")
	client := &http.Client{
		Timeout:       *timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: request failed: %v\n", err)
		return 1
	}
	defer func() { _ = response.Body.Close() }()
	body, err := readStreamsResponse(response.Body)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: invalid proxy response: %v\n", err)
		return 1
	}
	if response.StatusCode != http.StatusOK {
		writeStreamsHTTPError(stderr, response.Status, id, body)
		return 1
	}
	state, err := decodeStreamState(body)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: invalid proxy response: %v\n", err)
		return 1
	}
	if err := validateStreamState(state, id); err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: inconsistent proxy response: %v\n", err)
		return 1
	}
	if err := writeStreamState(stdout, state, *jsonOutput); err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl streams: write state: %v\n", err)
		return 1
	}
	return 0
}

func readStreamsResponse(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxStreamsResponse+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxStreamsResponse {
		return nil, errors.New("response exceeds the size limit")
	}
	return body, nil
}

func decodeStreamState(body []byte) (journal.StreamState, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var state journal.StreamState
	if err := decoder.Decode(&state); err != nil {
		return journal.StreamState{}, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return journal.StreamState{}, err
	}
	return state, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("response contains multiple JSON values")
		}
		return err
	}
	return nil
}

func validateStreamState(state journal.StreamState, requested journal.StreamID) error {
	if state.StreamID != requested {
		return fmt.Errorf("stream_id is %q, want %q", state.StreamID, requested)
	}
	if state.EarliestSeq == 0 || state.LastSeq < state.EarliestSeq {
		return fmt.Errorf("invalid sequence bounds %d..%d", state.EarliestSeq, state.LastSeq)
	}
	if state.CreatedAt.IsZero() || state.UpdatedAt.IsZero() || state.UpdatedAt.Before(state.CreatedAt) {
		return errors.New("invalid creation or update timestamp")
	}
	if state.Usage.PromptTokens > ^uint64(0)-state.Usage.CompletionTokens ||
		state.Usage.TotalTokens != state.Usage.PromptTokens+state.Usage.CompletionTokens {
		return errors.New("usage total does not equal prompt plus completion tokens")
	}

	var terminalKind journal.EntryKind
	switch state.Status {
	case journal.StatusOpen:
		if state.Terminal != nil {
			return errors.New("open stream has terminal state")
		}
		return nil
	case journal.StatusDone:
		terminalKind = journal.KindDone
	case journal.StatusError:
		terminalKind = journal.KindError
	case journal.StatusStopped:
		terminalKind = journal.KindStopped
	default:
		return fmt.Errorf("unknown status %q", state.Status)
	}
	if state.Terminal == nil {
		return fmt.Errorf("%s stream has no terminal state", state.Status)
	}
	if state.Terminal.Kind != terminalKind || state.Terminal.Seq != state.LastSeq || state.Terminal.TS.IsZero() {
		return fmt.Errorf("terminal state does not match %s at sequence %d", state.Status, state.LastSeq)
	}
	return nil
}

func writeStreamsHTTPError(writer io.Writer, status string, requested journal.StreamID, body []byte) {
	var envelope streamsErrorEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err == nil && envelope.Error.Type == "streamweld_error" &&
		envelope.Error.Code != "" && (envelope.Error.StreamID == "" || envelope.Error.StreamID == requested.String()) {
		message := strings.Join(strings.Fields(envelope.Error.Message), " ")
		if message == "" {
			_, _ = fmt.Fprintf(writer, "streamweldctl streams: proxy returned %s (%s)\n", status, envelope.Error.Code)
			return
		}
		_, _ = fmt.Fprintf(writer, "streamweldctl streams: proxy returned %s (%s): %s\n", status, envelope.Error.Code, message)
		return
	}
	_, _ = fmt.Fprintf(writer, "streamweldctl streams: proxy returned %s\n", status)
}

func writeStreamState(writer io.Writer, state journal.StreamState, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(state)
	}
	modelVersion := "-"
	if state.ModelVersion != nil && *state.ModelVersion != "" {
		modelVersion = *state.ModelVersion
	}
	if _, err := fmt.Fprintf(
		writer,
		"Stream: %s\nStatus: %s\nResumable: %t\nModel: %s\nModel version: %s\nBackend: %s -> %s\nCreated: %s\nUpdated: %s\nSequence: %d..%d\nUsage: prompt=%d completion=%d total=%d estimated=%t\nMigrations: %d\n",
		state.StreamID,
		state.Status,
		state.Resumable,
		state.Model,
		modelVersion,
		state.OriginBackend,
		state.CurrentBackend,
		state.CreatedAt.UTC().Format(time.RFC3339Nano),
		state.UpdatedAt.UTC().Format(time.RFC3339Nano),
		state.EarliestSeq,
		state.LastSeq,
		state.Usage.PromptTokens,
		state.Usage.CompletionTokens,
		state.Usage.TotalTokens,
		state.Usage.Estimated,
		len(state.Migrations),
	); err != nil {
		return err
	}
	for _, migration := range state.Migrations {
		if _, err := fmt.Fprintf(
			writer,
			"  seq=%d attempt=%d %s -> %s reason=%s rescued_tokens=%d estimated=%t\n",
			migration.Seq,
			migration.Attempt,
			migration.FromBackend,
			migration.ToBackend,
			migration.Reason,
			migration.RescuedTokens,
			migration.TokenCountEstimated,
		); err != nil {
			return err
		}
	}
	if state.Terminal != nil {
		_, err := fmt.Fprintf(
			writer,
			"Terminal: %s at seq=%d (%s)\n",
			state.Terminal.Kind,
			state.Terminal.Seq,
			state.Terminal.TS.UTC().Format(time.RFC3339Nano),
		)
		return err
	}
	return nil
}
