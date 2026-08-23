package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/migrate"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

// RemoteConfig runs the deterministic matrix against a provisioned proxy.
type RemoteConfig struct {
	ProfileName          string
	Execution            string
	ProxyURL             string
	DirectURL            string
	ConcurrentStreams    int
	OutputTokens         int
	SlowConsumerDelay    time.Duration
	ClientReconnectDelay time.Duration
	ScenarioTimeout      time.Duration
	Client               *http.Client
	Injector             Injector
	Now                  func() time.Time
}

// RunRemote executes all scenarios against a kind-style externally managed
// environment. The injector is mandatory, so an enabled cluster profile cannot
// silently fall back to the local simulation.
func RunRemote(ctx context.Context, config RemoteConfig) (Report, error) {
	if ctx == nil {
		return Report{}, errors.New("remote chaos context is nil")
	}
	if config.ProfileName == "" {
		config.ProfileName = "deterministic-kind"
	}
	if config.Execution == "" {
		config.Execution = "kind cluster with kubectl failure injection"
	}
	if config.ConcurrentStreams == 0 {
		config.ConcurrentStreams = DefaultConcurrentStreams
	}
	if config.OutputTokens == 0 {
		config.OutputTokens = DefaultOutputTokens
	}
	if config.SlowConsumerDelay == 0 {
		config.SlowConsumerDelay = time.Second
	}
	if config.ClientReconnectDelay == 0 {
		config.ClientReconnectDelay = 100 * time.Millisecond
	}
	if config.ScenarioTimeout == 0 {
		config.ScenarioTimeout = 3 * time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Injector == nil {
		return Report{}, errors.New("remote chaos injector is required")
	}
	if config.ConcurrentStreams < 1 || config.ConcurrentStreams > 1024 ||
		config.OutputTokens < 8 || config.OutputTokens > 100_000 ||
		config.SlowConsumerDelay <= 0 || config.ClientReconnectDelay <= 0 || config.ScenarioTimeout <= 0 {
		return Report{}, errors.New("remote chaos stream, token, and timeout settings are outside their safe bounds")
	}
	proxyURL, err := validateRemoteBaseURL(config.ProxyURL)
	if err != nil {
		return Report{}, fmt.Errorf("proxy URL: %w", err)
	}
	directURL, err := validateRemoteBaseURL(config.DirectURL)
	if err != nil {
		return Report{}, fmt.Errorf("direct URL: %w", err)
	}
	if config.Client == nil {
		transport := &http.Transport{
			DisableCompression:  true,
			MaxIdleConns:        config.ConcurrentStreams * 4,
			MaxIdleConnsPerHost: config.ConcurrentStreams * 2,
		}
		config.Client = &http.Client{Transport: transport}
	}

	measurement, err := measureRemoteTTFT(ctx, config.Client, directURL, proxyURL, config.ConcurrentStreams)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   config.Now().UTC(),
		Profile: NewProfile(
			config.ProfileName,
			config.Execution,
			"deterministic OpenAI-compatible backend in Kubernetes",
			true,
			config.ConcurrentStreams,
			config.OutputTokens,
		),
		Results: make([]Result, 0, len(definitions)),
	}
	// The Kubernetes fake backend flushes its first token immediately; its
	// configured delay is inter-token only. Network/process time keeps TTFT
	// measurable, but no artificial first-token delay is claimed.
	report.Profile.TTFTBackendDelayMilliseconds = 0
	for _, definition := range definitions {
		scenarioContext, cancel := context.WithTimeout(ctx, config.ScenarioTimeout)
		prepareErr := config.Injector.Prepare(scenarioContext, definition.Scenario)
		if prepareErr != nil {
			restoreContext, cancelRestore := context.WithTimeout(context.WithoutCancel(ctx), config.ScenarioTimeout)
			restoreErr := config.Injector.Restore(restoreContext, definition.Scenario)
			cancelRestore()
			cancel()
			return Report{}, fmt.Errorf("prepare %s: %w", definition.Scenario, errors.Join(prepareErr, restoreErr))
		}
		result, runErr := runRemoteScenario(scenarioContext, config, proxyURL, definition)
		restoreContext, cancelRestore := context.WithTimeout(context.WithoutCancel(ctx), config.ScenarioTimeout)
		restoreErr := config.Injector.Restore(restoreContext, definition.Scenario)
		var recoveryErr error
		if runErr == nil && restoreErr == nil && definition.Scenario == ScenarioRedisDown {
			recoveryErr = waitRemoteDurability(
				restoreContext, config.Client, proxyURL, config.ClientReconnectDelay,
			)
		}
		cancelRestore()
		cancel()
		if err := errors.Join(runErr, restoreErr, recoveryErr); err != nil {
			return Report{}, fmt.Errorf("run %s: %w", definition.Scenario, err)
		}
		result.DirectTTFPMilliseconds = roundMilliseconds(measurement.DirectMilliseconds)
		result.StreamweldTTFPMilliseconds = roundMilliseconds(measurement.StreamweldMilliseconds)
		result.AddedTTFTMilliseconds = roundMilliseconds(
			result.StreamweldTTFPMilliseconds - result.DirectTTFPMilliseconds,
		)
		report.Results = append(report.Results, result)
	}
	if err := report.Validate(); err != nil {
		return Report{}, fmt.Errorf("remote correctness gate: %w", err)
	}
	return report, nil
}

func waitRemoteDurability(
	ctx context.Context,
	client *http.Client,
	proxyURL string,
	retryDelay time.Duration,
) error {
	var lastErr error
	for attempt := 1; ; attempt++ {
		stream, err := attachRemoteStream(
			ctx, client, proxyURL, ScenarioClientDrop, -attempt, 8,
		)
		if err == nil {
			err = finishRemoteStream(
				ctx, client, proxyURL, ScenarioClientDrop, retryDelay, stream,
			)
		}
		if err == nil {
			canonical := deterministicOutput(8)
			if stream.terminal == "done" && !stream.degraded && stream.output.String() == canonical {
				return nil
			}
			err = fmt.Errorf(
				"durability canary ended as %q (degraded=%t, output_correct=%t)",
				stream.terminal, stream.degraded, stream.output.String() == canonical,
			)
		}
		lastErr = err
		if waitErr := waitRemoteReconnect(ctx, retryDelay); waitErr != nil {
			return fmt.Errorf(
				"wait for end-to-end durable proxy recovery: %w",
				errors.Join(waitErr, lastErr),
			)
		}
	}
}

func measureRemoteTTFT(
	ctx context.Context,
	client *http.Client,
	directURL, proxyURL string,
	concurrent int,
) (TTFTMeasurement, error) {
	if _, err := measureOneTTFT(ctx, client, directURL, -1); err != nil {
		return TTFTMeasurement{}, fmt.Errorf("warm remote direct path: %w", err)
	}
	if _, err := measureOneTTFT(ctx, client, proxyURL, -1); err != nil {
		return TTFTMeasurement{}, fmt.Errorf("warm remote streamweld path: %w", err)
	}
	direct, err := measureTTFTBatch(ctx, client, directURL, concurrent)
	if err != nil {
		return TTFTMeasurement{}, fmt.Errorf("measure remote direct TTFT: %w", err)
	}
	durable, err := measureTTFTBatch(ctx, client, proxyURL, concurrent)
	if err != nil {
		return TTFTMeasurement{}, fmt.Errorf("measure remote Streamweld TTFT: %w", err)
	}
	return TTFTMeasurement{
		DirectMilliseconds:     durationP50(direct).Seconds() * 1000,
		StreamweldMilliseconds: durationP50(durable).Seconds() * 1000,
	}, nil
}

type attachedStream struct {
	id           string
	lastSeq      uint64
	output       strings.Builder
	response     *http.Response
	decoder      *sse.Decoder
	terminal     string
	migrated     int
	rescued      int
	degraded     bool
	readerLagged bool

	awaitingSeam   bool
	seamOverlaps   []int
	promptRebilled int
}

func runRemoteScenario(
	ctx context.Context,
	config RemoteConfig,
	proxyURL string,
	definition Definition,
) (Result, error) {
	scenarioTokens := config.OutputTokens
	if definition.Scenario == ScenarioSlowConsumer {
		// Stay below the 4 MiB per-stream journal cap while producing enough
		// data to fill the deliberately constrained client receive window.
		scenarioTokens = max(scenarioTokens, 8192)
	}
	attached := make(chan struct {
		stream *attachedStream
		err    error
	}, config.ConcurrentStreams)
	for index := range config.ConcurrentStreams {
		go func() {
			stream, err := attachRemoteStream(ctx, config.Client, proxyURL, definition.Scenario, index, scenarioTokens)
			attached <- struct {
				stream *attachedStream
				err    error
			}{stream: stream, err: err}
		}()
	}
	streams := make([]*attachedStream, 0, config.ConcurrentStreams)
	for range config.ConcurrentStreams {
		select {
		case item := <-attached:
			if item.err != nil {
				closeRemoteStreams(streams)
				return Result{}, item.err
			}
			streams = append(streams, item.stream)
		case <-ctx.Done():
			closeRemoteStreams(streams)
			return Result{}, ctx.Err()
		}
	}
	if err := config.Injector.Inject(ctx, definition.Scenario); err != nil {
		closeRemoteStreams(streams)
		return Result{}, err
	}
	if definition.Scenario == ScenarioSlowConsumer {
		timer := time.NewTimer(config.SlowConsumerDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			closeRemoteStreams(streams)
			return Result{}, ctx.Err()
		}
	}

	results := make(chan struct {
		stream *attachedStream
		err    error
	}, len(streams))
	var workers sync.WaitGroup
	workers.Add(len(streams))
	for _, stream := range streams {
		go func() {
			defer workers.Done()
			err := finishRemoteStream(
				ctx,
				config.Client,
				proxyURL,
				definition.Scenario,
				config.ClientReconnectDelay,
				stream,
			)
			results <- struct {
				stream *attachedStream
				err    error
			}{stream: stream, err: err}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	result := Result{
		Scenario:              definition.Scenario,
		Injection:             definition.Injection,
		ExpectedOutcome:       definition.ExpectedOutcome,
		OutputTokensPerStream: scenarioTokens,
		StreamsStarted:        config.ConcurrentStreams,
	}
	canonical := deterministicOutput(scenarioTokens)
	seamOverlaps := make([]int, 0, config.ConcurrentStreams)
	for item := range results {
		if item.err != nil {
			return Result{}, item.err
		}
		stream := item.stream
		if definition.ExpectsMigration && stream.migrated != 1 {
			return Result{}, fmt.Errorf("stream %s observed %d migrations, want exactly one", stream.id, stream.migrated)
		}
		if !definition.ExpectsMigration && stream.migrated != 0 {
			return Result{}, fmt.Errorf("stream %s unexpectedly observed %d migrations", stream.id, stream.migrated)
		}
		if len(stream.seamOverlaps) != stream.migrated {
			return Result{}, fmt.Errorf(
				"stream %s observed %d/%d migration seam samples",
				stream.id,
				len(stream.seamOverlaps),
				stream.migrated,
			)
		}
		result.StreamsMigrated += stream.migrated
		result.TokensRescued += stream.rescued
		result.PromptTokensRebilled += stream.promptRebilled
		seamOverlaps = append(seamOverlaps, stream.seamOverlaps...)
		output := stream.output.String()
		correct := remoteOutputCorrect(definition, stream.terminal, output, canonical)
		switch definition.Scenario {
		case ScenarioExplicitStop:
			result.StreamsStopped++
		case ScenarioUnsafe:
			result.MigrationsRefused++
		default:
			result.StreamsCompleted++
		}
		if definition.Scenario == ScenarioSlowConsumer && !stream.readerLagged {
			return Result{}, fmt.Errorf("stream %s never received the injected reader-lag eviction", stream.id)
		}
		if correct {
			result.CorrectStreams++
		}
	}
	result.SeamOverlapBytesP50 = nearestRank(seamOverlaps, 0.50)
	result.SeamOverlapBytesP99 = nearestRank(seamOverlaps, 0.99)
	result.OutputCorrect = result.CorrectStreams == result.StreamsStarted
	return result, nil
}

func remoteOutputCorrect(definition Definition, terminal, output, canonical string) bool {
	switch definition.Scenario {
	case ScenarioExplicitStop, ScenarioUnsafe:
		return terminal == definition.ExpectedOutcome && output != "" && strings.HasPrefix(canonical, output)
	default:
		return terminal == definition.ExpectedOutcome && output == canonical
	}
}

func attachRemoteStream(
	ctx context.Context,
	client *http.Client,
	proxyURL string,
	scenario Scenario,
	index, tokens int,
) (*attachedStream, error) {
	body, err := json.Marshal(map[string]any{
		"model": "streamweld/deterministic-chaos",
		"messages": []map[string]string{{
			"role": "user", "content": fmt.Sprintf("streamweld-chaos:%s:%d", scenario, index),
		}},
		"stream": true, "temperature": 0, "max_tokens": tokens,
	})
	if err != nil {
		return nil, err
	}
	requestContext := ctx
	var receiveBufferErr error
	receiveBufferSet := false
	var receiveBufferMu sync.Mutex
	if scenario == ScenarioSlowConsumer {
		requestContext = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				receiveBufferMu.Lock()
				defer receiveBufferMu.Unlock()
				receiveBufferSet = true
				connection, ok := info.Conn.(*net.TCPConn)
				if !ok {
					receiveBufferErr = fmt.Errorf("slow-consumer connection %T is not TCP", info.Conn)
					return
				}
				receiveBufferErr = connection.SetReadBuffer(1024)
			},
		})
	}
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodPost,
		proxyURL+"/v1/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-Streamweld-Verbose", "1")
	request.Header.Set("X-Streamweld-Idempotency-Key", fmt.Sprintf("chaos-%s-%d-%d", scenario, index, time.Now().UnixNano()))
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	receiveBufferMu.Lock()
	bufferErr := receiveBufferErr
	bufferSet := receiveBufferSet
	receiveBufferMu.Unlock()
	if scenario == ScenarioSlowConsumer && (!bufferSet || bufferErr != nil) {
		_ = response.Body.Close()
		if !bufferSet {
			return nil, errors.New("slow-consumer connection did not expose a receive-window hook")
		}
		return nil, fmt.Errorf("bound slow-consumer receive window: %w", bufferErr)
	}
	if response.StatusCode != http.StatusOK {
		message, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		drainErr := drainAndClose(response.Body)
		if err := errors.Join(readErr, drainErr); err != nil {
			return nil, fmt.Errorf(
				"attach stream %d: status %s: %s: drain response: %w",
				index, response.Status, bytes.TrimSpace(message), err,
			)
		}
		return nil, fmt.Errorf("attach stream %d: status %s: %s", index, response.Status, bytes.TrimSpace(message))
	}
	if response.Header.Get("X-Streamweld-Durability") != "durable" {
		if err := drainAndClose(response.Body); err != nil {
			return nil, fmt.Errorf("attach stream %d: response is not durable: drain response: %w", index, err)
		}
		return nil, fmt.Errorf("attach stream %d: response is not durable", index)
	}
	stream := &attachedStream{
		id: response.Header.Get("X-Streamweld-Stream-Id"), response: response, decoder: sse.NewDecoder(response.Body),
	}
	if stream.id == "" {
		_ = response.Body.Close()
		return nil, fmt.Errorf("attach stream %d: missing stream ID", index)
	}
	for stream.output.Len() == 0 {
		event, decodeErr := stream.decoder.Decode()
		if decodeErr != nil {
			_ = response.Body.Close()
			return nil, fmt.Errorf("attach stream %d: %w", index, decodeErr)
		}
		if err := stream.accept(event); err != nil {
			_ = response.Body.Close()
			return nil, fmt.Errorf("attach stream %d: %w", index, err)
		}
	}
	return stream, nil
}

func finishRemoteStream(
	ctx context.Context,
	client *http.Client,
	proxyURL string,
	scenario Scenario,
	clientReconnectDelay time.Duration,
	stream *attachedStream,
) error {
	defer func() {
		if stream.response != nil {
			_ = stream.response.Body.Close()
		}
	}()
	if stream.readerLagged && scenario != ScenarioSlowConsumer {
		return fmt.Errorf("stream %s unexpectedly received reader-lag eviction during %s", stream.id, scenario)
	}
	if scenario == ScenarioClientDrop {
		_ = stream.response.Body.Close()
		stream.response = nil
		if err := waitRemoteReconnect(ctx, clientReconnectDelay); err != nil {
			return err
		}
		if err := stream.resume(ctx, client, proxyURL); err != nil {
			return err
		}
	}
	if scenario == ScenarioExplicitStop {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL+"/v1/streams/"+url.PathEscape(stream.id)+"/stop", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		closeErr := response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			return fmt.Errorf("stop stream %s: status %s", stream.id, response.Status)
		}
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
	}

	resumeAttempts := 0
	connectionSawFrame := false
	for stream.terminal == "" {
		event, err := stream.decoder.Decode()
		if err == nil {
			connectionSawFrame = true
			if acceptErr := stream.accept(event); acceptErr != nil {
				return acceptErr
			}
			if stream.readerLagged && scenario != ScenarioSlowConsumer {
				return fmt.Errorf("stream %s unexpectedly received reader-lag eviction during %s", stream.id, scenario)
			}
			continue
		}
		if connectionSawFrame {
			// Match the SDK's consecutive-failure accounting: a connection that
			// advances the event stream restores the full reconnect budget.
			resumeAttempts = 0
		}
		if resumeAttempts >= 3 {
			return fmt.Errorf("stream %s ended without terminal after %d resume attempts: %w", stream.id, resumeAttempts, err)
		}
		if stream.response != nil {
			_ = stream.response.Body.Close()
			stream.response = nil
		}
		if err := waitRemoteReconnect(ctx, clientReconnectDelay); err != nil {
			return err
		}
		resumeAttempts++
		if err := stream.resume(ctx, client, proxyURL); err != nil {
			return fmt.Errorf("resume stream %s attempt %d: %w", stream.id, resumeAttempts, err)
		}
		connectionSawFrame = false
	}
	return nil
}

func drainAndClose(body io.ReadCloser) error {
	_, readErr := io.Copy(io.Discard, body)
	return errors.Join(readErr, body.Close())
}

func waitRemoteReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (stream *attachedStream) resume(ctx context.Context, client *http.Client, proxyURL string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		proxyURL+"/v1/streams/"+url.PathEscape(stream.id)+"/events", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-Streamweld-Verbose", "1")
	request.Header.Set("Last-Event-ID", strconv.FormatUint(stream.lastSeq, 10))
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		return fmt.Errorf("resume stream %s: status %s: %s", stream.id, response.Status, bytes.TrimSpace(message))
	}
	stream.response = response
	stream.decoder = sse.NewDecoder(response.Body)
	return nil
}

func (stream *attachedStream) accept(event sse.Event) error {
	if event.HasID {
		if sequence, err := strconv.ParseUint(event.ID, 10, 64); err == nil && sequence > stream.lastSeq {
			stream.lastSeq = sequence
		}
	}
	if event.HasType {
		switch event.Type {
		case "streamweld.reader.error":
			if !bytes.Contains(event.Data, []byte(`"code":"reader_lag_exceeded"`)) {
				return fmt.Errorf("stream %s received an unknown reader error", stream.id)
			}
			stream.readerLagged = true
		case "streamweld.stream.migration":
			stream.migrated++
			var payload struct {
				Rescued int `json:"rescued_tokens"`
			}
			if err := json.Unmarshal(event.Data, &payload); err != nil || payload.Rescued <= 0 {
				return fmt.Errorf("stream %s has invalid migration metadata", stream.id)
			}
			stream.rescued += payload.Rescued
			stream.awaitingSeam = true
		case "streamweld.stream.warning":
			if bytes.Contains(event.Data, []byte(`"code":"journal_degraded"`)) {
				stream.degraded = true
			}
		case "streamweld.stream.done":
			stream.terminal = "done"
			if stream.degraded {
				stream.terminal = "done_degraded"
			}
		case "streamweld.stream.stopped":
			stream.terminal = "stopped"
		case "streamweld.stream.error":
			stream.terminal = "error"
			if bytes.Contains(event.Data, []byte(`"reason":"template_verdict"`)) {
				stream.terminal = "migration_refused"
			}
		}
	}
	if stream.awaitingSeam && !event.HasType {
		var payload struct {
			RawDelta string `json:"streamweld_chaos_raw_delta"`
			Usage    *struct {
				PromptTokens int `json:"prompt_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil ||
			payload.RawDelta == "" || payload.Usage == nil || payload.Usage.PromptTokens <= 0 {
			return fmt.Errorf("stream %s continuation omitted observed seam or prompt-usage metadata", stream.id)
		}
		seam, err := migrate.ReconcileSeam(
			[]byte(stream.output.String()),
			[]byte(payload.RawDelta),
			DeterministicSeamWindowBytes,
		)
		if err != nil || seam.OverlapBytes <= 0 {
			return fmt.Errorf("stream %s continuation seam observation failed: %w", stream.id, err)
		}
		stream.seamOverlaps = append(stream.seamOverlaps, seam.OverlapBytes)
		stream.promptRebilled += payload.Usage.PromptTokens
		stream.awaitingSeam = false
	}
	if bytes.Equal(event.Data, []byte("[DONE]")) {
		stream.terminal = "done"
		if stream.degraded {
			stream.terminal = "done_degraded"
		}
		return nil
	}
	stream.output.WriteString(completionText(event.Data))
	return nil
}

func closeRemoteStreams(streams []*attachedStream) {
	for _, stream := range streams {
		if stream != nil && stream.response != nil {
			_ = stream.response.Body.Close()
		}
	}
}

func validateRemoteBaseURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed.String(), nil
}
