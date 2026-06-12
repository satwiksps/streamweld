// Package chaos provides Streamweld's deterministic failure-injection matrix.
//
// The default profile is intentionally runnable without Kubernetes: it models
// each fault against the canonical token sequence and exercises the production
// seam reconciler. The kind profile drives the same matrix against a cluster,
// while the vLLM profile is an explicit, externally provisioned opt-in.
package chaos

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"strings"
	"time"
)

// Supported failure-injection scenarios.
const (
	// SchemaVersion changes when the committed result contract changes.
	SchemaVersion = "streamweld.benchmarks/v2"
	// DefaultConcurrentStreams is deliberately small enough for laptops and CI.
	DefaultConcurrentStreams = 8
	// DefaultOutputTokens is the length of every deterministic generation.
	DefaultOutputTokens = 64
	// DeterministicPromptTokens is reported by the fake backend for every attempt.
	DeterministicPromptTokens = 16
	// DeterministicSeamWindowBytes matches the committed chaos policy.
	DeterministicSeamWindowBytes = 64
	// LegacyTerminationGracePeriodSeconds is the comparison budget described in
	// the operations guidance. It is configuration input, not measured timing.
	LegacyTerminationGracePeriodSeconds = 300
	// StreamweldTerminationGracePeriodSeconds is the managed backend target.
	// It is configuration input, not measured timing.
	StreamweldTerminationGracePeriodSeconds = 15
)

const localRolloutMeasurementScope = "amortized monotonic wall-clock mean across a batch of sequential in-process rolling-update cohorts, from before each harness cohort setup through every simulated stream reaching its terminal result; repeated cohorts overcome host clock granularity and include harness overhead; excludes Kubernetes control-plane, scheduling, image-pull, readiness, process-exit, GPU-idle, and cost timing"

// Scenario identifies one required failure injection.
type Scenario string

const (
	// ScenarioPodKill abruptly deletes the active backend Pod.
	ScenarioPodKill Scenario = "pod-kill"
	// ScenarioRollingUpdate replaces the backend image through a rolling Deployment.
	ScenarioRollingUpdate Scenario = "rolling-update"
	// ScenarioSpotReclaim cordons and drains the backend's worker node.
	ScenarioSpotReclaim Scenario = "spot-reclaim"
	// ScenarioBackendOOM emits an in-band backend failure.
	ScenarioBackendOOM Scenario = "backend-oom"
	// ScenarioClientDrop closes and resumes only the consumer transport.
	ScenarioClientDrop Scenario = "client-drop"
	// ScenarioExplicitStop calls the durable stop endpoint.
	ScenarioExplicitStop Scenario = "explicit-stop"
	// ScenarioRedisDown removes journal persistence during generation.
	ScenarioRedisDown Scenario = "redis-down"
	// ScenarioSlowConsumer exceeds the bounded reader lag.
	ScenarioSlowConsumer Scenario = "slow-consumer"
	// ScenarioUnsafe refuses migration into an unsafe template.
	ScenarioUnsafe Scenario = "unsafe-template"
)

// Definition describes the distinct injection and expected terminal outcome.
type Definition struct {
	Scenario          Scenario
	Injection         string
	ExpectedOutcome   string
	ExpectsMigration  bool
	ExpectsCompletion bool
}

var definitions = []Definition{
	{ScenarioPodKill, "SIGKILL the selected backend Pod after every reader attaches", "done", true, true},
	{ScenarioRollingUpdate, "replace the backend image with maxUnavailable=1", "done", true, true},
	{ScenarioSpotReclaim, "cordon and drain the worker hosting a backend", "done", true, true},
	{ScenarioBackendOOM, "emit an OpenAI error chunk after a deterministic prefix", "done", true, true},
	{ScenarioClientDrop, "close each client transport, wait, and resume from its last sequence", "done", false, true},
	{ScenarioExplicitStop, "POST the stream stop endpoint after a deterministic prefix", "stopped", false, false},
	{ScenarioRedisDown, "scale the Redis Deployment to zero while producers continue", "done_degraded", false, true},
	{ScenarioSlowConsumer, "constrain the TCP receive window, exceed reader lag, then resume", "done", false, true},
	{ScenarioUnsafe, "fail a producer when only an UNSAFE continuation target remains", "migration_refused", false, false},
}

// Definitions returns the required scenarios in report order.
func Definitions() []Definition {
	return slices.Clone(definitions)
}

// Profile records exactly where and how results were produced.
type Profile struct {
	Name                          string  `json:"name"`
	Execution                     string  `json:"execution"`
	Cluster                       bool    `json:"cluster"`
	Backend                       string  `json:"backend"`
	ConcurrentStreams             int     `json:"concurrent_streams"`
	OutputTokensPerStream         int     `json:"output_tokens_per_stream"`
	PromptTokensPerAttempt        int     `json:"prompt_tokens_per_attempt"`
	GOOS                          string  `json:"goos"`
	GOARCH                        string  `json:"goarch"`
	GoVersion                     string  `json:"go_version"`
	TTFTSerializationMilliseconds float64 `json:"ttft_serialization_resolution_ms"`
	TTFTBackendDelayMilliseconds  float64 `json:"ttft_backend_first_token_delay_ms"`
	TimingScope                   string  `json:"timing_scope"`
	CorrectnessGate               string  `json:"correctness_gate"`
}

// Result is one row in the injected-failure evidence table.
type Result struct {
	Scenario                   Scenario `json:"scenario"`
	Injection                  string   `json:"injection"`
	ExpectedOutcome            string   `json:"expected_outcome"`
	OutputTokensPerStream      int      `json:"output_tokens_per_stream"`
	StreamsStarted             int      `json:"streams_started"`
	StreamsCompleted           int      `json:"streams_completed"`
	StreamsMigrated            int      `json:"streams_migrated"`
	StreamsStopped             int      `json:"streams_stopped"`
	MigrationsRefused          int      `json:"migrations_refused"`
	TokensRescued              int      `json:"tokens_rescued"`
	PromptTokensRebilled       int      `json:"prompt_tokens_rebilled"`
	SeamOverlapBytesP50        int      `json:"seam_overlap_bytes_p50"`
	SeamOverlapBytesP99        int      `json:"seam_overlap_bytes_p99"`
	DirectTTFPMilliseconds     float64  `json:"direct_ttft_ms_p50"`
	StreamweldTTFPMilliseconds float64  `json:"streamweld_ttft_ms_p50"`
	AddedTTFTMilliseconds      float64  `json:"added_ttft_ms_p50"`
	CorrectStreams             int      `json:"correct_streams"`
	OutputCorrect              bool     `json:"output_correct"`
}

// RolloutDurationImpact records a measured local migration interval and an
// explicitly modelled comparison with configured termination-grace budgets.
// PhysicalKubernetesTiming is false for the committed local profile; the
// report must not present this interval as a cluster rollout measurement.
type RolloutDurationImpact struct {
	Scenario                                   Scenario `json:"scenario"`
	MeasurementScope                           string   `json:"measurement_scope"`
	PhysicalKubernetesTiming                   bool     `json:"physical_kubernetes_timing"`
	MeasurementCohorts                         int      `json:"measurement_cohorts"`
	MeasuredMeanCohortCompletionMilliseconds   float64  `json:"measured_mean_cohort_completion_ms"`
	LegacyGracePeriodSeconds                   int      `json:"legacy_grace_period_seconds"`
	StreamweldGracePeriodSeconds               int      `json:"streamweld_grace_period_seconds"`
	ConfiguredGraceWindowReductionSeconds      int      `json:"configured_grace_window_reduction_seconds"`
	ModeledStreamweldGraceHeadroomMilliseconds float64  `json:"modeled_streamweld_grace_headroom_ms"`
	FitsWithinStreamweldGracePeriod            bool     `json:"fits_within_streamweld_grace_period"`
}

// Report is the complete benchmark artifact written as JSON and Markdown.
type Report struct {
	SchemaVersion         string                 `json:"schema_version"`
	GeneratedAt           time.Time              `json:"generated_at"`
	Profile               Profile                `json:"profile"`
	RolloutDurationImpact *RolloutDurationImpact `json:"rollout_duration_impact,omitempty"`
	Results               []Result               `json:"results"`
}

// NewProfile constructs metadata for an honestly labelled run.
func NewProfile(name, execution, backend string, cluster bool, streams, tokens int) Profile {
	return Profile{
		Name:                          name,
		Execution:                     execution,
		Cluster:                       cluster,
		Backend:                       backend,
		ConcurrentStreams:             streams,
		OutputTokensPerStream:         tokens,
		PromptTokensPerAttempt:        DeterministicPromptTokens,
		GOOS:                          runtime.GOOS,
		GOARCH:                        runtime.GOARCH,
		GoVersion:                     runtime.Version(),
		TTFTSerializationMilliseconds: 0.001,
		TTFTBackendDelayMilliseconds:  ttftBackendDelay.Seconds() * 1000,
		TimingScope:                   "one paired N-stream wall-clock p50 baseline measured before the matrix and joined to every scenario row; compare only within this host/profile",
		CorrectnessGate:               "all streams must match the deterministic full output or scenario-specific terminal prefix",
	}
}

// Validate rejects incomplete, duplicated, or incorrect reports. This is the
// nightly correctness regression gate; latency is deliberately not thresholded.
func (report Report) Validate() error {
	var problems []error
	if report.SchemaVersion != SchemaVersion {
		problems = append(problems, fmt.Errorf("schema_version = %q, want %q", report.SchemaVersion, SchemaVersion))
	}
	if report.GeneratedAt.IsZero() {
		problems = append(problems, errors.New("generated_at is required"))
	}
	if report.Profile.ConcurrentStreams <= 0 || report.Profile.ConcurrentStreams > 1024 ||
		report.Profile.OutputTokensPerStream < 8 || report.Profile.OutputTokensPerStream > 100_000 ||
		report.Profile.PromptTokensPerAttempt != DeterministicPromptTokens {
		problems = append(problems, errors.New("profile stream and token counts are outside deterministic harness bounds"))
	}
	if report.Profile.Name == "" || report.Profile.Execution == "" || report.Profile.Backend == "" ||
		report.Profile.GOOS == "" || report.Profile.GOARCH == "" || report.Profile.GoVersion == "" ||
		report.Profile.TimingScope == "" || report.Profile.CorrectnessGate == "" {
		problems = append(problems, errors.New("profile provenance metadata is incomplete"))
	}
	if !finiteNonNegative(report.Profile.TTFTBackendDelayMilliseconds) ||
		!finiteNonNegative(report.Profile.TTFTSerializationMilliseconds) ||
		report.Profile.TTFTSerializationMilliseconds == 0 {
		problems = append(problems, errors.New("profile TTFT delay must be nonnegative and serialization resolution must be positive"))
	}
	if len(report.Results) != len(definitions) {
		problems = append(problems, fmt.Errorf("result count = %d, want %d", len(report.Results), len(definitions)))
	}
	seen := make(map[Scenario]struct{}, len(report.Results))
	for index, result := range report.Results {
		if _, duplicate := seen[result.Scenario]; duplicate {
			problems = append(problems, fmt.Errorf("result %d duplicates scenario %q", index, result.Scenario))
		}
		seen[result.Scenario] = struct{}{}
		definition, ok := definitionFor(result.Scenario)
		if !ok {
			problems = append(problems, fmt.Errorf("result %d has unknown scenario %q", index, result.Scenario))
			continue
		}
		if result.Injection != definition.Injection || result.ExpectedOutcome != definition.ExpectedOutcome {
			problems = append(problems, fmt.Errorf("scenario %q metadata does not match its definition", result.Scenario))
		}
		if result.StreamsStarted != report.Profile.ConcurrentStreams {
			problems = append(problems, fmt.Errorf("scenario %q started %d streams, want %d", result.Scenario, result.StreamsStarted, report.Profile.ConcurrentStreams))
		}
		if result.OutputTokensPerStream < 8 || result.OutputTokensPerStream > 100_000 {
			problems = append(problems, fmt.Errorf("scenario %q has an out-of-bounds output-token workload", result.Scenario))
		}
		if result.Scenario != ScenarioSlowConsumer && result.OutputTokensPerStream != report.Profile.OutputTokensPerStream {
			problems = append(problems, fmt.Errorf("scenario %q token workload differs from the profile", result.Scenario))
		}
		if result.Scenario == ScenarioSlowConsumer && result.OutputTokensPerStream < report.Profile.OutputTokensPerStream {
			problems = append(problems, errors.New("slow-consumer workload cannot be smaller than the profile workload"))
		}
		if result.CorrectStreams != result.StreamsStarted || !result.OutputCorrect {
			problems = append(problems, fmt.Errorf("scenario %q deterministic output correctness failed (%d/%d)", result.Scenario, result.CorrectStreams, result.StreamsStarted))
		}
		if definition.ExpectsCompletion && result.StreamsCompleted != result.StreamsStarted {
			problems = append(problems, fmt.Errorf("scenario %q completed %d/%d streams", result.Scenario, result.StreamsCompleted, result.StreamsStarted))
		}
		if !definition.ExpectsCompletion && result.StreamsCompleted != 0 {
			problems = append(problems, fmt.Errorf("scenario %q unexpectedly completed %d streams", result.Scenario, result.StreamsCompleted))
		}
		counts := []struct {
			name  string
			value int
		}{
			{"streams_completed", result.StreamsCompleted},
			{"streams_migrated", result.StreamsMigrated},
			{"streams_stopped", result.StreamsStopped},
			{"migrations_refused", result.MigrationsRefused},
			{"correct_streams", result.CorrectStreams},
		}
		for _, count := range counts {
			if count.value < 0 || count.value > result.StreamsStarted {
				problems = append(problems, fmt.Errorf(
					"scenario %q %s = %d, want 0..%d",
					result.Scenario,
					count.name,
					count.value,
					result.StreamsStarted,
				))
			}
		}
		if result.TokensRescued < 0 || result.PromptTokensRebilled < 0 ||
			result.SeamOverlapBytesP50 < 0 || result.SeamOverlapBytesP99 < result.SeamOverlapBytesP50 {
			problems = append(problems, fmt.Errorf("scenario %q contains invalid token or seam counters", result.Scenario))
		}
		if result.TokensRescued > result.StreamsStarted*result.OutputTokensPerStream {
			problems = append(problems, fmt.Errorf("scenario %q rescued more tokens than its complete workload", result.Scenario))
		}
		if definition.ExpectsMigration && result.StreamsMigrated != result.StreamsStarted {
			problems = append(problems, fmt.Errorf(
				"scenario %q migrated %d/%d injected streams",
				result.Scenario,
				result.StreamsMigrated,
				result.StreamsStarted,
			))
		}
		if definition.ExpectsMigration && (result.TokensRescued <= 0 || result.PromptTokensRebilled <= 0) {
			problems = append(problems, fmt.Errorf("scenario %q migrated without measured rescued/re-billed tokens", result.Scenario))
		}
		if definition.ExpectsMigration && result.PromptTokensRebilled != result.StreamsMigrated*report.Profile.PromptTokensPerAttempt {
			problems = append(problems, fmt.Errorf("scenario %q prompt re-billing does not match observed migration attempts", result.Scenario))
		}
		if definition.ExpectsMigration && (result.SeamOverlapBytesP50 <= 0 || result.SeamOverlapBytesP99 <= 0 ||
			result.SeamOverlapBytesP99 > DeterministicSeamWindowBytes) {
			problems = append(problems, fmt.Errorf("scenario %q has no valid observed seam-overlap distribution", result.Scenario))
		}
		if !definition.ExpectsMigration && result.StreamsMigrated != 0 {
			problems = append(problems, fmt.Errorf("scenario %q unexpectedly migrated %d streams", result.Scenario, result.StreamsMigrated))
		}
		if !definition.ExpectsMigration && (result.TokensRescued != 0 || result.PromptTokensRebilled != 0) {
			problems = append(problems, fmt.Errorf("scenario %q recorded migration token counters without a migration", result.Scenario))
		}
		if !definition.ExpectsMigration && (result.SeamOverlapBytesP50 != 0 || result.SeamOverlapBytesP99 != 0) {
			problems = append(problems, fmt.Errorf("scenario %q recorded a seam distribution without a migration", result.Scenario))
		}
		switch result.Scenario {
		case ScenarioExplicitStop:
			if result.StreamsStopped != result.StreamsStarted || result.MigrationsRefused != 0 {
				problems = append(problems, fmt.Errorf("scenario %q did not stop every stream exactly once", result.Scenario))
			}
		case ScenarioUnsafe:
			if result.MigrationsRefused != result.StreamsStarted || result.StreamsStopped != 0 {
				problems = append(problems, fmt.Errorf("scenario %q did not refuse every migration exactly once", result.Scenario))
			}
		default:
			if result.StreamsStopped != 0 || result.MigrationsRefused != 0 {
				problems = append(problems, fmt.Errorf("scenario %q has an unexpected stop or refusal", result.Scenario))
			}
		}
		if !finiteNonNegative(result.DirectTTFPMilliseconds) || result.DirectTTFPMilliseconds == 0 ||
			!finiteNonNegative(result.StreamweldTTFPMilliseconds) || result.StreamweldTTFPMilliseconds == 0 ||
			math.IsNaN(result.AddedTTFTMilliseconds) || math.IsInf(result.AddedTTFTMilliseconds, 0) {
			problems = append(problems, fmt.Errorf("scenario %q contains invalid TTFT measurements", result.Scenario))
		}
		wantAdded := roundMilliseconds(result.StreamweldTTFPMilliseconds - result.DirectTTFPMilliseconds)
		if math.Abs(result.AddedTTFTMilliseconds-wantAdded) > report.Profile.TTFTSerializationMilliseconds/2 {
			problems = append(problems, fmt.Errorf("scenario %q added TTFT is inconsistent with its paired measurements", result.Scenario))
		}
	}
	for _, definition := range definitions {
		if _, ok := seen[definition.Scenario]; !ok {
			problems = append(problems, fmt.Errorf("scenario %q is missing", definition.Scenario))
		}
	}
	if report.Profile.Name == "deterministic-local" && report.RolloutDurationImpact == nil {
		problems = append(problems, errors.New("rollout_duration_impact is required for the deterministic-local profile"))
	}
	if impact := report.RolloutDurationImpact; impact != nil {
		if impact.Scenario != ScenarioRollingUpdate {
			problems = append(problems, fmt.Errorf("rollout_duration_impact scenario = %q, want %q", impact.Scenario, ScenarioRollingUpdate))
		}
		if impact.MeasurementScope == "" {
			problems = append(problems, errors.New("rollout_duration_impact measurement_scope is required"))
		}
		if report.Profile.Name == "deterministic-local" && impact.MeasurementScope != localRolloutMeasurementScope {
			problems = append(problems, errors.New("deterministic-local rollout_duration_impact measurement_scope does not match the generated local model"))
		}
		if report.Profile.Name == "deterministic-local" && impact.PhysicalKubernetesTiming {
			problems = append(problems, errors.New("deterministic-local rollout_duration_impact cannot claim physical Kubernetes timing"))
		}
		if impact.MeasurementCohorts <= 0 {
			problems = append(problems, errors.New("rollout_duration_impact measurement_cohorts must be positive"))
		}
		if !finiteNonNegative(impact.MeasuredMeanCohortCompletionMilliseconds) ||
			impact.MeasuredMeanCohortCompletionMilliseconds == 0 {
			problems = append(problems, errors.New("rollout_duration_impact measured interval must be finite and positive"))
		}
		if impact.LegacyGracePeriodSeconds != LegacyTerminationGracePeriodSeconds ||
			impact.StreamweldGracePeriodSeconds != StreamweldTerminationGracePeriodSeconds {
			problems = append(problems, errors.New("rollout_duration_impact grace periods do not match the documented 300s and 15s comparison"))
		}
		wantReduction := LegacyTerminationGracePeriodSeconds - StreamweldTerminationGracePeriodSeconds
		if impact.ConfiguredGraceWindowReductionSeconds != wantReduction {
			problems = append(problems, fmt.Errorf(
				"rollout_duration_impact configured grace-window reduction = %d, want %d",
				impact.ConfiguredGraceWindowReductionSeconds,
				wantReduction,
			))
		}
		wantHeadroom := roundMilliseconds(
			float64(StreamweldTerminationGracePeriodSeconds*1000) - impact.MeasuredMeanCohortCompletionMilliseconds,
		)
		if math.IsNaN(impact.ModeledStreamweldGraceHeadroomMilliseconds) ||
			math.IsInf(impact.ModeledStreamweldGraceHeadroomMilliseconds, 0) ||
			math.Abs(impact.ModeledStreamweldGraceHeadroomMilliseconds-wantHeadroom) > 0.0005 {
			problems = append(problems, errors.New("rollout_duration_impact modeled Streamweld grace headroom is inconsistent with the measured interval"))
		}
		wantFits := impact.MeasuredMeanCohortCompletionMilliseconds <=
			float64(StreamweldTerminationGracePeriodSeconds*1000)
		if impact.FitsWithinStreamweldGracePeriod != wantFits {
			problems = append(problems, errors.New("rollout_duration_impact grace-period fit does not match the measured interval"))
		}
	}
	return errors.Join(problems...)
}

func definitionFor(scenario Scenario) (Definition, bool) {
	for _, definition := range definitions {
		if definition.Scenario == scenario {
			return definition, true
		}
	}
	return Definition{}, false
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

// DecodeReport performs strict JSON decoding before validation.
func DecodeReport(data []byte) (Report, error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var report Report
	if err := decoder.Decode(&report); err != nil {
		return Report{}, fmt.Errorf("decode benchmark report: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Report{}, errors.New("benchmark report contains trailing JSON")
		}
		return Report{}, fmt.Errorf("decode trailing benchmark JSON: %w", err)
	}
	if err := report.Validate(); err != nil {
		return Report{}, err
	}
	return report, nil
}
