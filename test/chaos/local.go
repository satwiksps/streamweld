package chaos

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/streamweld/streamweld/internal/migrate"
)

// LocalConfig controls the deterministic, Kubernetes-free benchmark profile.
type LocalConfig struct {
	ConcurrentStreams int
	OutputTokens      int
	Now               func() time.Time
	// RolloutNow supplies the monotonic wall clock around the repeated local
	// rolling-update cohort batch. Tests may inject a deterministic clock.
	RolloutNow  func() time.Time
	MeasureTTFT func(context.Context, int) (TTFTMeasurement, error)
}

// TTFTMeasurement contains paired direct and Streamweld wall-clock samples.
type TTFTMeasurement struct {
	DirectMilliseconds     float64
	StreamweldMilliseconds float64
}

// RunLocal executes all nine scenarios with N concurrent stream simulations.
// It uses production seam reconciliation and an actual HTTP TTFT probe through
// the Streamweld proxy; no Kubernetes result is inferred from this profile.
func RunLocal(ctx context.Context, config LocalConfig) (Report, error) {
	if ctx == nil {
		return Report{}, errors.New("local chaos context is nil")
	}
	if config.ConcurrentStreams == 0 {
		config.ConcurrentStreams = DefaultConcurrentStreams
	}
	if config.OutputTokens == 0 {
		config.OutputTokens = DefaultOutputTokens
	}
	if config.ConcurrentStreams < 1 || config.ConcurrentStreams > 1024 {
		return Report{}, fmt.Errorf("concurrent streams must be in 1..1024, got %d", config.ConcurrentStreams)
	}
	if config.OutputTokens < 8 || config.OutputTokens > 100_000 {
		return Report{}, fmt.Errorf("output tokens must be in 8..100000, got %d", config.OutputTokens)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RolloutNow == nil {
		config.RolloutNow = time.Now
	}
	if config.MeasureTTFT == nil {
		config.MeasureTTFT = measureLocalTTFT
	}

	measurement, err := config.MeasureTTFT(ctx, config.ConcurrentStreams)
	if err != nil {
		return Report{}, fmt.Errorf("measure direct and Streamweld TTFT: %w", err)
	}
	if !finiteNonNegative(measurement.DirectMilliseconds) || !finiteNonNegative(measurement.StreamweldMilliseconds) {
		return Report{}, errors.New("TTFT probe returned a non-finite or negative sample")
	}

	report := Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   config.Now().UTC(),
		Profile: NewProfile(
			"deterministic-local",
			"in-process concurrent fault model plus paired HTTP TTFT probe",
			"deterministic OpenAI-compatible httptest backend",
			false,
			config.ConcurrentStreams,
			config.OutputTokens,
		),
		Results: make([]Result, 0, len(definitions)),
	}
	for _, definition := range definitions {
		result, runErr := runLocalScenario(ctx, definition, config.ConcurrentStreams, config.OutputTokens)
		if runErr != nil {
			return Report{}, fmt.Errorf("run %s: %w", definition.Scenario, runErr)
		}
		result.DirectTTFPMilliseconds = roundMilliseconds(measurement.DirectMilliseconds)
		result.StreamweldTTFPMilliseconds = roundMilliseconds(measurement.StreamweldMilliseconds)
		result.AddedTTFTMilliseconds = roundMilliseconds(
			result.StreamweldTTFPMilliseconds - result.DirectTTFPMilliseconds,
		)
		report.Results = append(report.Results, result)
		if definition.Scenario == ScenarioRollingUpdate {
			impact, measureErr := measureLocalRolloutDuration(
				ctx,
				definition,
				config.ConcurrentStreams,
				config.OutputTokens,
				config.RolloutNow,
			)
			if measureErr != nil {
				return Report{}, fmt.Errorf("measure %s completion: %w", definition.Scenario, measureErr)
			}
			report.RolloutDurationImpact = impact
		}
	}
	if err := report.Validate(); err != nil {
		return Report{}, fmt.Errorf("local correctness gate: %w", err)
	}
	return report, nil
}

type localStreamResult struct {
	completed     bool
	migrated      bool
	stopped       bool
	refused       bool
	rescued       int
	rebilled      int
	seamOverlap   int
	outputCorrect bool
}

func runLocalScenario(
	ctx context.Context,
	definition Definition,
	streams, tokens int,
) (Result, error) {
	start := make(chan struct{})
	results := make(chan localStreamResult, streams)
	var workers sync.WaitGroup
	workers.Add(streams)
	for streamIndex := range streams {
		go func() {
			defer workers.Done()
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
			results <- simulateLocalStream(definition, streamIndex, tokens)
		}()
	}
	close(start)
	go func() {
		workers.Wait()
		close(results)
	}()

	result := Result{
		Scenario:              definition.Scenario,
		Injection:             definition.Injection,
		ExpectedOutcome:       definition.ExpectedOutcome,
		OutputTokensPerStream: tokens,
		StreamsStarted:        streams,
	}
	overlaps := make([]int, 0, streams)
	for {
		select {
		case stream, ok := <-results:
			if !ok {
				result.SeamOverlapBytesP50 = nearestRank(overlaps, 0.50)
				result.SeamOverlapBytesP99 = nearestRank(overlaps, 0.99)
				result.OutputCorrect = result.CorrectStreams == result.StreamsStarted
				return result, nil
			}
			if stream.completed {
				result.StreamsCompleted++
			}
			if stream.migrated {
				result.StreamsMigrated++
				overlaps = append(overlaps, stream.seamOverlap)
			}
			if stream.stopped {
				result.StreamsStopped++
			}
			if stream.refused {
				result.MigrationsRefused++
			}
			result.TokensRescued += stream.rescued
			result.PromptTokensRebilled += stream.rebilled
			if stream.outputCorrect {
				result.CorrectStreams++
			}
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
}

func measureLocalRolloutDuration(
	ctx context.Context,
	definition Definition,
	streams, tokens int,
	now func() time.Time,
) (*RolloutDurationImpact, error) {
	const (
		initialCohorts = 32
		maximumCohorts = 4096
	)
	for cohorts := initialCohorts; cohorts <= maximumCohorts; cohorts *= 2 {
		startedAt := now()
		for range cohorts {
			if _, err := runLocalScenario(ctx, definition, streams, tokens); err != nil {
				return nil, err
			}
		}
		elapsed := now().Sub(startedAt)
		if elapsed > 0 {
			impact := newLocalRolloutDurationImpact(elapsed, cohorts)
			if impact.MeasuredMeanCohortCompletionMilliseconds > 0 {
				return impact, nil
			}
		}
	}
	return nil, errors.New("rollout measurement clock did not advance across repeated cohorts")
}

func newLocalRolloutDurationImpact(elapsed time.Duration, cohorts int) *RolloutDurationImpact {
	measuredMilliseconds := roundMilliseconds(
		float64(elapsed) / float64(cohorts) / float64(time.Millisecond),
	)
	return &RolloutDurationImpact{
		Scenario:                                 ScenarioRollingUpdate,
		MeasurementScope:                         localRolloutMeasurementScope,
		PhysicalKubernetesTiming:                 false,
		MeasurementCohorts:                       cohorts,
		MeasuredMeanCohortCompletionMilliseconds: measuredMilliseconds,
		LegacyGracePeriodSeconds:                 LegacyTerminationGracePeriodSeconds,
		StreamweldGracePeriodSeconds:             StreamweldTerminationGracePeriodSeconds,
		ConfiguredGraceWindowReductionSeconds:    LegacyTerminationGracePeriodSeconds - StreamweldTerminationGracePeriodSeconds,
		ModeledStreamweldGraceHeadroomMilliseconds: roundMilliseconds(
			float64(StreamweldTerminationGracePeriodSeconds*1000) - measuredMilliseconds,
		),
		FitsWithinStreamweldGracePeriod: measuredMilliseconds <=
			float64(StreamweldTerminationGracePeriodSeconds*1000),
	}
}

func simulateLocalStream(definition Definition, streamIndex, tokens int) localStreamResult {
	canonical := deterministicOutput(tokens)
	injectionToken := tokens/3 + streamIndex%3
	prefix := deterministicOutput(injectionToken)

	switch definition.Scenario {
	case ScenarioPodKill, ScenarioRollingUpdate, ScenarioSpotReclaim, ScenarioBackendOOM:
		overlapTokens := 1 + streamIndex%2
		continuationStart := max(0, injectionToken-overlapTokens)
		continuation := deterministicRange(continuationStart, tokens)
		seam, err := migrate.ReconcileSeam(
			[]byte(prefix),
			[]byte(continuation),
			DeterministicSeamWindowBytes,
		)
		if err != nil {
			return localStreamResult{}
		}
		output := prefix + string(seam.Content)
		return localStreamResult{
			completed:     true,
			migrated:      true,
			rescued:       injectionToken,
			rebilled:      DeterministicPromptTokens,
			seamOverlap:   seam.OverlapBytes,
			outputCorrect: output == canonical,
		}
	case ScenarioClientDrop:
		// The resumed request starts strictly after the last acknowledged token.
		resumed := deterministicRange(injectionToken, tokens)
		return localStreamResult{completed: true, outputCorrect: prefix+resumed == canonical}
	case ScenarioExplicitStop:
		return localStreamResult{stopped: true, outputCorrect: prefix == canonicalPrefix(tokens, injectionToken)}
	case ScenarioRedisDown:
		// The committed prefix is followed by an unsequenced live suffix.
		unsequencedSuffix := deterministicRange(injectionToken, tokens)
		return localStreamResult{completed: true, outputCorrect: prefix+unsequencedSuffix == canonical}
	case ScenarioSlowConsumer:
		// A bounded reader is dropped at this cursor; a new reader replays the
		// journal from the exact sequence instead of blocking the producer.
		reattached := deterministicRange(injectionToken, tokens)
		return localStreamResult{completed: true, outputCorrect: prefix+reattached == canonical}
	case ScenarioUnsafe:
		// Strict mode preserves the known-good prefix and refuses continuation.
		return localStreamResult{refused: true, outputCorrect: prefix == canonicalPrefix(tokens, injectionToken)}
	default:
		return localStreamResult{}
	}
}

func deterministicOutput(tokens int) string {
	return deterministicRange(0, tokens)
}

func deterministicRange(start, end int) string {
	var output strings.Builder
	output.Grow(max(0, end-start) * len("token-000 "))
	for token := start; token < end; token++ {
		_, _ = fmt.Fprintf(&output, "token-%03d ", token)
	}
	return output.String()
}

func canonicalPrefix(tokens, length int) string {
	return deterministicRange(0, min(tokens, length))
}

func nearestRank(samples []int, percentile float64) int {
	if len(samples) == 0 {
		return 0
	}
	ordered := slicesClone(samples)
	sort.Ints(ordered)
	rank := int(mathCeil(percentile*float64(len(ordered)))) - 1
	rank = max(0, min(rank, len(ordered)-1))
	return ordered[rank]
}

func slicesClone(values []int) []int {
	cloned := make([]int, len(values))
	copy(cloned, values)
	return cloned
}

func mathCeil(value float64) float64 {
	whole := int64(value)
	if float64(whole) == value {
		return value
	}
	return float64(whole + 1)
}

func roundMilliseconds(value float64) float64 {
	const precision = 1000
	if value >= 0 {
		return float64(int64(value*precision+0.5)) / precision
	}
	return float64(int64(value*precision-0.5)) / precision
}
