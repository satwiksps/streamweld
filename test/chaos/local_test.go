package chaos

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestDefinitionsContainTheCompleteRequiredMatrix(t *testing.T) {
	t.Parallel()

	want := []Scenario{
		ScenarioPodKill,
		ScenarioRollingUpdate,
		ScenarioSpotReclaim,
		ScenarioBackendOOM,
		ScenarioClientDrop,
		ScenarioExplicitStop,
		ScenarioRedisDown,
		ScenarioSlowConsumer,
		ScenarioUnsafe,
	}
	got := Definitions()
	if len(got) != len(want) {
		t.Fatalf("Definitions() count = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Scenario != want[index] || got[index].Injection == "" || got[index].ExpectedOutcome == "" {
			t.Fatalf("Definitions()[%d] = %+v, want scenario %q with metadata", index, got[index], want[index])
		}
	}
}

func TestRunLocalExecutesEveryStreamAndPassesCorrectness(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	rolloutTimes := []time.Time{generatedAt, generatedAt.Add(12_500 * time.Microsecond)}
	rolloutClockIndex := 0
	report, err := RunLocal(context.Background(), LocalConfig{
		ConcurrentStreams: 7,
		OutputTokens:      32,
		Now:               func() time.Time { return generatedAt },
		RolloutNow: func() time.Time {
			value := rolloutTimes[rolloutClockIndex]
			rolloutClockIndex++
			return value
		},
		MeasureTTFT: func(context.Context, int) (TTFTMeasurement, error) {
			return TTFTMeasurement{DirectMilliseconds: 1.25, StreamweldMilliseconds: 2.75}, nil
		},
	})
	if err != nil {
		t.Fatalf("RunLocal() error = %v", err)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("Report.Validate() error = %v", err)
	}
	if !report.GeneratedAt.Equal(generatedAt) || report.Profile.Cluster || report.Profile.ConcurrentStreams != 7 {
		t.Fatalf("profile metadata = %+v at %s", report.Profile, report.GeneratedAt)
	}
	impact := report.RolloutDurationImpact
	if impact == nil || impact.MeasurementCohorts != 32 ||
		impact.MeasuredMeanCohortCompletionMilliseconds != 0.391 ||
		impact.ModeledStreamweldGraceHeadroomMilliseconds != 14_999.609 ||
		impact.ConfiguredGraceWindowReductionSeconds != 285 ||
		impact.PhysicalKubernetesTiming || !impact.FitsWithinStreamweldGracePeriod {
		t.Fatalf("rollout duration impact = %+v", impact)
	}
	for _, result := range report.Results {
		if !result.OutputCorrect || result.CorrectStreams != 7 || result.StreamsStarted != 7 {
			t.Errorf("scenario %q did not exercise all streams: %+v", result.Scenario, result)
		}
		if result.AddedTTFTMilliseconds != 1.5 {
			t.Errorf("scenario %q added TTFT = %f, want 1.5", result.Scenario, result.AddedTTFTMilliseconds)
		}
		definition, _ := definitionFor(result.Scenario)
		if definition.ExpectsMigration {
			if result.StreamsMigrated != 7 || result.TokensRescued == 0 ||
				result.PromptTokensRebilled != 7*DeterministicPromptTokens || result.SeamOverlapBytesP50 == 0 {
				t.Errorf("migration scenario %q counters = %+v", result.Scenario, result)
			}
		}
	}
}

func TestRunLocalRejectsInvalidSettingsAndMeasurements(t *testing.T) {
	t.Parallel()

	if _, err := RunLocal(context.Background(), LocalConfig{ConcurrentStreams: -1}); err == nil {
		t.Fatal("RunLocal() accepted a negative stream count")
	}
	if _, err := RunLocal(context.Background(), LocalConfig{
		ConcurrentStreams: 1,
		OutputTokens:      8,
		MeasureTTFT: func(context.Context, int) (TTFTMeasurement, error) {
			return TTFTMeasurement{DirectMilliseconds: math.NaN()}, nil
		},
	}); err == nil {
		t.Fatal("RunLocal() accepted a NaN TTFT measurement")
	}
	if _, err := RunLocal(context.Background(), LocalConfig{
		ConcurrentStreams: 1,
		OutputTokens:      8,
		RolloutNow:        func() time.Time { return time.Unix(0, 0) },
		MeasureTTFT: func(context.Context, int) (TTFTMeasurement, error) {
			return TTFTMeasurement{DirectMilliseconds: 1, StreamweldMilliseconds: 2}, nil
		},
	}); err == nil {
		t.Fatal("RunLocal() accepted a rollout measurement clock that did not advance")
	}
}

func TestActualLocalTTFTProbeUsesDirectAndProxyPaths(t *testing.T) {
	measurement, err := measureLocalTTFT(context.Background(), 3)
	if err != nil {
		t.Fatalf("measureLocalTTFT() error = %v", err)
	}
	if !finiteNonNegative(measurement.DirectMilliseconds) || !finiteNonNegative(measurement.StreamweldMilliseconds) {
		t.Fatalf("measurement = %+v", measurement)
	}
}
