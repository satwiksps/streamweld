package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/streamweld/streamweld/test/chaos"
)

const (
	defaultBenchTimeout = 30 * time.Minute
	defaultBenchOutput  = "benchmarks"
	defaultBenchREADME  = "README.md"
)

func runBench(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("streamweldctl bench", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profile := flags.String("profile", "local", "execution profile: local, kind, or vllm")
	output := flags.String("output", defaultBenchOutput, "directory for results.json and results.md")
	streams := flags.Int("streams", chaos.DefaultConcurrentStreams, "concurrent streams per scenario")
	tokens := flags.Int("tokens", chaos.DefaultOutputTokens, "deterministic output tokens per stream")
	verify := flags.Bool("verify", false, "verify existing artifacts and the correctness column without running")
	timeout := flags.Duration("timeout", defaultBenchTimeout, "deadline for the complete profile")
	proxyURL := flags.String("proxy-url", "", "kind profile Streamweld base URL")
	directURL := flags.String("direct-url", "", "kind profile direct backend base URL")
	model := flags.String("model", "", "real-vLLM profile model name")
	prompt := flags.String("prompt", "Continue with a short deterministic explanation of durable token streaming.", "real-vLLM profile prompt")
	namespace := flags.String("namespace", "streamweld-system", "kind profile namespace")
	stableImage := flags.String("stable-image", "streamweld-chaos-backend:kind", "kind stable backend image")
	rolloutImage := flags.String("rollout-image", "streamweld-chaos-backend:kind-rollout", "kind rollout backend image")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: streamweldctl bench [--profile local|kind|vllm] [--output DIR] [--streams N] [--tokens N]")
		_, _ = fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "streamweldctl bench: unexpected positional arguments: %v\n", flags.Args())
		return 2
	}
	if *output == "" || *timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl bench: --output is required and --timeout must be positive")
		return 2
	}
	if *verify {
		if err := chaos.VerifyArtifacts(*output); err != nil {
			_, _ = fmt.Fprintf(stderr, "streamweldctl bench: verification failed: %v\n", err)
			return 1
		}
		if isDefaultBenchOutput(*output) {
			report, err := chaos.ReadArtifacts(*output)
			if err != nil {
				_, _ = fmt.Fprintf(stderr, "streamweldctl bench: verification failed: %v\n", err)
				return 1
			}
			if err := chaos.VerifyREADMEBenchmarkSection(defaultBenchREADME, report); err != nil {
				_, _ = fmt.Fprintf(stderr, "streamweldctl bench: verification failed: %v\n", err)
				return 1
			}
		}
		_, _ = fmt.Fprintf(stdout, "verified deterministic correctness in %s\n", *output)
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var report chaos.Report
	var err error
	switch *profile {
	case "local":
		report, err = chaos.RunLocal(ctx, chaos.LocalConfig{
			ConcurrentStreams: *streams,
			OutputTokens:      *tokens,
		})
	case "kind":
		if *proxyURL == "" || *directURL == "" {
			_, _ = fmt.Fprintln(stderr, "streamweldctl bench: kind profile requires --proxy-url and --direct-url")
			return 2
		}
		injector, injectorErr := chaos.NewKindInjector(chaos.KindConfig{
			Namespace:         *namespace,
			BackendDeployment: "streamweld-chaos-backend",
			BackendContainer:  "backend",
			BackendSelector:   "app.kubernetes.io/name=streamweld-chaos-backend",
			RedisDeployment:   "streamweld-redis",
			InferenceRoute:    "deterministic-chaos",
			StableImage:       *stableImage,
			RolloutImage:      *rolloutImage,
		}, nil)
		if injectorErr != nil {
			err = injectorErr
			break
		}
		if clusterErr := injector.RequireCluster(ctx); clusterErr != nil {
			err = clusterErr
			break
		}
		report, err = chaos.RunRemote(ctx, chaos.RemoteConfig{
			ProfileName:       "deterministic-kind",
			Execution:         "kind cluster with kubectl failure injection",
			ProxyURL:          *proxyURL,
			DirectURL:         *directURL,
			ConcurrentStreams: *streams,
			OutputTokens:      *tokens,
			Client:            &http.Client{},
			Injector:          injector,
		})
	case "vllm":
		if *proxyURL == "" || *directURL == "" || *model == "" || *prompt == "" {
			_, _ = fmt.Fprintln(stderr, "streamweldctl bench: vllm profile requires --proxy-url, --direct-url, --model, and --prompt")
			return 2
		}
		vllmReport, vllmErr := chaos.RunVLLM(ctx, chaos.VLLMConfig{
			ProxyURL:          *proxyURL,
			DirectURL:         *directURL,
			Model:             *model,
			Prompt:            *prompt,
			ConcurrentStreams: *streams,
			MaxTokens:         *tokens,
		})
		if vllmErr != nil {
			err = vllmErr
			break
		}
		if writeErr := chaos.WriteVLLMArtifacts(*output, vllmReport); writeErr != nil {
			_, _ = fmt.Fprintf(stderr, "streamweldctl bench: write real-vLLM artifacts: %v\n", writeErr)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "wrote %s/vllm-results.json and %s/vllm-results.md (exact output passed)\n", *output, *output)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "streamweldctl bench: unknown profile %q (want local, kind, or vllm)\n", *profile)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl bench: %s profile failed: %v\n", *profile, err)
		return 1
	}
	if err := chaos.WriteArtifacts(*output, report); err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl bench: write artifacts: %v\n", err)
		return 1
	}
	if isDefaultBenchOutput(*output) {
		if err := chaos.UpdateREADMEBenchmarkSection(defaultBenchREADME, report); err != nil {
			_, _ = fmt.Fprintf(stderr, "streamweldctl bench: update README: %v\n", err)
			return 1
		}
	}
	_, _ = fmt.Fprintf(stdout, "wrote %s/results.json and %s/results.md (%s profile; correctness passed)\n", *output, *output, *profile)
	return 0
}

func isDefaultBenchOutput(output string) bool {
	return filepath.Clean(output) == filepath.Clean(defaultBenchOutput)
}
