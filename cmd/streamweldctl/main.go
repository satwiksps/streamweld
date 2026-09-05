// Package main runs the Streamweld administrative command-line client.
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
	"os"
	"strings"
	"time"

	"github.com/satwiksps/streamweld/internal/conformance"
	"github.com/satwiksps/streamweld/internal/version"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultDoctorTimeout = 30 * time.Second
	defaultDrainTimeout  = 15 * time.Second
	maxDrainResponse     = 64 << 10
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	switch args[0] {
	case "--version", "-version":
		if len(args) != 1 {
			_, _ = fmt.Fprintln(stderr, "streamweldctl: --version does not accept additional arguments")
			return 2
		}
		if err := version.Current().Write(stdout, "streamweldctl"); err != nil {
			_, _ = fmt.Fprintf(stderr, "streamweldctl: write version: %v\n", err)
			return 1
		}
		return 0
	case "bench":
		return runBench(args[1:], stdout, stderr)
	case "doctor":
		return runDoctor(args[1:], stdout, stderr)
	case "drain":
		return runDrain(args[1:], stdout, stderr)
	case "streams":
		return runStreams(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "streamweldctl: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

type drainResult struct {
	PodNamespace string `json:"pod_namespace"`
	PodName      string `json:"pod_name"`
	ProxyCount   int    `json:"proxy_count"`
	InFlight     int    `json:"in_flight"`
	State        string `json:"state"`
}

func runDrain(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("streamweldctl drain", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8082", "operator drain endpoint (typically reached with kubectl port-forward)")
	namespace := flags.String("namespace", "default", "namespace containing the backend Pod")
	timeout := flags.Duration("timeout", defaultDrainTimeout, "deadline for the all-proxy drain barrier")
	jsonOutput := flags.Bool("json", false, "emit the drain result as JSON")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: streamweldctl drain [--endpoint URL] [--namespace NAME] [--json] POD")
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
		_, _ = fmt.Fprintln(stderr, "streamweldctl drain: exactly one Pod name is required")
		flags.Usage()
		return 2
	}
	pod := flags.Arg(0)
	if len(validation.IsDNS1123Label(*namespace)) != 0 || len(validation.IsDNS1123Subdomain(pod)) != 0 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl drain: namespace must be a DNS label and Pod must be a DNS subdomain")
		return 2
	}
	if *timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl drain: --timeout must be positive")
		return 2
	}
	baseURL, err := parseHTTPEndpoint(*endpoint)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl drain: %v\n", err)
		return 2
	}
	target := strings.TrimRight(baseURL.String(), "/") + "/internal/backends/by-pod/" +
		url.PathEscape(*namespace) + "/" + url.PathEscape(pod) + "/drain"
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "streamweldctl drain: construct request failed")
		return 1
	}
	request.Header.Set("Accept", "application/json")
	client := &http.Client{
		Timeout:       *timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl drain: request failed: %v\n", err)
		return 1
	}
	defer func() { _ = response.Body.Close() }()
	result, err := decodeDrainResult(response.Body)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl drain: invalid operator response: %v\n", err)
		return 1
	}
	if result.PodNamespace != *namespace || result.PodName != pod || result.ProxyCount < 0 || result.InFlight < 0 ||
		(result.State == "drained" && result.ProxyCount == 0) ||
		(result.State != "drained" && result.State != "draining") {
		_, _ = fmt.Fprintln(stderr, "streamweldctl drain: operator returned an inconsistent result")
		return 1
	}
	output := stdout
	if response.StatusCode != http.StatusOK {
		output = stderr
	}
	if err := writeDrainResult(output, result, *jsonOutput); err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl drain: write result: %v\n", err)
		return 1
	}
	if response.StatusCode != http.StatusOK || result.State != "drained" || result.InFlight != 0 {
		return 1
	}
	return 0
}

func parseHTTPEndpoint(raw string) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, errors.New("--endpoint must be an unpadded absolute HTTP(S) URL")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("--endpoint must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed, nil
}

func decodeDrainResult(reader io.Reader) (drainResult, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxDrainResponse+1))
	if err != nil {
		return drainResult{}, err
	}
	if len(data) > maxDrainResponse {
		return drainResult{}, errors.New("response exceeds the size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result drainResult
	if err := decoder.Decode(&result); err != nil {
		return drainResult{}, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return drainResult{}, errors.New("response contains multiple JSON values")
		}
		return drainResult{}, err
	}
	return result, nil
}

func writeDrainResult(writer io.Writer, result drainResult, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	_, err := fmt.Fprintf(
		writer,
		"Pod: %s/%s\nProxies: %d\nState: %s\nIn flight: %d\n",
		result.PodNamespace,
		result.PodName,
		result.ProxyCount,
		result.State,
		result.InFlight,
	)
	return err
}

func runDoctor(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("streamweldctl doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	backendURL := flags.String("backend", "", "base URL of an OpenAI-compatible backend")
	model := flags.String("model", "", "model name to probe")
	jsonOutput := flags.Bool("json", false, "emit the complete report as JSON")
	timeout := flags.Duration("timeout", defaultDoctorTimeout, "deadline for the complete twelve-request probe suite")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: streamweldctl doctor --backend URL --model NAME [--json]")
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
		_, _ = fmt.Fprintf(stderr, "streamweldctl doctor: unexpected positional arguments: %v\n", flags.Args())
		flags.Usage()
		return 2
	}
	if *backendURL == "" || *model == "" {
		_, _ = fmt.Fprintln(stderr, "streamweldctl doctor: --backend and --model are required")
		flags.Usage()
		return 2
	}
	if *timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "streamweldctl doctor: --timeout must be positive")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	checker := conformance.NewChecker(&http.Client{Timeout: *timeout}, nil)
	report, err := checker.Run(ctx, *backendURL, *model)
	if err != nil {
		if *jsonOutput {
			_ = json.NewEncoder(stderr).Encode(struct {
				Verdict conformance.Verdict `json:"verdict"`
				Error   string              `json:"error"`
			}{Verdict: conformance.VerdictUnknown, Error: err.Error()})
		} else {
			_, _ = fmt.Fprintf(stderr, "streamweldctl doctor: probe failed: %v\n", err)
		}
		return 1
	}
	if err := writeDoctorReport(stdout, report, *jsonOutput); err != nil {
		_, _ = fmt.Fprintf(stderr, "streamweldctl doctor: write report: %v\n", err)
		return 1
	}
	if report.Verdict == conformance.VerdictUnsafe || report.Verdict == conformance.VerdictUnknown {
		return 1
	}
	return 0
}

func writeDoctorReport(writer io.Writer, report conformance.Report, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	if _, err := fmt.Fprintf(writer, "Backend: %s\nModel: %s\nVerdict: %s\nChecked: %s\nProbes:\n",
		report.BackendURL,
		report.Model,
		report.Verdict,
		report.CheckedAt.Format(time.RFC3339),
	); err != nil {
		return err
	}
	for _, probe := range report.Probes {
		status := "FAIL"
		if probe.Passed {
			status = "PASS"
		}
		passed := 0
		for _, attempt := range probe.Runs {
			if attempt.Passed {
				passed++
			}
		}
		if _, err := fmt.Fprintf(writer, "  %-12s %s (%d/%d)\n", probe.Name, status, passed, len(probe.Runs)); err != nil {
			return err
		}
		for _, attempt := range probe.Runs {
			if attempt.Detail == "" {
				continue
			}
			if _, err := fmt.Fprintf(writer, "    attempt %d: %s\n", attempt.Attempt, attempt.Detail); err != nil {
				return err
			}
		}
	}
	return nil
}

func printUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "Usage: streamweldctl <command> [options]")
	_, _ = fmt.Fprintln(writer, "       streamweldctl --version")
	_, _ = fmt.Fprintln(writer)
	_, _ = fmt.Fprintln(writer, "Commands:")
	_, _ = fmt.Fprintln(writer, "  bench    run or verify the reproducible chaos benchmark matrix")
	_, _ = fmt.Fprintln(writer, "  doctor   probe chat-template continuation conformance")
	_, _ = fmt.Fprintln(writer, "  drain    drain one backend Pod across every proxy replica")
	_, _ = fmt.Fprintln(writer, "  streams  inspect one durable stream's state")
}
