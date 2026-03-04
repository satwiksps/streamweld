// Package main runs the Streamweld administrative command-line client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
)

const defaultDoctorTimeout = 30 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	switch args[0] {
	case "doctor":
		return runDoctor(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "streamweldctl: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
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
	_, _ = fmt.Fprintln(writer)
	_, _ = fmt.Fprintln(writer, "Commands:")
	_, _ = fmt.Fprintln(writer, "  doctor   probe chat-template continuation conformance")
}
