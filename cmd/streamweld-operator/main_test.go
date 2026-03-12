package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseOptionsPodMutationDisabledNeedsNoWebhookConfiguration(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	options, help, err := parseOptions([]string{
		"--enable-pod-mutation=false",
		"--webhook-port=0",
		"--webhook-cert-dir=",
		"--drain-hook-port=0",
	}, &stderr)
	if err != nil || help {
		t.Fatalf("options/help/error = %#v/%v/%v", options, help, err)
	}
	if options.enablePodMutation {
		t.Fatal("pod mutation unexpectedly enabled")
	}
}

func TestParseOptionsEnabledWebhookRequiresUsableConfiguration(t *testing.T) {
	t.Parallel()
	_, _, err := parseOptions([]string{
		"--enable-pod-mutation=true",
		"--webhook-port=0",
		"--webhook-cert-dir=",
		"--drain-hook-port=0",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "webhook-port") || !strings.Contains(err.Error(), "webhook-cert-dir") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadCredentialFileIsBoundedAndAcceptsSecretVolumeNewline(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	tokenPath := filepath.Join(directory, "token")
	if err := os.WriteFile(tokenPath, []byte("opaque-token\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := readCredentialFile(tokenPath)
	if err != nil || token != "opaque-token" {
		t.Fatalf("token/error = %q/%v", token, err)
	}
	largePath := filepath.Join(directory, "large")
	if err := os.WriteFile(largePath, bytes.Repeat([]byte("x"), maxCredentialFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readCredentialFile(largePath); err == nil {
		t.Fatal("oversized credential file was accepted")
	}
}

func TestPublicCredentialErrorDoesNotExposeSecretPath(t *testing.T) {
	t.Parallel()
	err := publicCredentialError(errors.New(`open C:\private\token-super-secret: access denied`))
	if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
		t.Fatalf("public error = %v", err)
	}
}

func TestParseOptionsHelp(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	_, help, err := parseOptions([]string{"--help"}, &output)
	if err != nil || !help || !strings.Contains(output.String(), "enable-pod-mutation") {
		t.Fatalf("help/error/output = %v/%v/%q", help, err, output.String())
	}
}
