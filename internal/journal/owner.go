package journal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var (
	// ErrOwnerNotRecorded means a journal has no private producer owner.
	ErrOwnerNotRecorded = errors.New("journal: stream owner is not recorded")
	// ErrOwnerUnavailable means the recorded owner has no live presence lease.
	ErrOwnerUnavailable = errors.New("journal: stream owner is unavailable")
)

var replicaIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// OwnerRecord is private producer-routing metadata. It must never be included
// in a public open event or StreamState JSON response.
type OwnerRecord struct {
	ReplicaID string `json:"-"`
	RelayURL  string `json:"-"`
}

// Validate checks the owner metadata before it is persisted or advertised.
func (owner OwnerRecord) Validate() error {
	if !replicaIDPattern.MatchString(owner.ReplicaID) {
		return errors.New("replica ID must be 1-128 letters, digits, dots, underscores, or hyphens")
	}
	relayURL, err := url.Parse(owner.RelayURL)
	if err != nil {
		return errors.New("relay URL is malformed")
	}
	if relayURL.Scheme != "http" && relayURL.Scheme != "https" {
		return fmt.Errorf("relay URL scheme must be http or https, got %q", relayURL.Scheme)
	}
	if relayURL.Host == "" || relayURL.Hostname() == "" {
		return errors.New("relay URL must be absolute and include a host")
	}
	if relayURL.User != nil || relayURL.RawQuery != "" || relayURL.Fragment != "" {
		return errors.New("relay URL must not contain credentials, a query, or a fragment")
	}
	if relayURL.Path != "" && relayURL.Path != "/" {
		return errors.New("relay URL must not contain a path")
	}
	if strings.ContainsAny(owner.RelayURL, "\r\n\x00") {
		return errors.New("relay URL must not contain line breaks or NUL")
	}
	return nil
}

// OwnerDirectory is an optional distributed journal extension. LocateOwner
// returns only an owner with a currently live presence lease. HeartbeatOwner
// refreshes that lease without changing immutable per-stream owner metadata.
type OwnerDirectory interface {
	LocateOwner(ctx context.Context, id StreamID) (OwnerRecord, error)
	HeartbeatOwner(ctx context.Context, owner OwnerRecord, ttl time.Duration) error
}
