package journal

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOwnerRecordValidateAndJSONPrivacy(t *testing.T) {
	t.Parallel()
	owner := OwnerRecord{ReplicaID: "replica-a_1", RelayURL: "https://relay-a.internal:8443"}
	if err := owner.Validate(); err != nil {
		t.Fatalf("OwnerRecord.Validate() error = %v", err)
	}
	metaJSON, err := json.Marshal(Meta{Model: "model", BackendID: "backend", Owner: &owner})
	if err != nil {
		t.Fatalf("json.Marshal(Meta) error = %v", err)
	}
	if strings.Contains(string(metaJSON), owner.ReplicaID) || strings.Contains(string(metaJSON), owner.RelayURL) {
		t.Fatalf("private owner metadata leaked through Meta JSON: %s", metaJSON)
	}

	for _, test := range []OwnerRecord{
		{},
		{ReplicaID: "bad id", RelayURL: owner.RelayURL},
		{ReplicaID: owner.ReplicaID, RelayURL: "redis://relay.internal"},
		{ReplicaID: owner.ReplicaID, RelayURL: "https://user:secret@relay.internal"},
		{ReplicaID: owner.ReplicaID, RelayURL: "https://relay.internal/path"},
	} {
		if err := test.Validate(); err == nil {
			t.Errorf("OwnerRecord.Validate(%+v) returned nil error", test)
		}
	}
}

func TestOwnerRecordMalformedURLDoesNotLeakCredentials(t *testing.T) {
	t.Parallel()
	const secret = "operator-secret"
	owner := OwnerRecord{
		ReplicaID: "replica-a",
		RelayURL:  "https://user:" + secret + "%zz@relay.internal",
	}
	err := owner.Validate()
	if err == nil {
		t.Fatal("OwnerRecord.Validate() returned nil error")
	}
	if strings.Contains(err.Error(), secret) || err.Error() != "relay URL is malformed" {
		t.Fatalf("OwnerRecord.Validate() error = %q, want redacted malformed URL error", err)
	}
}
