package chaos

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRemoteStreamProductionDoneRejectsNonresumableState(t *testing.T) {
	for _, status := range []string{"open", "done", "error"} {
		t.Run(status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/v1/streams/slow-degraded" {
					t.Errorf("state request path = %q", request.URL.Path)
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(writer, `{"status":%q,"resumable":false}`, status)
			}))
			t.Cleanup(server.Close)
			done, err := remoteStreamProductionDone(context.Background(), server.Client(), server.URL, "slow-degraded")
			if done || err == nil || !strings.Contains(err.Error(), "not resumable") {
				t.Fatalf("remoteStreamProductionDone() = %t, %v; want nonresumable error for %q state", done, err, status)
			}
		})
	}
}
