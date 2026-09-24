package ccloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	iam "github.com/electric-saw/ccloud-client-go/v2/ccloud/iam"
)

// Smoke test: generated client + shared auth client wire auth, base URL and
// JSON decoding correctly end-to-end against a stub server. No real
// Confluent credentials required.
func TestNewHTTPClient_roundTrip(t *testing.T) {
	var gotAuth, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"api_version": "iam/v2",
			"kind":        "ApiKey",
			"id":          "k-abc123",
		})
	}))
	defer srv.Close()

	c, err := iam.NewClientWithResponses(srv.URL, iam.WithHTTPClient(NewHTTPClient(authFunc(func(r *http.Request) error {
		r.SetBasicAuth("key", "secret")

		return nil
	}))))
	if err != nil {
		t.Fatalf("iam.NewClientWithResponses: %v", err)
	}

	resp, err := c.GetIamV2ApiKeyWithResponse(t.Context(), "k-abc123")
	if err != nil {
		t.Fatalf("GetIamV2ApiKeyWithResponse: %v", err)
	}
	defer resp.HTTPResponse.Body.Close()

	if resp.HTTPResponse.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.HTTPResponse.StatusCode)
	}
	if gotAuth == "" {
		t.Fatal("Authorization header missing — auth RoundTripper not applied")
	}
	if want := "/iam/v2/api-keys/k-abc123"; gotPath != want {
		t.Fatalf("path = %q, want %q", gotPath, want)
	}

	var key struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(resp.Body, &key); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if key.Kind != "ApiKey" || key.ID != "k-abc123" {
		t.Fatalf("decoded = %+v, want ApiKey k-abc123", key)
	}
}

type authFunc func(*http.Request) error

func (f authFunc) SetAuth(r *http.Request) error { return f(r) }
