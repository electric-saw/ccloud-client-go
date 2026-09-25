package ccloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	iam "github.com/electric-saw/ccloud-client-go/v2/ccloud/iam"
)

// Smoke test: generated ogen client + shared auth client wire auth, base URL
// and JSON decoding correctly end-to-end against a stub server. No real
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
			"metadata":    map[string]any{},
			"spec": map[string]any{
				"owner": map[string]any{
					"id":            "u-111aaa",
					"kind":          "User",
					"related":       "https://api.confluent.cloud/iam/v2/users/u-111aaa",
					"resource_name": "crn://confluent.cloud/user=u-111aaa",
				},
			},
		})
	}))
	defer srv.Close()

	// AuthSource from the generated security.go helper; credentials are
	// irrelevant here (the stub ignores them) — this exercises the path.
	c, err := iam.NewClient(srv.URL, iam.AuthSource{Key: "key", Secret: "secret"}, iam.WithClient(NewHTTPClient(authFunc(func(r *http.Request) error {
		r.SetBasicAuth("key", "secret")
		return nil
	}))))
	if err != nil {
		t.Fatalf("iam.NewClient: %v", err)
	}

	res, err := c.GetIamV2ApiKey(t.Context(), iam.GetIamV2ApiKeyParams{ID: "k-abc123"})
	if err != nil {
		t.Fatalf("GetIamV2ApiKey: %v", err)
	}
	ok, okOK := res.(*iam.GetIamV2ApiKeyOKHeaders)
	if !okOK {
		t.Fatalf("unexpected response %T", res)
	}
	if gotAuth == "" {
		t.Fatal("Authorization header missing — auth RoundTripper not applied")
	}
	if want := "/iam/v2/api-keys/k-abc123"; gotPath != want {
		t.Fatalf("path = %q, want %q", gotPath, want)
	}
	if got := ok.Response.GetID(); got != "k-abc123" {
		t.Fatalf("id = %q, want k-abc123", got)
	}
}

type authFunc func(*http.Request) error

func (f authFunc) SetAuth(r *http.Request) error { return f(r) }
