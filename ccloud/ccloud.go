// Package ccloud is the Confluent Cloud management API client.
//
// The API surface is 100% generated from the official OpenAPI spec
// (https://docs.confluent.io/cloud/current/openapi.yaml) by `mise run gen`.
// Each API domain lives in its own sub-package, one generated file per
// OpenAPI tag (e.g. ccloud/iam/api_keys__iam_v2.gen.go).
//
// ccloud.ClientAuth is the shared auth interface; every generated package
// accepts an *http.Client built by ccloud.NewHTTPClient.
package ccloud

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// ClientAuth authenticates outgoing requests. SetAuth is called once per
// request (skipped when an Authorization header is already present).
type ClientAuth interface {
	SetAuth(req *http.Request) error
}

// retryableTransport wraps an auth-injecting transport inside retryablehttp's
// retrying RoundTripper.
func retryableTransport(auth ClientAuth) http.RoundTripper {
	rc := retryablehttp.NewClient()
	rc.RetryMax = 10
	rc.RetryWaitMin = time.Second
	rc.RetryWaitMax = 30 * time.Second
	rc.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		ok, e := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if !ok && resp != nil &&
			(resp.StatusCode == http.StatusUnauthorized || resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented) {
			return true, e
		}

		return ok, nil
	}

	base := http.DefaultTransport
	rt := base
	if auth != nil {
		rt = authTransport{auth: auth, next: base}
	}
	rc.HTTPClient = &http.Client{Transport: rt}

	return &retryablehttp.RoundTripper{Client: rc}
}

// authTransport injects auth into every request that doesn't already carry an
// Authorization header.
type authTransport struct {
	auth ClientAuth
	next http.RoundTripper
}

func (t authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Authorization") == "" {
		if err := t.auth.SetAuth(r); err != nil {
			return nil, fmt.Errorf("failed to set auth: %w", err)
		}
	}

	return t.next.RoundTrip(r)
}

// NewHTTPClient returns an *http.Client with retries (10 attempts, backoff;
// retried on 401/5xx) and the shared auth injected on every request. Pass it
// to any generated package's WithHTTPClient option.
//
// auth may be nil (public/unauthenticated endpoints only).
func NewHTTPClient(auth ClientAuth) *http.Client {
	return &http.Client{
		Transport: retryableTransport(auth),
		Timeout:   5 * time.Minute,
	}
}
