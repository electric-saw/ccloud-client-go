// Example: minimal usage of the generated Confluent Cloud client.
//
// Run with:
//
//	CCLOUD_API_KEY=<key> CCLOUD_API_SECRET=<secret> go run ./examples/basic
//
// or with a single token:
//
//	CCLOUD_API_TOKEN=<token> go run ./examples/basic
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/electric-saw/ccloud-client-go/v2/ccloud"
	iam "github.com/electric-saw/ccloud-client-go/v2/ccloud/iam"
)

type basicAuth struct{ key, secret string }

func (b basicAuth) SetAuth(r *http.Request) error {
	r.SetBasicAuth(b.key, b.secret)

	return nil
}

type bearerAuth struct{ token string }

func (b bearerAuth) SetAuth(r *http.Request) error {
	r.Header.Set("Authorization", "Bearer "+b.token)

	return nil
}

func main() {
	ctx := context.Background()

	// Build the shared auth. One http.Client is reused by every package.
	var auth ccloud.ClientAuth
	if token := os.Getenv("CCLOUD_API_TOKEN"); token != "" {
		auth = bearerAuth{token: token}
	} else {
		auth = basicAuth{key: os.Getenv("CCLOUD_API_KEY"), secret: os.Getenv("CCLOUD_API_SECRET")}
	}
	httpClient := ccloud.NewHTTPClient(auth)

	// ClientWithResponses: typed, decoded responses.
	iamClient, err := iam.NewClientWithResponses("https://api.confluent.cloud", iam.WithHTTPClient(httpClient))
	if err != nil {
		fatal("iam.NewClientWithResponses: %v", err)
	}

	resp, err := iamClient.ListIamV2ApiKeysWithResponse(ctx, &iam.ListIamV2ApiKeysParams{})
	if err != nil {
		fatal("list api keys: %v", err)
	}
	if resp.HTTPResponse.StatusCode != http.StatusOK {
		fatal("list api keys: unexpected status %d: %s", resp.HTTPResponse.StatusCode, resp.Body)
	}

	// The list response schema is a oneOf, so the generated JSON200 data items
	// carry only Spec.Owner/Resource. Decode the full typed list for access to
	// Id and Spec.DisplayName.
	var list iam.IamV2ApiKeyList
	if err := json.Unmarshal(resp.Body, &list); err != nil {
		fatal("decode list: %v", err)
	}
	fmt.Printf("api keys: %d\n", len(list.Data))
	for _, k := range list.Data {
		// Spec is a oneOf in the spec, so it decodes as map[string]any.
		name, _ := k.Spec["display_name"].(string)
		fmt.Printf("  - %s (%s)\n", k.Id, name)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
