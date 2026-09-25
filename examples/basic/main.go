// Example: minimal usage of the ogen-generated Confluent Cloud client.
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

	// Facade: one client per domain package, all sharing the http.Client.
	// SecuritySource is nil — auth is injected by NewHTTPClient's transport.
	cl, err := ccloud.New(ccloud.WithHTTPClient(httpClient))
	if err != nil {
		fatal("ccloud.New: %v", err)
	}

	// Typed response: ListIamV2ApiKeysRes is an interface; assert the OK case.
	res, err := cl.Iam.ListIamV2ApiKeys(ctx, iam.ListIamV2ApiKeysParams{})
	if err != nil {
		fatal("list api keys: %v", err)
	}
	ok, okOk := res.(*iam.ListIamV2ApiKeysOKHeaders)
	if !okOk {
		fatal("list api keys: unexpected response %T", res)
	}
	fmt.Printf("api keys: %d\n", len(ok.Response.Data))
	for _, k := range ok.Response.Data {
		fmt.Printf("  - %s\n", k.GetID())
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
