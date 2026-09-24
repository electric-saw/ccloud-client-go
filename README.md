# ccloud-client-go

[![Go Report Card](https://goreportcard.com/badge/github.com/electric-saw/ccloud-client-go)](https://goreportcard.com/report/github.com/electric-saw/ccloud-client-go)
[![GoDoc](https://godoc.org/github.com/electric-saw/ccloud-client-go?status.svg)](https://godoc.org/github.com/electric-saw/ccloud-client-go)
[![License](https://img.shields.io/github/license/electric-saw/ccloud-client-go)](LICENSE)

Go client for the [Confluent Cloud management API](https://docs.confluent.io/cloud/current/api.html/), **100% generated** from the official [OpenAPI spec](https://docs.confluent.io/cloud/current/openapi.yaml) by [`ccloud-gen`](scripts/ccloud-gen). It covers the entire API surface: IAM, Kafka clusters, environments, Flink, Schema Registry, networking, connectors, BYOK, and more.

## Installation

```bash
go get github.com/electric-saw/ccloud-client-go/v2
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/electric-saw/ccloud-client-go/v2/ccloud"
	iam "github.com/electric-saw/ccloud-client-go/v2/ccloud/iam"
)

type basicAuth struct{ key, secret string }

func (b basicAuth) SetAuth(r *http.Request) error {
	r.SetBasicAuth(b.key, b.secret)
	return nil
}

func main() {
	// One shared http.Client (with retries + auth) for every domain package.
	hc := ccloud.NewHTTPClient(basicAuth{key: os.Getenv("CCLOUD_API_KEY"), secret: os.Getenv("CCLOUD_API_SECRET")})

	// Facade: ccloud.New() wires all 38 domain packages to the same client.
	cl, err := ccloud.New(ccloud.WithHTTPClient(hc))
	if err != nil {
		panic(err)
	}

	// Typed responses via ...WithResponse.
	resp, err := cl.IAM.ListIamV2ApiKeysWithResponse(context.Background(), &iam.ListIamV2ApiKeysParams{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("api keys: %d\n", len(resp.JSON200.Data))
}
```

Or use a single domain package directly (smaller binaries):

```go
c, _ := iam.NewClient("https://api.confluent.cloud", iam.WithHTTPClient(hc))
resp, _ := c.ListIamV2ApiKeysWithResponse(ctx, &iam.ListIamV2ApiKeysParams{})
```

## Layout

- `ccloud/` — package root: `ClientAuth`, `NewHTTPClient` (retry + auth), and the `ccloud.New()` facade.
- `ccloud/<domain>/` — one generated package per API namespace (`iam`, `kafka`, `org`, `networking`, `flink`, `connect`, `byok`, ...), one file per OpenAPI tag inside each package.
- `spec/openapi.yaml` — vendored upstream spec (single source of truth).

## Regenerating

```bash
mise install        # go, golangci-lint, oapi-codegen (v2.8.0)
mise run gen        # spec -> 38 packages -> per-tag files -> facade
mise run build      # go build ./...
mise run test       # go test ./...
```

Every `mise run gen` re-derives the package grouping from the spec, so new
Confluent endpoints are picked up automatically. A GitHub Action
(`spec-sync`) runs this daily and opens a PR when the spec changes.

## Migration

Existing consumers of the old hand-written client: see [MIGRATION.md](MIGRATION.md).

## Examples

See [examples/](examples/) for a runnable example.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
