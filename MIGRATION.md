# Migration guide: hand-written client → generated client

**This is a breaking change.** The old hand-written `ccloud.ConfluentClient`
was replaced by a 100% generated client from the official Confluent Cloud
OpenAPI spec (513 operations across 38 domain packages).

The module is now `github.com/electric-saw/ccloud-client-go/v2`; only the
`ccloud` package layout and API changed.

## What changed

| | Old | New |
|---|---|---|
| Client struct | `ccloud.ConfluentClient` (one client for everything) | One `Client` per domain package (`ccloud/iam`, `ccloud/kafka`, `ccloud/org`, ...) |
| Auth | `ccloud.NewClient().WithAuth(auth)` | `ccloud.NewHTTPClient(auth)` → shared `*http.Client`, passed to every package |
| Methods | Ergonomic flat names (`ListApiKeys`, `CreateFlinkComputePool(envID, ...)`) | oapi-codegen names from the spec's `operationId` (`ListIamV2ApiKeys`, `CreateFcpmV2ComputePool`) |
| Method style | `(*T, error)` returns | `(*http.Response, error)` or typed `...WithResponse` variants |
| Query params | go-querystring options structs | Generated `...Params` structs |

## Auth setup

Old:

```go
c := ccloud.NewClient().WithAuth(client.NewBasicAuth(key, secret))
```

New:

```go
hc := ccloud.NewHTTPClient(client.NewBasicAuth(key, secret)) // implements ccloud.ClientAuth
```

`ccloud.ClientAuth` is the same `SetAuth(req *http.Request) error` interface
as before, so existing auth implementations keep working.

## Making a call

Old:

```go
keys, err := c.ListApiKeys(&ApiKeyListOptions{})
```

New (plain response — you close/decode the body):

```go
import iam "github.com/electric-saw/ccloud-client-go/v2/ccloud/iam"

c, _ := iam.NewClient("https://api.confluent.cloud", iam.WithHTTPClient(hc))
resp, err := c.ListIamV2ApiKeys(ctx, &iam.ListIamV2ApiKeysParams{})
// resp is *http.Response; read resp.Body yourself
```

New (typed variant — decoded for you, via `NewClientWithResponses`):

```go
c, _ := iam.NewClientWithResponses("https://api.confluent.cloud", iam.WithHTTPClient(hc))
resp, err := c.ListIamV2ApiKeysWithResponse(ctx, &iam.ListIamV2ApiKeysParams{})
if resp.HTTPResponse.StatusCode != http.StatusOK { ... }
for _, k := range resp.JSON200.Data { fmt.Println(k.Id) }
```

## Method mapping (old → new)

| Old method | New package + method |
|---|---|
| `ListApiKeys(opts)` | `iam.ListIamV2ApiKeys(ctx, params)` |
| `GetApiKey(id)` | `iam.GetIamV2ApiKey(ctx, id)` |
| `CreateApiKey(req)` | `iam.CreateIamV2ApiKey(ctx, body)` |
| `DeleteApiKey(id)` | `iam.DeleteIamV2ApiKey(ctx, id)` |
| `ListServiceAccounts(opts)` | `iam.ListIamV2ServiceAccounts(ctx, params)` |
| `ListEnvironments(opts)` | `org.ListOrgV2Environments(ctx, params)` |
| `GetEnvironment(id)` | `org.GetOrgV2Environment(ctx, id)` |
| `ListClusters(opts)` | `cmk.ListCmkV2Clusters(ctx, params)` |
| `ListTopics(opts)` | `kafka.ListKafkaTopics(ctx, clusterId)` (see note) |
| `CreateTopic(...)` | `kafka.CreateKafkaTopic(ctx, body)` |
| `ListConnectors(opts)` | `connect.ListConnectv1Connectors(ctx, params)` |
| `CreateConnector(...)` | `connect.CreateConnectv1Connector(ctx, body)` |
| `CreateFlinkComputePool(envID, ...)` | `flink.CreateFcpmV2ComputePool(ctx, body)` |
| `ListFlinkComputePools(...)` | `flink.ListFcpmV2ComputePools(ctx, params)` |
| `CreateFlinkStatement(...)` | `flink.CreateSqlv1Statement(ctx, body)` |
| `ListSchemaRegistryClusters(...)` | `srcm.ListSrcmV3Clusters(ctx, params)` |

> Note: confirm the exact method name with `grep '^func (c *Client) ' ccloud/<pkg>/` — the table reflects the spec at generation time; a spec sync can rename schemas (method names are stable because they come from `operationId`).

## Pagination

Generated list methods take a `...Params` struct with page fields (e.g.
`ListIamV2ApiKeysParams.PageSize`, `PageToken`). Loop until the returned list
is shorter than the page size or `meta.next` is absent:

```go
params := &iam.ListIamV2ApiKeysParams{PageSize: ptr(100)}
for {
    resp, err := c.ListIamV2ApiKeysWithResponse(ctx, params)
    if err != nil { ... }
    for _, k := range resp.JSON200.Data { ... }
    if resp.JSON200.Metadata.Next == "" { break }
    params.PageToken = ptr(resp.JSON200.Metadata.Next)
}
```

## Error handling

- Plain methods return `*http.Response`; check `StatusCode`, read/close the
  body yourself.
- `...WithResponse` variants decode the body into typed fields (`resp.JSON200`,
  `resp.JSON400`, ...) and `resp.Body` holds the raw bytes.

## Notes / differences

- One client per domain: create each package's client with the same shared
  `*http.Client` from `ccloud.NewHTTPClient`.
- The generated packages are self-contained; importing only what you use keeps
  your binary small.
- Live-API tests were removed; the repo now relies on `go build`, `go vet`,
  and a smoke test against `httptest`.
- Regenerate with `make gen` (spec is vendored at `spec/openapi.yaml`);
  `mise install` sets up Go + Python tooling, `make build/vet/test` validate.
