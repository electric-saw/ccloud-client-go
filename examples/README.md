# Examples

## basic

List API keys from the IAM API using the generated client.

```sh
CCLOUD_API_KEY=<key> CCLOUD_API_SECRET=<secret> go run ./examples/basic
# or with a bearer token:
CCLOUD_API_TOKEN=<token> go run ./examples/basic
```

The example shows the core pattern used by every generated package:

1. Build a shared `ccloud.ClientAuth` (basic auth or bearer token).
2. Create one `*http.Client` with `ccloud.NewHTTPClient(auth)`.
3. Pass it to the domain package's `NewClient(url, pkg.WithHTTPClient(httpClient))`.
4. Call methods; use the `...WithResponse` variants to get typed, decoded bodies.

## Generated packages

| Package | Domains (OpenAPI tags) |
|---|---|
| `ccloud/iam` | API Keys, Service Accounts, Role Bindings, Invitations, Users (iam/v2), Identity Pools/Providers, IP Filters/Groups, Certificates, SSO Group Mappings, Jwks |
| `ccloud/org` | Environments, Organizations, Scim Tokens (org/v2) |
| `ccloud/kafka` | Cluster, Topic, Partition, Configs, ACL, Cluster Linking, Consumer Group, Share Group, Records, Streams Group (v3) |
| `ccloud/flink` | Compute Pools (fcpm/v2), Statements (sql/v1), Flink Artifacts (artifact/v1) |
| `ccloud/networking` | Networks, Peerings, Gateways, Private Link, DNS, Access Points, Transit Gateway, Network Links (networking/v1) |
| `ccloud/connect` | Connectors, Custom/Managed Plugins, Lifecycle, Offsets, Status, Presigned Urls (connect/v1) |
| `ccloud/sql` | Connections, Tools, Materialized Tables, Statement Results/Exceptions, Agents (sql/v1) |
| `ccloud/cmk` | Clusters (cmk/v2) |
| `ccloud/srcm` | Schema Registry clusters/regions (srcm/v2, srcm/v3) |
| `ccloud/schemas` | Schemas (v1) |
| `ccloud/subjects` | Subjects (v1) |
| `ccloud/byok` | Keys (byok/v1) |
| `ccloud/cdx` | Consumer/Provider Shares, Shared Resources/Tokens, Opt Ins (cdx/v1) |
| `ccloud/notifications` | Integrations, Subscriptions, User Notifications (notifications/v1) |
| `ccloud/partner` | Organizations, Entitlements, Signup (partner/v2) |
| `ccloud/billing` | Costs (billing/v1) |
| `ccloud/catalog` | Entity, Search, Types (v1) |
| `ccloud/cam` | Connect Artifacts, Presigned Urls (cam/v1) |
| `ccloud/ccl` | Custom Code Loggings (ccl/v1) |
| `ccloud/ccpm` | Custom Connect Plugin Versions/Plugins, Presigned Urls (ccpm/v1) |
| `ccloud/cdx` | Consumer/Provider Shared Resources/Shares, Opt Ins, Shared Tokens |
| `ccloud/clusterconfig` | Config (v1) |
| `ccloud/compatibility` | Compatibility (v1) |
| `ccloud/contexts` | Contexts (v1) |
| `ccloud/dek_registry` | Data/Key Encryption Keys (v1) |
| `ccloud/endpoint` | Endpoints (endpoint/v1) |
| `ccloud/exporters` | Exporters (v1) |
| `ccloud/fcpm` | Regions, Org Compute Pool Configs (fcpm/v2) |
| `ccloud/kafka_quotas` | Client Quotas (kafka-quotas/v1) |
| `ccloud/ksqldbcm` | Clusters (ksqldbcm/v2) |
| `ccloud/mode` | Modes (v1) |
| `ccloud/pim` | Integrations (pim/v1, pim/v2) |
| `ccloud/query` | Statements (query/v1alpha1) |
| `ccloud/rtce` | Rtce Topics (rtce/v1) |
| `ccloud/scim` | Users (scim/v2) |
| `ccloud/service_quota` | Applied Quotas, Scopes (service-quota/v1) |
| `ccloud/sts` | OAuth Tokens (sts/v1) |
| `ccloud/tableflow` | Tableflow Topics, Catalog Integrations (tableflow/v1) |
| `ccloud/usm` | Kafka Clusters, Connect Clusters (usm/v1) |

Packages are self-contained: each carries its own generated types, so importing
only `ccloud/iam` pulls only IAM types into your binary.
