# ccloud-client-go

[![Go Report Card](https://goreportcard.com/badge/github.com/electric-saw/ccloud-client-go)](https://goreportcard.com/report/github.com/electric-saw/ccloud-client-go)
[![GoDoc](https://godoc.org/github.com/electric-saw/ccloud-client-go?status.svg)](https://godoc.org/github.com/electric-saw/ccloud-client-go)
[![License](https://img.shields.io/github/license/electric-saw/ccloud-client-go)](LICENSE)

A comprehensive Go client library for Confluent Cloud API. This library enables developers to programmatically manage Confluent Cloud resources including Kafka clusters, environments, service accounts, client quotas, and more.

## Features

- **Environment Management**: Create, list, and delete Confluent Cloud environments
- **Kafka Cluster Management**: Provision and manage Kafka clusters
- **Service Account Management**: Create and manage service accounts and API keys
- **Client Quota Management**: Define and control resource quotas for clients
- **Schema Registry Integration**: Manage schemas and subjects
- **RBAC Support**: Role-based access control operations
- **Cluster Linking**: Configure and manage cluster linking
- **ACL Management**: Control access to Kafka resources

## Installation

```bash
go get github.com/electric-saw/ccloud-client-go
```

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/electric-saw/ccloud-client-go/ccloud"
)

func main() {
	// Create a client with Basic Auth
	auth := ccloud.BasicAuth{
		Username: "YOUR_API_KEY",
		Password: "YOUR_API_SECRET",
	}

	client := ccloud.NewClient().WithAuth(auth)

	// List environments
	environments, err := client.ListEnvironments(nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d environments\n", len(environments.Data))
	for _, env := range environments.Data {
		fmt.Printf("- %s (ID: %s)\n", env.DisplayName, env.Id)
	}
}
```

## Authentication

The library supports API Key authentication:

```go
// Using Basic Auth (API Key & Secret)
auth := ccloud.BasicAuth{
    Username: "YOUR_API_KEY",
    Password: "YOUR_API_SECRET",
}
client := ccloud.NewClient().WithAuth(auth)
```

## Working with Client Quotas

```go
// List client quotas
quotas, err := client.ListClientQuotas(&ccloud.ClientQuotaListOptions{
    Cluster:     "lkc-abc123",
    Environment: "env-xyz789",
})

// Get a specific client quota
quota, err := client.GetClientQuota("cq-12345")

// Create a new client quota
newQuota := &ccloud.ClientQuotaCreateReq{
    DisplayName: "My Client Quota",
    Description: "Quota for test service",
    Throughput: &ccloud.ClientQuotaThroughput{
        IngressByteRate: "1048576", // 1 MB/s
        EgressByteRate:  "1048576", // 1 MB/s
    },
    Cluster: &ccloud.ClientQuotaCluster{
        ID: "lkc-abc123",
    },
    Principals: []ccloud.ClientQuotaPrincipal{
        {ID: "sa-abc123"},
    },
    Environment: &ccloud.ClientQuotaEnvironment{
        ID: "env-xyz789",
    },
}

createdQuota, err := client.CreateClientQuota(newQuota)

// Update a client quota
updateReq := &ccloud.ClientQuotaUpdateReq{
    DisplayName: "Updated Quota Name",
    Throughput: &ccloud.ClientQuotaThroughput{
        IngressByteRate: "2097152", // 2 MB/s
    },
}

updatedQuota, err := client.UpdateClientQuota(quota.ID, updateReq)

// Delete a client quota
err = client.DeleteClientQuota(quota.ID)
```

## Additional Examples

Check the [examples directory](examples/) for more usage examples.

## Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Skip integration tests
go test -short ./...
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
