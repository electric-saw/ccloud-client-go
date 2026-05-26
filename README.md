# ccloud-client-go

[![Go Report Card](https://goreportcard.com/badge/github.com/electric-saw/ccloud-client-go)](https://goreportcard.com/report/github.com/electric-saw/ccloud-client-go)
[![GoDoc](https://godoc.org/github.com/electric-saw/ccloud-client-go?status.svg)](https://godoc.org/github.com/electric-saw/ccloud-client-go)
[![License](https://img.shields.io/github/license/electric-saw/ccloud-client-go)](LICENSE)

A comprehensive Go client library for Confluent Cloud API. This library enables developers to programmatically manage Confluent Cloud resources including Kafka clusters, environments, service accounts, client quotas, connectors, and more.

## Features

- **Environment Management**: Create, list, and delete Confluent Cloud environments
- **Kafka Cluster Management**: Provision and manage Kafka clusters
- **Service Account Management**: Create and manage service accounts and API keys
- **Client Quota Management**: Define and control resource quotas for clients
- **Connector Management**: Create, configure, monitor, and manage Kafka connectors with lifecycle control
- **Schema Registry Integration**: Manage schemas and subjects
- **RBAC Support**: Role-based access control operations (create, list, get, delete role bindings)
- **Cluster Linking**: Configure and manage cluster linking
- **ACL Management**: Control access to Kafka resources
- **Topic Management**: Create, update, and delete topics including partition count updates

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

## Working with Connectors

### Creating a Connector

```go
// Create an S3 Sink Connector
s3Config := &ccloud.S3SinkConnectorConfig{
    ConnectorClass:        "S3_SINK",
    Bucket:                "my-s3-bucket",
    AuthenticationMethod:  "IAM Roles",
    ProviderIntegrationId: "pi-12345",
    Topics:                "my-topic",
    InputDataFormat:       "AVRO",
    OutputDataFormat:      "AVRO",
    TasksMax:              "2",
}

connector, err := client.CreateConnector("env-xyz789", "lkc-abc123", "my-s3-connector", s3Config)
if err != nil {
    panic(err)
}

fmt.Printf("Connector created: %s\n", connector.Name)
```

### Managing Connector Lifecycle

```go
// Get connector details
connector, err := client.GetConnector("env-xyz789", "lkc-abc123", "my-s3-connector")

// Get connector status
status, err := client.GetConnectorStatus("env-xyz789", "lkc-abc123", "my-s3-connector")
fmt.Printf("Connector status: %s\n", status.Connector.State)

// Pause a connector
err = client.PauseConnector("env-xyz789", "lkc-abc123", "my-s3-connector")

// Resume a connector
err = client.ResumeConnector("env-xyz789", "lkc-abc123", "my-s3-connector")

// Restart a connector
err = client.RestartConnector("env-xyz789", "lkc-abc123", "my-s3-connector")

// List all connectors
connectors, err := client.ListConnectors("env-xyz789", "lkc-abc123")

// Update connector configuration
newConfig := &ccloud.S3SinkConnectorConfig{
    TasksMax: "4",
}

updatedConnector, err := client.UpdateConnectorConfig("env-xyz789", "lkc-abc123", "my-s3-connector", newConfig)

// Delete a connector
err = client.DeleteConnector("env-xyz789", "lkc-abc123", "my-s3-connector")
```

### Understanding Connector Status

The connector status provides detailed information about the connector and its tasks:

```go
status, err := client.GetConnectorStatus("env-xyz789", "lkc-abc123", "my-s3-connector")

// Check connector state
if status.Connector.State == "RUNNING" {
    fmt.Println("Connector is running")
}

// Check individual task status
for _, task := range status.Tasks {
    fmt.Printf("Task %d: %s\n", task.Id, task.State)
}
```

## Working with Role Bindings

```go
// List role bindings
roleBindings, err := client.ListRoleBindings(&ccloud.ListRoleBindingsQuery{
    Principal:  "User:u-111aaa",
    RoleName:   "CloudClusterAdmin",
    CrnPattern: "crn://confluent.cloud/organization=org-id",
})

// Get a specific role binding
roleBinding, err := client.GetRoleBinding("rb-12345")

// Create a role binding
newRoleBinding := &ccloud.RoleBindingCreateReq{
    Principal:  "User:u-111aaa",
    RoleName:   "CloudClusterAdmin",
    CrnPattern: "crn://confluent.cloud/organization=org-id/environment=env-id/cloud-cluster=lkc-id",
}
createdRoleBinding, err := client.CreateRoleBinding(newRoleBinding)

// Delete a role binding
err = client.DeleteRoleBinding("rb-12345")
```

## Working with Topics (Cluster Client)

```go
// Get a cluster client
clusterClient, err := client.GetClusterClient("lkc-abc123", "env-xyz789")

// List topics
topics, err := clusterClient.ListTopics(nil)

// Create a topic
newTopic := &cluster.TopicCreateReq{
    TopicName:         "my-topic",
    PartitionCount:    6,
    ReplicationFactor: 3,
}
topic, err := clusterClient.CreateTopic(newTopic)

// Update topic partition count (increase only)
updatedTopic, err := clusterClient.UpdateTopicPartitions("my-topic", 12)

// Delete a topic
err = clusterClient.DeleteTopic("my-topic")
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
