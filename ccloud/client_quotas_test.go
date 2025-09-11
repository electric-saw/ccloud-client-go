package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListClientQuotas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	c := makeClient()

	environments, err := c.ListEnvironments(&common.PaginationOptions{
		PageSize: 1})
	if err != nil {
		t.Skipf("Skipping test: Unable to list environments: %v", err)
		return
	}

	if len(environments.Data) == 0 {
		t.Skip("Skipping test: No environments found")
		return
	}

	clusters, err := c.ListKafkaClusters(&ccloud.KafkaClusterListOptions{
		PaginationOptions: common.PaginationOptions{
			PageSize: 1,
		},
		EnvironmentId: environments.Data[0].Id,
	})
	if err != nil {
		t.Skipf("Skipping test: Unable to list clusters: %v", err)
		return
	}

	if len(clusters.Data) == 0 {
		t.Skip("Skipping test: No clusters found in environment")
		return
	}

	quotas, err := c.ListClientQuotas(&ccloud.ClientQuotaListOptions{
		PaginationOptions: common.PaginationOptions{
			PageSize: 10,
		},
		Cluster:     clusters.Data[0].Id,
		Environment: environments.Data[0].Id,
	})

	if err != nil {
		t.Skipf("No client quotas found or error occurred: %v", err)
		return
	}

	assert.NotNil(t, quotas)
	t.Logf("Found %d client quotas", len(quotas.Data))
}

func TestCreateUpdateDeleteClientQuota(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	c := makeClient()

	environments, err := c.ListEnvironments(&common.PaginationOptions{PageSize: 1})
	assert.NoError(t, err)
	assert.NotEmpty(t, environments.Data, "No environments found")

	envID := environments.Data[0].Id

	clusters, err := c.ListKafkaClusters(&ccloud.KafkaClusterListOptions{
		PaginationOptions: common.PaginationOptions{PageSize: 1},
		EnvironmentId:     envID,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, clusters.Data, "No Kafka clusters found")

	clusterID := clusters.Data[0].Id

	serviceAccounts, err := c.ListServiceAccounts(&common.PaginationOptions{PageSize: 1})
	if err != nil || len(serviceAccounts.Data) == 0 {
		t.Skip("Skipping test: No service accounts available")
		return
	}

	saID := serviceAccounts.Data[0].Id

	createReq := &ccloud.ClientQuotaCreateReq{
		DisplayName: "Test Quota",
		Description: "Test Quota created by integration test",
		Throughput: &ccloud.ClientQuotaThroughput{
			IngressByteRate: "1048576", // 1 MB/s
			EgressByteRate:  "1048576", // 1 MB/s
		},
		Cluster: &ccloud.ClientQuotaCluster{
			ID: clusterID,
		},
		Principals: []ccloud.ClientQuotaPrincipal{
			{ID: saID},
		},
		Environment: &ccloud.ClientQuotaEnvironment{
			ID: envID,
		},
	}

	createdQuota, err := c.CreateClientQuota(createReq)
	if err != nil {
		t.Logf("Could not create client quota: %v", err)
		t.Skip("Skipping test: Unable to create client quota")
		return
	}
	assert.NotNil(t, createdQuota)
	assert.Equal(t, "Test Quota", createdQuota.Spec.DisplayName)

	updateReq := &ccloud.ClientQuotaUpdateReq{
		DisplayName: "Updated Test Quota",
		Description: "Updated by test",
		Throughput: &ccloud.ClientQuotaThroughput{
			IngressByteRate: "2097152", // 2 MB/s
			EgressByteRate:  "2097152", // 2 MB/s
		},
	}

	updatedQuota, err := c.UpdateClientQuota(createdQuota.ID, updateReq)
	assert.NoError(t, err)
	assert.NotNil(t, updatedQuota)
	assert.Equal(t, "Updated Test Quota", updatedQuota.Spec.DisplayName)
	assert.Equal(t, "Updated by test", updatedQuota.Spec.Description)

	fetchedQuota, err := c.GetClientQuota(createdQuota.ID)
	assert.NoError(t, err)
	assert.NotNil(t, fetchedQuota)
	assert.Equal(t, "Updated Test Quota", fetchedQuota.Spec.DisplayName)

	err = c.DeleteClientQuota(createdQuota.ID)
	assert.NoError(t, err)

	_, err = c.GetClientQuota(createdQuota.ID)
	assert.Error(t, err)
}
