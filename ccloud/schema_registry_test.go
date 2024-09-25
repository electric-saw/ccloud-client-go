package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListSr(t *testing.T) {
	c := makeClient()

	environments, err := c.ListEnvironments(&common.PaginationOptions{
		PageSize: 1})

	assert.NoError(t, err)

	clusters, err := c.ListSchemaRegistry(&ccloud.SchemaRegistryClusterListOptions{
		PaginationOptions: common.PaginationOptions{
			PageSize: 1,
		},
		EnvironmentId: environments.Data[0].Id,
	})
	assert.NoError(t, err)

	assert.NotNil(t, clusters)

	cluster, err := c.GetSchemaRegistry(clusters.Data[0].Id, &ccloud.SchemaRegistryClusterListOptions{
		EnvironmentId: clusters.Data[0].Spec.Environment.Id,
	})
	assert.NoError(t, err)

	assert.NotNil(t, cluster)

}
