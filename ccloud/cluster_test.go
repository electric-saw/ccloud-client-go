package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListCluster(t *testing.T) {
	c := makeClient()

	environments, err := c.ListEnvironments(&common.PaginationOptions{
		PageSize: 1})

	assert.NoError(t, err)

	clusters, err := c.ListKafkaClusters(&ccloud.KafkaClusterListOptions{
		PaginationOptions: common.PaginationOptions{
			PageSize: 1,
		},
		EnvironmentId: environments.Data[0].Id,
	})
	assert.NoError(t, err)

	assert.NotNil(t, clusters)

	cluster, err := c.GetKafkaCluster(clusters.Data[0].Id, &ccloud.KafkaClusterListOptions{
		EnvironmentId: clusters.Data[0].Spec.Environment.Id,
	})
	assert.NoError(t, err)

	assert.NotNil(t, cluster)

}
