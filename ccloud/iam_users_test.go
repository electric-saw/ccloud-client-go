package ccloud_test

import (
	"fmt"
	"os"

	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func makeClient() *ccloud.ConfluentClient {
	key := os.Getenv("CONFLUENT_API_KEY")
	secret := os.Getenv("CONFLUENT_API_SECRET")

	return ccloud.NewClient().WithAuth(ccloud.NewBasicAuth(key, secret))
}

func makeRbacCrn() string {
	schemaRegistryCluster := os.Getenv("SCHEMA_REGISTRY_CLUSTER")
	organization := os.Getenv("ORGANIZATION")
	environment := os.Getenv("ENVIRONMENT")

	return fmt.Sprintf("crn://confluent.cloud/organization=%s/environment=%s/schema-registry=%s/subject=*", organization, environment, schemaRegistryCluster)
}

func TestListRoles(t *testing.T) {
	c := makeClient()
	users, err := c.ListUsers(&common.PaginationOptions{
		PageSize: 1,
	})
	assert.NoError(t, err)

	assert.NotNil(t, users)

	user, err := c.GetUser(users.Data[0].Id)
	assert.NoError(t, err)

	assert.NotNil(t, user)

}
