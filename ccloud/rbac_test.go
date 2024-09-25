package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestGetSchemaRegistryRbac(t *testing.T) {
	c := makeClient()
	crn := makeRbacCrn()

	rbacList, err := c.ListSchemaRegistryRBAC(
		&ccloud.SchemaRegistryRbacListOptions{
			PaginationOptions: common.PaginationOptions{
				PageSize: 10,
			},
			CrnPattern: crn,
		})

	assert.NoError(t, err)
	assert.NotNil(t, rbacList)

	rbacList, err = c.ListSchemaRegistryRBAC(
		&ccloud.SchemaRegistryRbacListOptions{
			CrnPattern: rbacList.Data[0].CrnPattern,
			Principal:  rbacList.Data[0].Principal,
		})

	assert.NoError(t, err)
	assert.NotNil(t, rbacList)

}
