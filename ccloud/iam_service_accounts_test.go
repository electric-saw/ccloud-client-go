package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListServiceAccounts(t *testing.T) {
	c := makeClient()
	serviceAccounts, err := c.ListServiceAccounts(&common.PaginationOptions{
		PageSize: 1,
	})
	assert.NoError(t, err)

	assert.NotNil(t, serviceAccounts)

	serviceAccount, err := c.GetServiceAccount(serviceAccounts.Data[0].Id)
	assert.NoError(t, err)

	assert.NotNil(t, serviceAccount)

}
