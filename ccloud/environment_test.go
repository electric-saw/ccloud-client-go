package ccloud_test

import (
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListEnvironment(t *testing.T) {
	c := makeClient()
	environments, err := c.ListEnvironments(&common.PaginationOptions{
		PageSize: 1,
	})
	assert.NoError(t, err)

	assert.NotNil(t, environments)

	environment, err := c.GetEnvironment(environments.Data[0].Id)
	assert.NoError(t, err)

	assert.NotNil(t, environment)

}
