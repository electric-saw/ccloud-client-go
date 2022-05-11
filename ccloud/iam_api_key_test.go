package ccloud_test

import (
	"fmt"
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

func TestListApiKeys(t *testing.T) {
	c := makeClient()
	pageToken := ""

	result := []ccloud.ApiKey{}
	for {

		apiKeys, err := c.ListApiKeys(
			&ccloud.ApiKeyListOptions{
				PaginationOptions: common.PaginationOptions{
					PageSize: 100,
					PageToken: pageToken,
				},
			})

		result = append(result, apiKeys.Data...)

		pageToken = apiKeys.GetPageNextToken()

		if pageToken == "" {
			break
		}

		assert.NoError(t, err)
	}

	apiKey, err := c.GetApiKey(result[0].Id)
	assert.NoError(t, err)

	assert.NotNil(t, apiKey)

	specificApiKeys, errSpec := c.ListApiKeys(
		&ccloud.ApiKeyListOptions{
			PaginationOptions: common.PaginationOptions{
				PageSize: 1,
			},
			Owner:    apiKey.Spec.Owner.Id,
			Resource: apiKey.Spec.Resource.Id,
		})

	assert.NoError(t, errSpec)

	assert.NotNil(t, specificApiKeys)

	specificApiKey, errSpec := c.GetApiKey(specificApiKeys.Data[0].Id)
	assert.NoError(t, errSpec)

	assert.NotNil(t, specificApiKey)
	assert.Equal(t, apiKey.Id, specificApiKey.Id)

}

func TestCreateUpdateDeleteApiKey(t *testing.T) {
	c := makeClient()
	apiKeys, err := c.ListApiKeys(
		&ccloud.ApiKeyListOptions{
			PaginationOptions: common.PaginationOptions{
				PageSize: 1,
			},
		})

	assert.NoError(t, err)
	assert.NotNil(t, apiKeys)

	apiKey := apiKeys.Data[0]

	apiCreated, err := c.CreateApiKey(&ccloud.ApiKeyCreateReq{
		DisplayName: "test-api-key",
		Description: "test-api-key description",
		Owner: ccloud.ApiKeyCommonReq{
			Id: apiKey.Spec.Owner.Id,
		},
		Resource:  ccloud.ApiKeyCommonReq{
			Id: apiKey.Spec.Resource.Id,
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, apiCreated)

	if apiCreated != nil {
		fmt.Println(apiCreated.Id)
		var update ccloud.ApiKeyUpdateReq
		update.DisplayName="test"
		update.Description="test description"

		apiKeyUpdated, errUpdate  := c.UpdateApiKey(
			apiCreated.Id,
			&update,
		)
		assert.NoError(t, errUpdate)
		assert.NotNil(t, apiKeyUpdated)
		if apiKeyUpdated != nil {
			assert.Equal(t, apiKeyUpdated.Spec.DisplayName, "test")
			assert.Equal(t, apiKeyUpdated.Spec.Description, "test description")
		}
		
		err = c.DeleteApiKey(apiKeyUpdated.Id)
		assert.NoError(t, err)
	}
}
