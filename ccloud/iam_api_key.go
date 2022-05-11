package ccloud

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type ApiKeySpec struct {
	Description string `json:"description,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Owner common.BaseModel
	Resource common.BaseModel
	Secret string `json:"secret,omitempty"`
}

type ApiKey struct {
	common.BaseModel
	Spec ApiKeySpec `json:"spec"`
}

type ApiKeyList struct {
	common.BaseModel
	Data []ApiKey `json:"data"`
}

type ApiKeyListOptions struct {
	common.PaginationOptions
	Owner    string `url:"spec.owner,omitempty"`
	Resource string `url:"spec.resource,omitempty"`
}

type ApiKeyCommonReq struct {
	Id string `json:"id"`
	Environment string `json:"environment,omitempty"`
}

type ApiKeyUpdateReq struct {
	Spec struct {
		DisplayName string `json:"display_name,omitempty"`
		Description string `json:"description,omitempty"`
	}`json:"spec,omitempty"`
}

type ApiKeySpecReq struct {
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	Owner ApiKeyCommonReq `json:"owner"`
	Resource ApiKeyCommonReq `json:"resource,omitempty"`
} 
type ApiKeyCreateReq struct {
	Spec  ApiKeySpecReq `json:"spec"`
} 

func (c *ConfluentClient) ListApiKeys(opt *ApiKeyListOptions) (*ApiKeyList, error) {
	urlPath := "/iam/v2/api-keys"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list api keys: %s", req.Status)
	}

	defer req.Body.Close()

	var apiKeys ApiKeyList
	err = json.NewDecoder(req.Body).Decode(&apiKeys)
	if err != nil {
		return nil, err
	}

	return &apiKeys, nil
}

func (c *ConfluentClient) GetApiKey(apyKeyId string) (*ApiKey, error) {
	urlPath := fmt.Sprintf("/iam/v2/api-keys/%s", apyKeyId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get api-key: %s", req.Status)
	}

	defer req.Body.Close()

	var apiKey ApiKey
	err = json.NewDecoder(req.Body).Decode(&apiKey)
	if err != nil {
		return nil, err
	}

	return &apiKey, nil
}

func (c *ConfluentClient) CreateApiKey(create *ApiKeyCreateReq) (*ApiKey, error) {
	urlPath := "/iam/v2/api-keys"
	req, err := c.doRequest(urlPath, http.MethodPost, create, nil)
	if err != nil {
		return nil, err
	}
	
	defer req.Body.Close()

	if http.StatusAccepted != req.StatusCode {
		data, _ := ioutil.ReadAll(req.Body)
		fmt.Println(string(data))
		return nil, fmt.Errorf("failed to create api-key: %s", req.Status)
	}

	

	var ApiKey ApiKey
	err = json.NewDecoder(req.Body).Decode(&ApiKey)
	if err != nil {
		return nil, err
	}

	return &ApiKey, nil
}


func (c *ConfluentClient) DeleteApiKey(id string) (error) {
	urlPath := fmt.Sprintf("/iam/v2/api-keys/%s", id)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete api-key: %s", req.Status)
	}

	defer req.Body.Close()

	return nil
}

func (c *ConfluentClient) UpdateApiKey(apyKeyId string, update *ApiKeyUpdateReq) (*ApiKey, error) {
	urlPath := fmt.Sprintf("/iam/v2/api-keys/%s", apyKeyId)
	req, err := c.doRequest(urlPath, http.MethodPatch, update, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to update api-key: %s", req.Status)
	}

	defer req.Body.Close()

	var apiKey ApiKey
	err = json.NewDecoder(req.Body).Decode(&apiKey)
	if err != nil {
		return nil, err
	}

	return &apiKey, nil
}