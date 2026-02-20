package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type ServiceAccount struct {
	common.BaseModel
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
}

type ServiceAccountList struct {
	common.BaseModel
	Data []ServiceAccount `json:"data"`
}

func (c *ConfluentClient) ListServiceAccounts(pagination *common.PaginationOptions, displayNames ...string) (*ServiceAccountList, error) {
	urlPath := "/iam/v2/service-accounts"

	type listServiceAccountsParams struct {
		PageSize     int      `url:"page_size,omitempty"`
		PageToken    string   `url:"page_token,omitempty"`
		DisplayNames []string `url:"display_name"`
	}

	params := &listServiceAccountsParams{}
	if pagination != nil {
		params.PageSize = pagination.PageSize
		params.PageToken = pagination.PageToken
	}
	params.DisplayNames = displayNames

	req, err := c.doRequest(urlPath, http.MethodGet, nil, params)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list service-accounts: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccounts ServiceAccountList
	err = json.NewDecoder(req.Body).Decode(&serviceAccounts)
	if err != nil {
		return nil, err
	}

	return &serviceAccounts, nil
}

func (c *ConfluentClient) GetServiceAccount(serviceAccountId string) (*ServiceAccount, error) {
	urlPath := fmt.Sprintf("/iam/v2/service-accounts/%s", serviceAccountId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get service-account: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccount ServiceAccount
	err = json.NewDecoder(req.Body).Decode(&serviceAccount)
	if err != nil {
		return nil, err
	}

	return &serviceAccount, nil
}

type ServiceAccountCreateReq struct {
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
}

func (c *ConfluentClient) CreateServiceAccount(create *ServiceAccountCreateReq) (*ServiceAccount, error) {
	urlPath := "/iam/v2/service-accounts"
	req, err := c.doRequest(urlPath, http.MethodPost, create, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != req.StatusCode {
		return nil, fmt.Errorf("failed to create service account: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccount ServiceAccount
	err = json.NewDecoder(req.Body).Decode(&serviceAccount)
	if err != nil {
		return nil, err
	}

	return &serviceAccount, nil
}

type ServiceAccountUpdateReq struct {
	FullName string `json:"full_name"`
}

func (c *ConfluentClient) UpdateServiceAccount(serviceAccountId string, update *ServiceAccountUpdateReq) (*ServiceAccount, error) {
	urlPath := fmt.Sprintf("/iam/v2/service-accounts/%s", serviceAccountId)
	req, err := c.doRequest(urlPath, http.MethodPatch, update, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get service-account: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccount ServiceAccount
	err = json.NewDecoder(req.Body).Decode(&serviceAccount)
	if err != nil {
		return nil, err
	}

	return &serviceAccount, nil
}

func (c *ConfluentClient) DeleteServiceAccount(serviceAccountId string) error {
	urlPath := fmt.Sprintf("/iam/v2/service-accounts/%s", serviceAccountId)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete service-account: %s", req.Status)
	}

	return nil
}
