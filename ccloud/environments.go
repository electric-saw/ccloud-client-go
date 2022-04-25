package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type Environment struct {
	common.BaseModel
	DisplayName string `json:"display_name"`
}

type EnvironmentList struct {
	common.BaseModel
	Data []Environment `json:"data"`
}

func (c *ConfluentClient) ListEnvironments(opt *common.PaginationOptions) (*EnvironmentList, error) {
	urlPath := "/org/v2/environments"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list environments: %s", req.Status)
	}

	defer req.Body.Close()

	var environments EnvironmentList
	err = json.NewDecoder(req.Body).Decode(&environments)
	if err != nil {
		return nil, err
	}

	return &environments, nil
}

func (c *ConfluentClient) GetEnvironment(environmentId string) (*Environment, error) {
	urlPath := fmt.Sprintf("/org/v2/environments/%s", environmentId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get environment: %s", req.Status)
	}

	defer req.Body.Close()

	var environment Environment
	err = json.NewDecoder(req.Body).Decode(&environment)
	if err != nil {
		return nil, err
	}

	return &environment, nil
}

type EnvironmentCreateReq struct {
	DisplayName string `json:"display_name"`
}

func (c *ConfluentClient) CreateEnvironment(create *EnvironmentCreateReq) (*ServiceAccount, error) {
	urlPath := "/org/v2/environments"
	req, err := c.doRequest(urlPath, http.MethodPost, create, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != req.StatusCode {
		return nil, fmt.Errorf("failed to create environment: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccount ServiceAccount
	err = json.NewDecoder(req.Body).Decode(&serviceAccount)
	if err != nil {
		return nil, err
	}

	return &serviceAccount, nil
}

type EnvironmentUpdateReq struct {
	DisplayName string `json:"display_name"`
}

func (c *ConfluentClient) UpdateEnvironment(environmentId string, update *EnvironmentUpdateReq) (*Environment, error) {
	urlPath := fmt.Sprintf("/org/v2/environments/%s", environmentId)
	req, err := c.doRequest(urlPath, http.MethodPatch, update, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get environment: %s", req.Status)
	}

	defer req.Body.Close()

	var environment Environment
	err = json.NewDecoder(req.Body).Decode(&environment)
	if err != nil {
		return nil, err
	}

	return &environment, nil
}

func (c *ConfluentClient) DeleteEnvironment(environmentId string) error {
	urlPath := fmt.Sprintf("/org/v2/environments/%s", environmentId)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete environment: %s", req.Status)
	}

	return nil
}
