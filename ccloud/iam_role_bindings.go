package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type RoleBindingCreateReq struct {
	Principal  string `json:"principal"`
	RoleName   string `json:"role_name"`
	CrnPattern string `json:"crn_pattern"`
}

type RoleBinding struct {
	common.BaseModel
	Principal  string `json:"principal"`
	RoleName   string `json:"role_name"`
	CrnPattern string `json:"crn_pattern"`
}

type RoleBindingList struct {
	common.BaseModel
	Data []RoleBinding `json:"data"`
}

type ListRoleBindingsQuery struct {
	common.PaginationOptions
	Principal  string `url:"principal,omitempty"`
	RoleName   string `url:"role_name,omitempty"`
	CrnPattern string `url:"crn_pattern,omitempty"`
}

func (c *ConfluentClient) ListRoleBindings(query *ListRoleBindingsQuery) (*RoleBindingList, error) {
	urlPath := "/iam/v2/role-bindings"

	res, err := c.doRequest(urlPath, http.MethodGet, nil, query)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to list role bindings: %s", res.Status)
	}

	defer res.Body.Close()

	var roleBindingList RoleBindingList
	err = json.NewDecoder(res.Body).Decode(&roleBindingList)
	if err != nil {
		return nil, err
	}

	return &roleBindingList, nil
}

func (c *ConfluentClient) GetRoleBinding(roleBindingId string) (*RoleBinding, error) {
	urlPath := fmt.Sprintf("/iam/v2/role-bindings/%s", roleBindingId)

	res, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get role binding: %s", res.Status)
	}

	defer res.Body.Close()

	var roleBinding RoleBinding
	err = json.NewDecoder(res.Body).Decode(&roleBinding)
	if err != nil {
		return nil, err
	}

	return &roleBinding, nil
}

func (c *ConfluentClient) CreateRoleBinding(req *RoleBindingCreateReq) (*RoleBinding, error) {
	urlPath := "/iam/v2/role-bindings"

	res, err := c.doRequest(urlPath, http.MethodPost, req, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != res.StatusCode {
		return nil, fmt.Errorf("failed to create role binding: %s", res.Status)
	}

	defer res.Body.Close()

	var roleBinding RoleBinding
	err = json.NewDecoder(res.Body).Decode(&roleBinding)
	if err != nil {
		return nil, err
	}

	return &roleBinding, nil
}
