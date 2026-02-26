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
