package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type Rbac struct {
	common.BaseModel
	Principal string `json:"principal"`
	RoleName  string `json:"role_name"`
	CrnPattern string `json:"crn_pattern"`
}

type RbacList struct {
	common.BaseModel
	Data []Rbac `json:"data"`
}

type SchemaRegistryRbacListOptions struct {
	common.PaginationOptions
	RoleName              string `url:"role_name,omitempty"`
	CrnPattern            string `url:"crn_pattern,omitempty"`
	Principal             string `url:"principal,omitempty"`
}

func (c *ConfluentClient) ListSchemaRegistryRBAC(opt *SchemaRegistryRbacListOptions) (*RbacList, error) {
	urlPath := "/iam/v2/role-bindings"
     
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get schema registry rbac: %s", req.Status)
	}

	defer req.Body.Close()

	var rbacList RbacList
	err = json.NewDecoder(req.Body).Decode(&rbacList)
	if err != nil {
		return nil, err
	}

	return &rbacList, nil
}