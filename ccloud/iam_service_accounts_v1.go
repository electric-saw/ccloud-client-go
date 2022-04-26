package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type V1ServiceAccount struct {
	Id         *int32  `json:"id,omitempty"`
	ResourceId *string `json:"resource_id,omitempty"`
}

type V1ServiceAccountList struct {
	Users    []V1ServiceAccount `json:"users"`
	PageInfo V1QueryOpts        `json:"page_info"`
}

type V1QueryOpts struct {
	PageSize  int32  `url:"page_size,omitempty" json:"page_size,omitempty"`
	PageToken string `url:"page_token,omitempty" json:"page_token,omitempty"`
}

func (c *ConfluentClient) V1ListServiceAccounts(opt *V1QueryOpts) (*V1ServiceAccountList, error) {
	urlPath := "/service_accounts"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list service-accounts: %s", req.Status)
	}

	defer req.Body.Close()

	var serviceAccounts V1ServiceAccountList
	err = json.NewDecoder(req.Body).Decode(&serviceAccounts)
	if err != nil {
		return nil, err
	}

	return &serviceAccounts, nil
}

func (s *V1ServiceAccount) HasId() bool {
	return s.Id != nil
}
