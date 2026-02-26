package ccloud_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

type noopAuth struct{}

func (n noopAuth) SetAuth(req *http.Request) error { return nil }

func TestCreateRoleBinding(t *testing.T) {
	expectedReq := &ccloud.RoleBindingCreateReq{
		Principal:  "User:u-111aaa",
		RoleName:   "CloudClusterAdmin",
		CrnPattern: "crn://confluent.cloud/organization=1111aaaa-11aa-11aa-11aa-111111aaaaaa/environment=env-aaa1111/cloud-cluster=lkc-1111aaa",
	}

	expectedRes := &ccloud.RoleBinding{
		BaseModel: common.BaseModel{
			ApiVersion: "iam/v2",
			Kind:       "RoleBinding",
			Id:         "dlz-f3a90de",
		},
		Principal:  "User:u-111aaa",
		RoleName:   "CloudClusterAdmin",
		CrnPattern: "crn://confluent.cloud/organization=1111aaaa-11aa-11aa-11aa-111111aaaaaa/environment=env-aaa1111/cloud-cluster=lkc-1111aaa",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/iam/v2/role-bindings", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(expectedRes)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	result, err := client.CreateRoleBinding(expectedReq)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedRes.Id, result.Id)
	assert.Equal(t, expectedRes.Principal, result.Principal)
	assert.Equal(t, expectedRes.RoleName, result.RoleName)
	assert.Equal(t, expectedRes.CrnPattern, result.CrnPattern)
}

func TestCreateRoleBindingError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "invalid request"}`))
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	req := &ccloud.RoleBindingCreateReq{
		Principal:  "invalid",
		RoleName:   "CloudClusterAdmin",
		CrnPattern: "invalid-crn",
	}

	result, err := client.CreateRoleBinding(req)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to create role binding")
}
