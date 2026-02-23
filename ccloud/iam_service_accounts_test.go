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

type noopAuthSA struct{}

func (n noopAuthSA) SetAuth(req *http.Request) error { return nil }

func TestListServiceAccounts(t *testing.T) {
	c := makeClient()
	serviceAccounts, err := c.ListServiceAccounts(&ccloud.ListServiceAccountsQuery{
		PaginationOptions: common.PaginationOptions{
			PageSize: 1,
		},
	})
	assert.NoError(t, err)

	assert.NotNil(t, serviceAccounts)

	serviceAccount, err := c.GetServiceAccount(serviceAccounts.Data[0].Id)
	assert.NoError(t, err)

	assert.NotNil(t, serviceAccount)

}

func TestListServiceAccountsWithDisplayName(t *testing.T) {
	serviceAccountsList := ccloud.ServiceAccountList{
		Data: []ccloud.ServiceAccount{
			{
				DisplayName: "tf_runner_sa",
				Description: "Test Service Account 1",
			},
			{
				DisplayName: "mySA",
				Description: "Test Service Account 2",
			},
		},
	}
	serviceAccountsList.Data[0].Id = "sa-12345"
	serviceAccountsList.Data[1].Id = "sa-67890"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := "/iam/v2/service-accounts"
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		// Verify display_name query parameters (multiple parameters with same name)
		displayNames := r.URL.Query()["display_name"]
		if len(displayNames) != 2 {
			t.Fatalf("expected 2 display_name parameters, got %d", len(displayNames))
		}
		if displayNames[0] != "tf_runner_sa" || displayNames[1] != "mySA" {
			t.Fatalf("unexpected display_name values: %v", displayNames)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(serviceAccountsList)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthSA{}).WithBaseUrl(ts.URL)

	serviceAccounts, err := client.ListServiceAccounts(&ccloud.ListServiceAccountsQuery{
		DisplayNames: []string{"tf_runner_sa", "mySA"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, serviceAccounts)
	assert.Len(t, serviceAccounts.Data, 2)
	assert.Equal(t, "tf_runner_sa", serviceAccounts.Data[0].DisplayName)
	assert.Equal(t, "mySA", serviceAccounts.Data[1].DisplayName)
}

func TestListServiceAccountsWithPagination(t *testing.T) {
	serviceAccountsList := ccloud.ServiceAccountList{
		Data: []ccloud.ServiceAccount{
			{
				DisplayName: "test-service-1",
				Description: "Test Service Account 1",
			},
		},
	}
	serviceAccountsList.Data[0].Id = "sa-12345"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := "/iam/v2/service-accounts"
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		// Verify pagination query parameters
		pageSize := r.URL.Query().Get("page_size")
		if pageSize != "10" {
			t.Fatalf("expected page_size=10, got %s", pageSize)
		}

		// Verify no display_name parameter
		displayName := r.URL.Query().Get("display_name")
		if displayName != "" {
			t.Fatalf("expected no display_name parameter, got %s", displayName)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(serviceAccountsList)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthSA{}).WithBaseUrl(ts.URL)

	serviceAccounts, err := client.ListServiceAccounts(&ccloud.ListServiceAccountsQuery{
		PaginationOptions: common.PaginationOptions{
			PageSize: 10,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, serviceAccounts)
	assert.Len(t, serviceAccounts.Data, 1)
	assert.Equal(t, "test-service-1", serviceAccounts.Data[0].DisplayName)
}
