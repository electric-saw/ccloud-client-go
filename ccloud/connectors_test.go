package ccloud

// teste da lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

type noopAuth struct{}

func (n noopAuth) SetAuth(req *http.Request) error { return nil }

func TestListConnectors(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"

	expected := ConnectorList{
		BaseModel: common.BaseModel{},
		Data: []Connector{
			{Name: "conn-1"},
			{Name: "conn-2"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		_ = json.NewEncoder(w).Encode(expected)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	got, err := client.ListConnectors(env, cluster)
	assert.NoError(t, err)
	assert.Len(t, got.Data, 2)
	assert.Equal(t, expected.Data[0].Name, got.Data[0].Name)
}

func TestGetConnectorConfig(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "conn-1"

	expectedCfg := map[string]interface{}{
		"connector.class": "S3_SINK",
		"s3.bucket.name":  "my-bucket",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expectedCfg)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	cfg, err := client.GetConnectorConfig(env, cluster, name)
	assert.NoError(t, err)
	assert.Equal(t, expectedCfg["connector.class"], cfg["connector.class"])
	assert.Equal(t, expectedCfg["s3.bucket.name"], cfg["s3.bucket.name"])
}
