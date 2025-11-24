package ccloud

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

func TestCreateConnector(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	cfg := map[string]interface{}{
		"connector.class": "S3_SINK",
		"s3.bucket.name":  "test-bucket",
	}

	expectedResponse := Connector{
		Name:   name,
		Config: cfg,
	}
	expectedResponse.Id = "ctr-123"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(expectedResponse)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	created, err := client.CreateConnector(env, cluster, name, cfg)
	assert.NoError(t, err)
	assert.Equal(t, name, created.Name)
	assert.Equal(t, "ctr-123", created.Id)
	assert.Equal(t, cfg["connector.class"], created.Config["connector.class"])
}

func TestCreateS3SinkConnector(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "order-v4-data.status-with-orders"
	bucket := "access-point"
	providerIntegration := "provaider"
	topics := "topicos"
	timeInterval := "HOURLY"
	tasksMax := 1

	transformsName := "insert"
	transformsInsertType := "org.apache.kafka.connect.transforms.InsertField$Value"
	transformsInsertPartitionField := "PartitionField"
	transformsInsertStaticField := "InsertedStaticField"
	transformsInsertStaticValue := "SomeValue"
	transformsInsertTimestampField := "event_timestamp"
	transformsInsertTopicField := "topic"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		var req ConnectorCreateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request body: %s", err)
		}

		// Validar payload
		assert.Equal(t, name, req.Name)
		assert.Equal(t, "S3_SINK", req.Config["connector.class"])
		assert.Equal(t, bucket, req.Config["s3.bucket.name"])
		assert.Equal(t, providerIntegration, req.Config["provider.integration.id"])
		assert.Equal(t, topics, req.Config["topics"])
		assert.Equal(t, timeInterval, req.Config["time.interval"])
		assert.Equal(t, "1", req.Config["tasks.max"])
		assert.Equal(t, transformsName, req.Config["transforms"])
		assert.Equal(t, transformsInsertType, req.Config[fmt.Sprintf("transforms.%s.type", transformsName)])

		response := Connector{
			Name:   name,
			Config: req.Config,
		}
		response.Id = "ctr-s3-123"

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	created, err := client.CreateS3SinkConnector(
		env, cluster, name, bucket, providerIntegration, topics,
		transformsName, transformsInsertType, transformsInsertPartitionField,
		transformsInsertStaticField, transformsInsertStaticValue,
		transformsInsertTimestampField, transformsInsertTopicField,
		timeInterval,
		tasksMax,
	)

	assert.NoError(t, err)
	assert.Equal(t, name, created.Name)
	assert.Equal(t, "ctr-s3-123", created.Id)
	assert.Equal(t, "S3_SINK", created.Config["connector.class"])
	assert.Equal(t, bucket, created.Config["s3.bucket.name"])
}