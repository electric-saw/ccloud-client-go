package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type noopAuth struct{}

func (n noopAuth) SetAuth(req *http.Request) error { return nil }

func TestListConnectors(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	names := []string{"conn-1", "conn-2"}

	connectors := map[string]Connector{
		"conn-1": {Name: "conn-1"},
		"conn-2": {Name: "conn-2"},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path == listPath {
			_ = json.NewEncoder(w).Encode(names)
			return
		}

		// If not the list path, it should be the get-by-name path.
		for n := range connectors {
			if r.URL.Path == fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s", env, cluster, n) {
				_ = json.NewEncoder(w).Encode(connectors[n])
				return
			}
		}

		t.Fatalf("unexpected path: %s", r.URL.Path)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	got, err := client.ListConnectors(env, cluster)
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, names[0], got[0].Name)
	assert.Equal(t, names[1], got[1].Name)
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

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.Header().Set("Content-Type", "application/json")
		// Return a connector that includes the config
		conn := Connector{
			Name:   name,
			Config: expectedCfg,
		}
		_ = json.NewEncoder(w).Encode(conn)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	conn, err := client.GetConnector(env, cluster, name)
	assert.NoError(t, err)
	assert.Equal(t, expectedCfg["connector.class"], conn.Config["connector.class"])
	assert.Equal(t, expectedCfg["s3.bucket.name"], conn.Config["s3.bucket.name"])
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
	tasksMax := "1"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request body: %s", err)
		}

		assert.Equal(t, name, payload["name"])
		config, ok := payload["config"].(map[string]interface{})
		assert.True(t, ok, "config should be a map")

		assert.Equal(t, "S3_SINK", config["connector.class"])
		assert.Equal(t, bucket, config["s3.bucket.name"])
		assert.Equal(t, providerIntegration, config["provider.integration.id"])
		assert.Equal(t, topics, config["topics"])
		assert.Equal(t, timeInterval, config["time.interval"])
		assert.Equal(t, tasksMax, config["tasks.max"])
		assert.Equal(t, name, config["name"])

		assert.Equal(t, "IAM Roles", config["authentication.method"])
		assert.Equal(t, "AVRO", config["input.data.format"])
		assert.Equal(t, "AVRO", config["output.data.format"])

		response := Connector{
			Name:   name,
			Config: config,
		}
		response.Id = "ctr-s3-123"

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	s3Config := &S3SinkConnectorConfig{
		Bucket:                bucket,
		ProviderIntegrationId: providerIntegration,
		Topics:                topics,
		FlushSize:             "1000",
		PartitionerClass:      "TimeBasedPartitioner",
		TimeInterval:          timeInterval,
		TasksMax:              tasksMax,
	}

	created, err := client.CreateConnector(env, cluster, name, s3Config)
	assert.NoError(t, err)
	assert.Equal(t, name, created.Name)
	assert.Equal(t, "ctr-s3-123", created.Id)
	assert.Equal(t, "S3_SINK", created.Config["connector.class"])
	assert.Equal(t, bucket, created.Config["s3.bucket.name"])
	assert.Equal(t, "IAM Roles", created.Config["authentication.method"])
	assert.Equal(t, "AVRO", created.Config["input.data.format"])
}

func TestUpdateConnectorConfig(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "conn-update"

	newCfg := map[string]interface{}{
		"flush.size": "500",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request body: %s", err)
		}

		assert.Equal(t, name, payload["name"])
		assert.Equal(t, newCfg["flush.size"], payload["flush.size"])

		response := Connector{
			Name:   name,
			Config: payload,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	updated, err := client.UpdateConnectorConfig(env, cluster, name, newCfg)
	assert.NoError(t, err)
	assert.Equal(t, name, updated.Name)
	assert.Equal(t, newCfg["flush.size"], updated.Config["flush.size"])
}

func TestUpdateConnectorTyped(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "conn-update-typed"

	s3Config := &S3SinkConnectorConfig{
		Bucket:                "my-bucket",
		ProviderIntegrationId: "provider",
		Topics:                "topicos",
		TasksMax:              "2",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request body: %s", err)
		}

		assert.Equal(t, name, payload["name"])
		assert.Equal(t, s3Config.Bucket, payload["s3.bucket.name"])
		assert.Equal(t, s3Config.TasksMax, payload["tasks.max"])
		assert.Equal(t, "S3_SINK", payload["connector.class"])
		assert.Equal(t, "IAM Roles", payload["authentication.method"])

		response := Connector{
			Name:   name,
			Config: payload,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	updated, err := client.UpdateConnectorConfig(env, cluster, name, s3Config)
	assert.NoError(t, err)
	assert.Equal(t, name, updated.Name)
	assert.Equal(t, s3Config.Bucket, updated.Config["s3.bucket.name"])
	assert.Equal(t, "S3_SINK", updated.Config["connector.class"])
}

func TestGetConnectorStatus(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	expectedStatus := ConnectorStatus{
		Name: name,
		Connector: ConnectorTaskStatus{
			State:    "RUNNING",
			WorkerId: "worker-1",
		},
		Tasks: []TaskStatus{
			{
				Id:       0,
				State:    "RUNNING",
				WorkerId: "worker-1",
			},
			{
				Id:       1,
				State:    "RUNNING",
				WorkerId: "worker-1",
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/status", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expectedStatus)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	status, err := client.GetConnectorStatus(env, cluster, name)
	assert.NoError(t, err)
	assert.Equal(t, name, status.Name)
	assert.Equal(t, "RUNNING", status.Connector.State)
	assert.Equal(t, "worker-1", status.Connector.WorkerId)
	assert.Len(t, status.Tasks, 2)
	assert.Equal(t, "RUNNING", status.Tasks[0].State)
	assert.Equal(t, "RUNNING", status.Tasks[1].State)
}

func TestPauseConnector(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/pause", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.PauseConnector(env, cluster, name)
	assert.NoError(t, err)
}

func TestPauseConnectorAccepted(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/pause", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.PauseConnector(env, cluster, name)
	assert.NoError(t, err)
}

func TestResumeConnector(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/resume", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.ResumeConnector(env, cluster, name)
	assert.NoError(t, err)
}

func TestResumeConnectorAccepted(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/resume", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.ResumeConnector(env, cluster, name)
	assert.NoError(t, err)
}

func TestRestartConnector(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/restart", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.RestartConnector(env, cluster, name)
	assert.NoError(t, err)
}

func TestRestartConnectorAccepted(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	name := "test-connector"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/restart", env, cluster, name)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	err := client.RestartConnector(env, cluster, name)
	assert.NoError(t, err)
}
