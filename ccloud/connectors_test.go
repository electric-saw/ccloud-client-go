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

func TestListConnectorsWithExpansions(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"

	connectorsList := map[string]ConnectorWithExpansions{
		"MyGcsLogsBucketConnector": {
			Id: ConnectorId{
				Id:     "lcc-12345",
				IdType: "ID",
			},
			Info: ConnectorInfo{
				Name: "MyGcsLogsBucketConnector",
				Config: map[string]interface{}{
					"connector.class": "GcsSink",
					"gcs.bucket.name": "APILogsBucket",
					"kafka.region":    "us-west-2",
					"topics":          "APILogsTopic",
					"flush.size":      "1000",
					"time.interval":   "DAILY",
					"tasks.max":       "1",
				},
				Type: "sink",
			},
			Status: ConnectorStatus{
				Name: "MyGcsLogsBucketConnector",
				Connector: ConnectorTaskStatus{
					State:    "PROVISIONING",
					WorkerId: "MyGcsLogsBucketConnector",
					Trace:    "",
				},
				Tasks: []TaskStatus{},
			},
		},
		"MyDatagenConnector": {
			Id: ConnectorId{
				Id:     "lcc-54321",
				IdType: "ID",
			},
			Info: ConnectorInfo{
				Name: "MyDatagenConnector",
				Config: map[string]interface{}{
					"connector.class": "DatagenSource",
					"quickstart":      "ORDERS",
					"topics":          "APILogsTopic",
				},
				Type: "source",
			},
			Status: ConnectorStatus{
				Name: "MyDatagenConnector",
				Connector: ConnectorTaskStatus{
					State:    "RUNNING",
					WorkerId: "MyDatagenConnector",
					Trace:    "",
				},
				Tasks: []TaskStatus{
					{
						Id:       0,
						State:    "RUNNING",
						WorkerId: "MyDatagenConnector",
					},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		// Verify expand parameters
		expand := r.URL.Query().Get("expand")
		if expand != "id,info,status" {
			t.Fatalf("unexpected expand parameter: %s", expand)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(connectorsList)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	connectors, err := client.ListConnectorsWithExpansions(env, cluster, "id", "info", "status")
	assert.NoError(t, err)
	assert.Len(t, connectors, 2)

	// Verify GCS connector
	gcs, ok := connectors["MyGcsLogsBucketConnector"]
	assert.True(t, ok)
	assert.Equal(t, "lcc-12345", gcs.Id.Id)
	assert.Equal(t, "ID", gcs.Id.IdType)
	assert.Equal(t, "MyGcsLogsBucketConnector", gcs.Info.Name)
	assert.Equal(t, "GcsSink", gcs.Info.Config["connector.class"])
	assert.Equal(t, "sink", gcs.Info.Type)
	assert.Equal(t, "PROVISIONING", gcs.Status.Connector.State)

	// Verify Datagen connector
	datagen, ok := connectors["MyDatagenConnector"]
	assert.True(t, ok)
	assert.Equal(t, "lcc-54321", datagen.Id.Id)
	assert.Equal(t, "MyDatagenConnector", datagen.Info.Name)
	assert.Equal(t, "DatagenSource", datagen.Info.Config["connector.class"])
	assert.Equal(t, "source", datagen.Info.Type)
	assert.Equal(t, "RUNNING", datagen.Status.Connector.State)
	assert.Len(t, datagen.Status.Tasks, 1)
}

func TestGetConnectorWithExpansions(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	connectorName := "MyDatagenConnector"

	connectorsList := map[string]ConnectorWithExpansions{
		"MyGcsLogsBucketConnector": {
			Id: ConnectorId{
				Id:     "lcc-12345",
				IdType: "ID",
			},
			Info: ConnectorInfo{
				Name: "MyGcsLogsBucketConnector",
				Config: map[string]interface{}{
					"connector.class": "GcsSink",
				},
				Type: "sink",
			},
			Status: ConnectorStatus{
				Name: "MyGcsLogsBucketConnector",
				Connector: ConnectorTaskStatus{
					State:    "PROVISIONING",
					WorkerId: "MyGcsLogsBucketConnector",
				},
				Tasks: []TaskStatus{},
			},
		},
		connectorName: {
			Id: ConnectorId{
				Id:     "lcc-54321",
				IdType: "ID",
			},
			Info: ConnectorInfo{
				Name: connectorName,
				Config: map[string]interface{}{
					"connector.class": "DatagenSource",
					"quickstart":      "ORDERS",
					"topics":          "APILogsTopic",
					"tasks.max":       "1",
				},
				Type: "source",
			},
			Status: ConnectorStatus{
				Name: connectorName,
				Connector: ConnectorTaskStatus{
					State:    "RUNNING",
					WorkerId: connectorName,
					Trace:    "",
				},
				Tasks: []TaskStatus{
					{
						Id:       0,
						State:    "RUNNING",
						WorkerId: connectorName,
					},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		// GetConnectorWithExpansions now calls ListConnectorsWithExpansions internally
		expectedPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", env, cluster)
		if r.URL.Path != expectedPath {
			t.Fatalf("unexpected path: %s (expect %s)", r.URL.Path, expectedPath)
		}

		// Verify expand parameters
		expand := r.URL.Query().Get("expand")
		if expand != "id,info,status" {
			t.Fatalf("unexpected expand parameter: %s", expand)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(connectorsList)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	connector, err := client.GetConnectorWithExpansions(env, cluster, connectorName, "id", "info", "status")
	assert.NoError(t, err)
	assert.NotNil(t, connector)
	assert.Equal(t, "lcc-54321", connector.Id.Id)
	assert.Equal(t, connectorName, connector.Info.Name)
	assert.Equal(t, "DatagenSource", connector.Info.Config["connector.class"])
	assert.Equal(t, "source", connector.Info.Type)
	assert.Equal(t, "RUNNING", connector.Status.Connector.State)
	assert.Len(t, connector.Status.Tasks, 1)
}

func TestGetConnectorWithExpansionsNotFound(t *testing.T) {
	env := "env-1"
	cluster := "cluster-1"
	connectorName := "NonExistentConnector"

	connectorsList := map[string]ConnectorWithExpansions{
		"MyGcsLogsBucketConnector": {
			Id: ConnectorId{
				Id:     "lcc-12345",
				IdType: "ID",
			},
			Info: ConnectorInfo{
				Name: "MyGcsLogsBucketConnector",
				Config: map[string]interface{}{
					"connector.class": "GcsSink",
				},
				Type: "sink",
			},
			Status: ConnectorStatus{
				Name: "MyGcsLogsBucketConnector",
				Connector: ConnectorTaskStatus{
					State:    "PROVISIONING",
					WorkerId: "MyGcsLogsBucketConnector",
				},
				Tasks: []TaskStatus{},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(connectorsList)
	}))
	defer ts.Close()

	client := NewClient().WithAuth(noopAuth{}).WithBaseUrl(ts.URL)

	connector, err := client.GetConnectorWithExpansions(env, cluster, connectorName, "id", "info", "status")
	assert.Error(t, err)
	assert.Nil(t, connector)
	assert.Contains(t, err.Error(), "not found")
}
