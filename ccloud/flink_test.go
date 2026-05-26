package ccloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestCreateFlinkComputePool tests the CreateFlinkComputePool function
func TestCreateFlinkComputePool(t *testing.T) {
	tests := []struct {
		name           string
		environmentID  string
		displayName    string
		maxCFU         int
		cloud          string
		region         string
		mockStatusCode int
		mockResponse   *FlinkComputePool
		wantErr        bool
		errContains    string
	}{
		{
			name:           "successful creation",
			environmentID:  "env-123",
			displayName:    "test-pool",
			maxCFU:         5,
			cloud:          "AWS",
			region:         "us-east-1",
			mockStatusCode: http.StatusCreated,
			mockResponse: &FlinkComputePool{
				DisplayName: "test-pool",
				MaxCFU:      5,
				Cloud:       "AWS",
				Region:      "us-east-1",
				Status:      ComputePoolStatusProvisioning,
			},
			wantErr: false,
		},
		{
			name:          "empty environment ID",
			environmentID: "",
			displayName:   "test-pool",
			maxCFU:        5,
			cloud:         "AWS",
			region:        "us-east-1",
			wantErr:       true,
			errContains:   "environmentID cannot be empty",
		},
		{
			name:          "empty display name",
			environmentID: "env-123",
			displayName:   "",
			maxCFU:        5,
			cloud:         "AWS",
			region:        "us-east-1",
			wantErr:       true,
			errContains:   "displayName cannot be empty",
		},
		{
			name:          "invalid maxCFU - zero",
			environmentID: "env-123",
			displayName:   "test-pool",
			maxCFU:        0,
			cloud:         "AWS",
			region:        "us-east-1",
			wantErr:       true,
			errContains:   "maxCFU must be greater than 0",
		},
		{
			name:          "invalid maxCFU - negative",
			environmentID: "env-123",
			displayName:   "test-pool",
			maxCFU:        -5,
			cloud:         "AWS",
			region:        "us-east-1",
			wantErr:       true,
			errContains:   "maxCFU must be greater than 0",
		},
		{
			name:          "empty cloud",
			environmentID: "env-123",
			displayName:   "test-pool",
			maxCFU:        5,
			cloud:         "",
			region:        "us-east-1",
			wantErr:       true,
			errContains:   "cloud cannot be empty",
		},
		{
			name:          "empty region",
			environmentID: "env-123",
			displayName:   "test-pool",
			maxCFU:        5,
			cloud:         "AWS",
			region:        "",
			wantErr:       true,
			errContains:   "region cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock server only for non-validation tests
			if tt.mockStatusCode != 0 {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Verify request method and path
					if r.Method != http.MethodPost {
						t.Errorf("expected POST, got %s", r.Method)
					}

					// Return mock response
					w.WriteHeader(tt.mockStatusCode)
					if tt.mockResponse != nil {
						_ = json.NewEncoder(w).Encode(tt.mockResponse)
					}
				}))
				defer server.Close()

				client := NewClient().
					WithBaseUrl(server.URL).
					WithAuth(BasicAuth{Username: "test", Password: "test"})

				// Execute
				pool, err := client.CreateFlinkComputePool(
					tt.environmentID,
					tt.displayName,
					tt.maxCFU,
					tt.cloud,
					tt.region,
				)

				// Assert
				if tt.wantErr {
					if err == nil {
						t.Errorf("expected error, got nil")
					}
					if pool != nil {
						t.Errorf("expected nil pool, got %v", pool)
					}
				} else {
					if err != nil {
						t.Errorf("expected no error, got %v", err)
					}
					if pool == nil {
						t.Error("expected pool, got nil")
					} else {
						if pool.DisplayName != tt.mockResponse.DisplayName {
							t.Errorf("expected DisplayName %s, got %s", tt.mockResponse.DisplayName, pool.DisplayName)
						}
						if pool.MaxCFU != tt.mockResponse.MaxCFU {
							t.Errorf("expected MaxCFU %d, got %d", tt.mockResponse.MaxCFU, pool.MaxCFU)
						}
					}
				}
			} else {
				// For validation errors, we don't need a server
				client := NewClient().WithAuth(BasicAuth{Username: "test", Password: "test"})
				pool, err := client.CreateFlinkComputePool(
					tt.environmentID,
					tt.displayName,
					tt.maxCFU,
					tt.cloud,
					tt.region,
				)

				if !tt.wantErr {
					t.Errorf("expected no error, got %v", err)
				}
				if err == nil {
					t.Error("expected error, got nil")
				}
				if pool != nil {
					t.Errorf("expected nil pool, got %v", pool)
				}
			}
		})
	}
}

// TestGetFlinkComputePool tests the GetFlinkComputePool function
func TestGetFlinkComputePool(t *testing.T) {
	tests := []struct {
		name           string
		environmentID  string
		computePoolID  string
		mockStatusCode int
		mockResponse   *FlinkComputePool
		wantErr        bool
	}{
		{
			name:           "successful get",
			environmentID:  "env-123",
			computePoolID:  "pool-456",
			mockStatusCode: http.StatusOK,
			mockResponse: &FlinkComputePool{
				DisplayName: "test-pool",
				MaxCFU:      5,
				Status:      ComputePoolStatusAvailable,
			},
			wantErr: false,
		},
		{
			name:          "empty environment ID",
			environmentID: "",
			computePoolID: "pool-456",
			wantErr:       true,
		},
		{
			name:          "empty pool ID",
			environmentID: "env-123",
			computePoolID: "",
			wantErr:       true,
		},
		{
			name:           "pool not found",
			environmentID:  "env-123",
			computePoolID:  "pool-456",
			mockStatusCode: http.StatusNotFound,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr || tt.mockStatusCode != 0 {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tt.mockStatusCode)
					if tt.mockResponse != nil {
						_ = json.NewEncoder(w).Encode(tt.mockResponse)
					}
				}))
				defer server.Close()

				client := NewClient().
					WithBaseUrl(server.URL).
					WithAuth(BasicAuth{Username: "test", Password: "test"})
				pool, err := client.GetFlinkComputePool(tt.environmentID, tt.computePoolID)

				if tt.wantErr {
					if err == nil {
						t.Error("expected error, got nil")
					}
				} else {
					if err != nil {
						t.Errorf("expected no error, got %v", err)
					}
					if pool.DisplayName != tt.mockResponse.DisplayName {
						t.Errorf("expected DisplayName %s, got %s", tt.mockResponse.DisplayName, pool.DisplayName)
					}
				}
			} else {
				client := NewClient().WithAuth(BasicAuth{Username: "test", Password: "test"})
				_, err := client.GetFlinkComputePool(tt.environmentID, tt.computePoolID)
				if err == nil {
					t.Error("expected error, got nil")
				}
			}
		})
	}
}

// TestListFlinkComputePools tests the ListFlinkComputePools function
func TestListFlinkComputePools(t *testing.T) {
	tests := []struct {
		name          string
		environmentID string
		mockPools     []FlinkComputePool
		wantErr       bool
	}{
		{
			name:          "successful list",
			environmentID: "env-123",
			mockPools: []FlinkComputePool{
				{DisplayName: "pool-1", MaxCFU: 5},
				{DisplayName: "pool-2", MaxCFU: 10},
			},
			wantErr: false,
		},
		{
			name:          "empty list",
			environmentID: "env-123",
			mockPools:     []FlinkComputePool{},
			wantErr:       false,
		},
		{
			name:          "empty environment ID",
			environmentID: "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.environmentID != "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					response := FlinkComputePoolList{Data: tt.mockPools}
					_ = json.NewEncoder(w).Encode(response)
				}))
				defer server.Close()

				client := NewClient().
					WithBaseUrl(server.URL).
					WithAuth(BasicAuth{Username: "test", Password: "test"})
				pools, err := client.ListFlinkComputePools(tt.environmentID)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if len(pools) != len(tt.mockPools) {
					t.Errorf("expected %d pools, got %d", len(tt.mockPools), len(pools))
				}
			} else {
				client := NewClient().WithAuth(BasicAuth{Username: "test", Password: "test"})
				_, err := client.ListFlinkComputePools(tt.environmentID)
				if err == nil {
					t.Error("expected error, got nil")
				}
			}
		})
	}
}

// TestCreateFlinkStatement tests the CreateFlinkStatement function
func TestCreateFlinkStatement(t *testing.T) {
	tests := []struct {
		name           string
		environmentID  string
		computePoolID  string
		displayName    string
		statement      string
		mockStatusCode int
		mockResponse   *FlinkStatement
		wantErr        bool
	}{
		{
			name:           "successful creation",
			environmentID:  "env-123",
			computePoolID:  "pool-456",
			displayName:    "test-statement",
			statement:      "CREATE TABLE test (id STRING);",
			mockStatusCode: http.StatusCreated,
			mockResponse: &FlinkStatement{
				DisplayName: "test-statement",
				Spec: &FlinkStatementSpec{
					Statement:     "CREATE TABLE test (id STRING);",
					ComputePoolID: "pool-456",
				},
			},
			wantErr: false,
		},
		{
			name:          "empty environment ID",
			environmentID: "",
			computePoolID: "pool-456",
			displayName:   "test-statement",
			statement:     "CREATE TABLE test (id STRING);",
			wantErr:       true,
		},
		{
			name:          "empty compute pool ID",
			environmentID: "env-123",
			computePoolID: "",
			displayName:   "test-statement",
			statement:     "CREATE TABLE test (id STRING);",
			wantErr:       true,
		},
		{
			name:          "empty display name",
			environmentID: "env-123",
			computePoolID: "pool-456",
			displayName:   "",
			statement:     "CREATE TABLE test (id STRING);",
			wantErr:       true,
		},
		{
			name:          "empty statement",
			environmentID: "env-123",
			computePoolID: "pool-456",
			displayName:   "test-statement",
			statement:     "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tt.mockStatusCode)
					if tt.mockResponse != nil {
						_ = json.NewEncoder(w).Encode(tt.mockResponse)
					}
				}))
				defer server.Close()

				client := NewClient().
					WithBaseUrl(server.URL).
					WithAuth(BasicAuth{Username: "test", Password: "test"})
				stmt, err := client.CreateFlinkStatement(
					tt.environmentID,
					tt.computePoolID,
					tt.displayName,
					tt.statement,
				)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if stmt.DisplayName != tt.mockResponse.DisplayName {
					t.Errorf("expected DisplayName %s, got %s", tt.mockResponse.DisplayName, stmt.DisplayName)
				}
			} else {
				client := NewClient().WithAuth(BasicAuth{Username: "test", Password: "test"})
				_, err := client.CreateFlinkStatement(
					tt.environmentID,
					tt.computePoolID,
					tt.displayName,
					tt.statement,
				)
				if err == nil {
					t.Error("expected error, got nil")
				}
			}
		})
	}
}

// TestFlinkComputePoolTypes verifies Flink Compute Pool data structures
func TestFlinkComputePoolTypes(t *testing.T) {
	now := time.Now()
	pool := FlinkComputePool{
		DisplayName: "test-pool",
		MaxCFU:      5,
		Cloud:       "AWS",
		Region:      "us-east-1",
		Status:      ComputePoolStatusAvailable,
		CreatedAt:   &now,
		UpdatedAt:   &now,
	}

	if pool.DisplayName != "test-pool" {
		t.Errorf("expected display_name 'test-pool', got %s", pool.DisplayName)
	}

	if pool.MaxCFU != 5 {
		t.Errorf("expected max_cfu 5, got %d", pool.MaxCFU)
	}

	if pool.Status != ComputePoolStatusAvailable {
		t.Errorf("expected status AVAILABLE, got %s", pool.Status)
	}
}

// TestFlinkStatementTypes verifies Flink Statement data structures
func TestFlinkStatementTypes(t *testing.T) {
	spec := &FlinkStatementSpec{
		Statement:     "CREATE TABLE test (id STRING);",
		ComputePoolID: "pool-123",
	}

	status := &FlinkStatementStatus{
		Phase:         StatementPhaseRunning,
		DetailedPhase: StatementDetailedPhaseReady,
		Completed:     false,
	}

	stmt := FlinkStatement{
		DisplayName: "test-statement",
		Spec:        spec,
		Status:      status,
	}

	if stmt.DisplayName != "test-statement" {
		t.Errorf("expected display_name 'test-statement', got %s", stmt.DisplayName)
	}

	if stmt.Spec.ComputePoolID != "pool-123" {
		t.Errorf("expected compute_pool_id 'pool-123', got %s", stmt.Spec.ComputePoolID)
	}

	if stmt.Status.Phase != StatementPhaseRunning {
		t.Errorf("expected phase RUNNING, got %s", stmt.Status.Phase)
	}

	if stmt.Status.DetailedPhase != StatementDetailedPhaseReady {
		t.Errorf("expected detailed phase READY, got %s", stmt.Status.DetailedPhase)
	}
}

// TestStatusConstants verifies status constants
func TestStatusConstants(t *testing.T) {
	// Test ComputePoolStatus constants
	if ComputePoolStatusProvisioning != "PROVISIONING" {
		t.Errorf("expected PROVISIONING, got %s", ComputePoolStatusProvisioning)
	}
	if ComputePoolStatusAvailable != "AVAILABLE" {
		t.Errorf("expected AVAILABLE, got %s", ComputePoolStatusAvailable)
	}

	// Test StatementPhase constants
	if StatementPhaseRunning != "RUNNING" {
		t.Errorf("expected RUNNING, got %s", StatementPhaseRunning)
	}
	if StatementPhaseCompleted != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", StatementPhaseCompleted)
	}

	// Test StatementDetailedPhase constants
	if StatementDetailedPhaseInitializing != "INITIALIZING" {
		t.Errorf("expected INITIALIZING, got %s", StatementDetailedPhaseInitializing)
	}
	if StatementDetailedPhaseReady != "READY" {
		t.Errorf("expected READY, got %s", StatementDetailedPhaseReady)
	}
}
