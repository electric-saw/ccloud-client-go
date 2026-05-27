package ccloud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

// FlinkComputePool represents a Flink Compute Pool in Confluent Cloud
type FlinkComputePool struct {
	common.BaseModel
	DisplayName string                `json:"display_name,omitempty"`
	MaxCFU      int                   `json:"max_cfu,omitempty"`
	Environment *EnvironmentReference `json:"environment,omitempty"`
	Cloud       string                `json:"cloud,omitempty"`
	Region      string                `json:"region,omitempty"`
	Status      ComputePoolStatus     `json:"status,omitempty"`
	UpdatedAt   *time.Time            `json:"updated_at,omitempty"`
	CreatedAt   *time.Time            `json:"created_at,omitempty"`
}

// FlinkComputePoolList represents a list of Flink Compute Pools
type FlinkComputePoolList struct {
	common.BaseModel
	Data []FlinkComputePool `json:"data"`
}

// FlinkStatement represents a Flink SQL statement/job
type FlinkStatement struct {
	common.BaseModel
	DisplayName string                `json:"display_name,omitempty"`
	Spec        *FlinkStatementSpec   `json:"spec,omitempty"`
	Status      *FlinkStatementStatus `json:"status,omitempty"`
	UpdatedAt   *time.Time            `json:"updated_at,omitempty"`
	CreatedAt   *time.Time            `json:"created_at,omitempty"`
}

// FlinkStatementSpec defines the specification for a Flink statement
type FlinkStatementSpec struct {
	Statement     string                `json:"statement,omitempty"`
	ComputePoolID string                `json:"compute_pool_id,omitempty"`
	Principal     *PrincipalReference   `json:"principal,omitempty"`
	Environment   *EnvironmentReference `json:"environment,omitempty"`
	Catalog       *CatalogReference     `json:"catalog,omitempty"`
	Database      *DatabaseReference    `json:"database,omitempty"`
}

// FlinkStatementStatus represents the current status of a Flink statement
type FlinkStatementStatus struct {
	Phase            StatementPhase         `json:"phase,omitempty"`
	DetailedPhase    StatementDetailedPhase `json:"detailed_phase,omitempty"`
	FailureReason    string                 `json:"failure_reason,omitempty"`
	StatementName    string                 `json:"statement_name,omitempty"`
	ComputePoolID    string                 `json:"compute_pool_id,omitempty"`
	FlinkJobID       string                 `json:"flink_job_id,omitempty"`
	HistoryServerURL string                 `json:"history_server_url,omitempty"`
	Completed        bool                   `json:"completed,omitempty"`
}

// FlinkStatementList represents a list of Flink statements
type FlinkStatementList struct {
	common.BaseModel
	Data []FlinkStatement `json:"data"`
}

// extractAPIError extracts detailed error information from an API response
func extractAPIError(resp *http.Response, operation string) error {
	var errResp struct {
		ErrorCode int    `json:"error_code,omitempty"`
		Message   string `json:"message,omitempty"`
	}

	// Try to parse error response, but don't fail if we can't
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &errResp)

	if errResp.Message != "" {
		return fmt.Errorf("%s failed: %s (code: %d, status: %d)",
			operation, errResp.Message, errResp.ErrorCode, resp.StatusCode)
	}

	return fmt.Errorf("%s failed: HTTP %d %s",
		operation, resp.StatusCode, resp.Status)
}

// CreateFlinkComputePool creates a new Flink Compute Pool
func (c *ConfluentClient) CreateFlinkComputePool(
	environmentID string,
	displayName string,
	maxCFU int,
	cloud string,
	region string,
) (*FlinkComputePool, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}
	if displayName == "" {
		return nil, fmt.Errorf("displayName cannot be empty")
	}
	if maxCFU <= 0 {
		return nil, fmt.Errorf("maxCFU must be greater than 0, got %d", maxCFU)
	}
	if cloud == "" {
		return nil, fmt.Errorf("cloud cannot be empty")
	}
	if region == "" {
		return nil, fmt.Errorf("region cannot be empty")
	}

	urlPath := fmt.Sprintf("/compute/v1/environments/%s/compute-pools", environmentID)

	payload := CreateComputePoolRequest{
		DisplayName: displayName,
		MaxCFU:      maxCFU,
		Cloud:       cloud,
		Region:      region,
	}

	req, err := c.doRequest(urlPath, http.MethodPost, payload, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for compute pool %q: %w", displayName, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusCreated && req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, fmt.Sprintf("create compute pool %q", displayName))
	}

	var pool FlinkComputePool
	if err := json.NewDecoder(req.Body).Decode(&pool); err != nil {
		return nil, fmt.Errorf("failed to decode compute pool response: %w", err)
	}

	return &pool, nil
}

// GetFlinkComputePool retrieves a Flink Compute Pool by ID
func (c *ConfluentClient) GetFlinkComputePool(environmentID, computePoolID string) (*FlinkComputePool, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}
	if computePoolID == "" {
		return nil, fmt.Errorf("computePoolID cannot be empty")
	}

	urlPath := fmt.Sprintf("/compute/v1/environments/%s/compute-pools/%s", environmentID, computePoolID)

	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for compute pool %q: %w", computePoolID, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, fmt.Sprintf("get compute pool %q", computePoolID))
	}

	var pool FlinkComputePool
	if err := json.NewDecoder(req.Body).Decode(&pool); err != nil {
		return nil, fmt.Errorf("failed to decode compute pool response: %w", err)
	}

	return &pool, nil
}

// ListFlinkComputePools lists all Flink Compute Pools in an environment
func (c *ConfluentClient) ListFlinkComputePools(environmentID string) ([]FlinkComputePool, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}

	urlPath := fmt.Sprintf("/compute/v1/environments/%s/compute-pools", environmentID)

	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for listing compute pools: %w", err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, "list compute pools")
	}

	var poolList FlinkComputePoolList
	if err := json.NewDecoder(req.Body).Decode(&poolList); err != nil {
		return nil, fmt.Errorf("failed to decode compute pools list response: %w", err)
	}

	return poolList.Data, nil
}

// DeleteFlinkComputePool deletes a Flink Compute Pool
func (c *ConfluentClient) DeleteFlinkComputePool(environmentID, computePoolID string) error {
	// Input validation
	if environmentID == "" {
		return fmt.Errorf("environmentID cannot be empty")
	}
	if computePoolID == "" {
		return fmt.Errorf("computePoolID cannot be empty")
	}

	urlPath := fmt.Sprintf("/compute/v1/environments/%s/compute-pools/%s", environmentID, computePoolID)

	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to make request for deleting compute pool %q: %w", computePoolID, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK && req.StatusCode != http.StatusNoContent && req.StatusCode != http.StatusAccepted {
		return extractAPIError(req, fmt.Sprintf("delete compute pool %q", computePoolID))
	}

	return nil
}

// CreateFlinkStatement creates a new Flink SQL statement
func (c *ConfluentClient) CreateFlinkStatement(
	environmentID string,
	computePoolID string,
	displayName string,
	statement string,
) (*FlinkStatement, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}
	if computePoolID == "" {
		return nil, fmt.Errorf("computePoolID cannot be empty")
	}
	if displayName == "" {
		return nil, fmt.Errorf("displayName cannot be empty")
	}
	if statement == "" {
		return nil, fmt.Errorf("statement cannot be empty")
	}

	urlPath := fmt.Sprintf("/sql/v1/environments/%s/statements", environmentID)

	payload := CreateStatementRequest{
		DisplayName: displayName,
		Spec: FlinkStatementSpec{
			Statement:     statement,
			ComputePoolID: computePoolID,
		},
	}

	req, err := c.doRequest(urlPath, http.MethodPost, payload, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for statement %q: %w", displayName, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusCreated && req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, fmt.Sprintf("create statement %q", displayName))
	}

	var stmt FlinkStatement
	if err := json.NewDecoder(req.Body).Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// GetFlinkStatement retrieves a Flink statement by ID
func (c *ConfluentClient) GetFlinkStatement(environmentID, statementName string) (*FlinkStatement, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}
	if statementName == "" {
		return nil, fmt.Errorf("statementName cannot be empty")
	}

	urlPath := fmt.Sprintf("/sql/v1/environments/%s/statements/%s", environmentID, statementName)

	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for statement %q: %w", statementName, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, fmt.Sprintf("get statement %q", statementName))
	}

	var stmt FlinkStatement
	if err := json.NewDecoder(req.Body).Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// ListFlinkStatements lists all Flink statements in an environment
func (c *ConfluentClient) ListFlinkStatements(environmentID string) ([]FlinkStatement, error) {
	// Input validation
	if environmentID == "" {
		return nil, fmt.Errorf("environmentID cannot be empty")
	}

	urlPath := fmt.Sprintf("/sql/v1/environments/%s/statements", environmentID)

	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make request for listing statements: %w", err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK {
		return nil, extractAPIError(req, "list statements")
	}

	var stmtList FlinkStatementList
	if err := json.NewDecoder(req.Body).Decode(&stmtList); err != nil {
		return nil, fmt.Errorf("failed to decode statements list response: %w", err)
	}

	return stmtList.Data, nil
}

// DeleteFlinkStatement deletes a Flink statement
func (c *ConfluentClient) DeleteFlinkStatement(environmentID, statementName string) error {
	// Input validation
	if environmentID == "" {
		return fmt.Errorf("environmentID cannot be empty")
	}
	if statementName == "" {
		return fmt.Errorf("statementName cannot be empty")
	}

	urlPath := fmt.Sprintf("/sql/v1/environments/%s/statements/%s", environmentID, statementName)

	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to make request for deleting statement %q: %w", statementName, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK && req.StatusCode != http.StatusNoContent && req.StatusCode != http.StatusAccepted {
		return extractAPIError(req, fmt.Sprintf("delete statement %q", statementName))
	}

	return nil
}

// CancelFlinkStatement cancels a running Flink statement
func (c *ConfluentClient) CancelFlinkStatement(environmentID, statementName string) error {
	// Input validation
	if environmentID == "" {
		return fmt.Errorf("environmentID cannot be empty")
	}
	if statementName == "" {
		return fmt.Errorf("statementName cannot be empty")
	}

	urlPath := fmt.Sprintf("/sql/v1/environments/%s/statements/%s:cancel", environmentID, statementName)

	req, err := c.doRequest(urlPath, http.MethodPost, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to make request for cancelling statement %q: %w", statementName, err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusOK && req.StatusCode != http.StatusAccepted {
		return extractAPIError(req, fmt.Sprintf("cancel statement %q", statementName))
	}

	return nil
}
