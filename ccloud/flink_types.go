package ccloud

// ComputePoolStatus represents the status of a Flink Compute Pool
type ComputePoolStatus string

const (
	ComputePoolStatusProvisioning ComputePoolStatus = "PROVISIONING"
	ComputePoolStatusAvailable    ComputePoolStatus = "AVAILABLE"
	ComputePoolStatusDeleting     ComputePoolStatus = "DELETING"
	ComputePoolStatusFailed       ComputePoolStatus = "FAILED"
)

// StatementPhase represents the phase of a Flink statement
type StatementPhase string

const (
	StatementPhaseRunning   StatementPhase = "RUNNING"
	StatementPhaseCompleted StatementPhase = "COMPLETED"
	StatementPhaseFailed    StatementPhase = "FAILED"
	StatementPhasePending   StatementPhase = "PENDING"
)

// StatementDetailedPhase represents the detailed phase of a Flink statement
type StatementDetailedPhase string

const (
	StatementDetailedPhaseInitializing StatementDetailedPhase = "INITIALIZING"
	StatementDetailedPhaseReady        StatementDetailedPhase = "READY"
	StatementDetailedPhaseRunning      StatementDetailedPhase = "RUNNING"
)

// EnvironmentReference represents a reference to an environment
type EnvironmentReference struct {
	ID string `json:"id,omitempty"`
}

// PrincipalReference represents a reference to a principal (service account)
type PrincipalReference struct {
	ID string `json:"id,omitempty"`
}

// CatalogReference represents a reference to a catalog
type CatalogReference struct {
	Name string `json:"name,omitempty"`
}

// DatabaseReference represents a reference to a database
type DatabaseReference struct {
	Name string `json:"name,omitempty"`
}

// CreateComputePoolRequest represents the request body for creating a compute pool
type CreateComputePoolRequest struct {
	DisplayName string `json:"display_name"`
	MaxCFU      int    `json:"max_cfu"`
	Cloud       string `json:"cloud"`
	Region      string `json:"region"`
}

// CreateStatementRequest represents the request body for creating a statement
type CreateStatementRequest struct {
	DisplayName string             `json:"display_name"`
	Spec        FlinkStatementSpec `json:"spec"`
}
