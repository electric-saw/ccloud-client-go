package sql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oapi-codegen/runtime"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

const (
	Artifactv1 ArtifactV1FlinkArtifactApiVersion = "artifact/v1"
)

func (e ArtifactV1FlinkArtifactApiVersion) Valid() bool {
	switch e {
	case Artifactv1:
		return true
	default:
		return false
	}
}

const (
	FlinkArtifact ArtifactV1FlinkArtifactKind = "FlinkArtifact"
)

func (e ArtifactV1FlinkArtifactKind) Valid() bool {
	switch e {
	case FlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1UploadSource ArtifactV1UploadSourcePresignedUrlApiVersion = "artifact.v1/UploadSource"
)

func (e ArtifactV1UploadSourcePresignedUrlApiVersion) Valid() bool {
	switch e {
	case ArtifactV1UploadSource:
		return true
	default:
		return false
	}
}

const (
	PresignedUrl ArtifactV1UploadSourcePresignedUrlKind = "PresignedUrl"
)

func (e ArtifactV1UploadSourcePresignedUrlKind) Valid() bool {
	switch e {
	case PresignedUrl:
		return true
	default:
		return false
	}
}

const (
	SqlV1AgentApiVersionSqlv1 SqlV1AgentApiVersion = "sql/v1"
)

func (e SqlV1AgentApiVersion) Valid() bool {
	switch e {
	case SqlV1AgentApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1AgentKindAgent SqlV1AgentKind = "Agent"
)

func (e SqlV1AgentKind) Valid() bool {
	switch e {
	case SqlV1AgentKindAgent:
		return true
	default:
		return false
	}
}

const (
	SqlV1AgentListApiVersionSqlv1 SqlV1AgentListApiVersion = "sql/v1"
)

func (e SqlV1AgentListApiVersion) Valid() bool {
	switch e {
	case SqlV1AgentListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	AgentList SqlV1AgentListKind = "AgentList"
)

func (e SqlV1AgentListKind) Valid() bool {
	switch e {
	case AgentList:
		return true
	default:
		return false
	}
}

const (
	Computed SqlV1ComputedColumnKind = "Computed"
)

func (e SqlV1ComputedColumnKind) Valid() bool {
	switch e {
	case Computed:
		return true
	default:
		return false
	}
}

const (
	SqlV1ConnectionApiVersionSqlv1 SqlV1ConnectionApiVersion = "sql/v1"
)

func (e SqlV1ConnectionApiVersion) Valid() bool {
	switch e {
	case SqlV1ConnectionApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ConnectionKindConnection SqlV1ConnectionKind = "Connection"
)

func (e SqlV1ConnectionKind) Valid() bool {
	switch e {
	case SqlV1ConnectionKindConnection:
		return true
	default:
		return false
	}
}

const (
	SqlV1ConnectionListApiVersionSqlv1 SqlV1ConnectionListApiVersion = "sql/v1"
)

func (e SqlV1ConnectionListApiVersion) Valid() bool {
	switch e {
	case SqlV1ConnectionListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ConnectionListDataApiVersionSqlv1 SqlV1ConnectionListDataApiVersion = "sql/v1"
)

func (e SqlV1ConnectionListDataApiVersion) Valid() bool {
	switch e {
	case SqlV1ConnectionListDataApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ConnectionListDataKindConnection SqlV1ConnectionListDataKind = "Connection"
)

func (e SqlV1ConnectionListDataKind) Valid() bool {
	switch e {
	case SqlV1ConnectionListDataKindConnection:
		return true
	default:
		return false
	}
}

const (
	ConnectionList SqlV1ConnectionListKind = "ConnectionList"
)

func (e SqlV1ConnectionListKind) Valid() bool {
	switch e {
	case ConnectionList:
		return true
	default:
		return false
	}
}

const (
	SqlV1MaterializedTableApiVersionSqlv1 SqlV1MaterializedTableApiVersion = "sql/v1"
)

func (e SqlV1MaterializedTableApiVersion) Valid() bool {
	switch e {
	case SqlV1MaterializedTableApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1MaterializedTableKindMaterializedTable SqlV1MaterializedTableKind = "MaterializedTable"
)

func (e SqlV1MaterializedTableKind) Valid() bool {
	switch e {
	case SqlV1MaterializedTableKindMaterializedTable:
		return true
	default:
		return false
	}
}

const (
	SqlV1MaterializedTableListApiVersionSqlv1 SqlV1MaterializedTableListApiVersion = "sql/v1"
)

func (e SqlV1MaterializedTableListApiVersion) Valid() bool {
	switch e {
	case SqlV1MaterializedTableListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	MaterializedTableList SqlV1MaterializedTableListKind = "MaterializedTableList"
)

func (e SqlV1MaterializedTableListKind) Valid() bool {
	switch e {
	case MaterializedTableList:
		return true
	default:
		return false
	}
}

const (
	SqlV1MaterializedTableVersionApiVersionSqlv1 SqlV1MaterializedTableVersionApiVersion = "sql/v1"
)

func (e SqlV1MaterializedTableVersionApiVersion) Valid() bool {
	switch e {
	case SqlV1MaterializedTableVersionApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	MaterializedTableVersion SqlV1MaterializedTableVersionKind = "MaterializedTableVersion"
)

func (e SqlV1MaterializedTableVersionKind) Valid() bool {
	switch e {
	case MaterializedTableVersion:
		return true
	default:
		return false
	}
}

const (
	SqlV1MaterializedTableVersionListApiVersionSqlv1 SqlV1MaterializedTableVersionListApiVersion = "sql/v1"
)

func (e SqlV1MaterializedTableVersionListApiVersion) Valid() bool {
	switch e {
	case SqlV1MaterializedTableVersionListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	MaterializedTableVersionList SqlV1MaterializedTableVersionListKind = "MaterializedTableVersionList"
)

func (e SqlV1MaterializedTableVersionListKind) Valid() bool {
	switch e {
	case MaterializedTableVersionList:
		return true
	default:
		return false
	}
}

const (
	Metadata SqlV1MetadataColumnKind = "Metadata"
)

func (e SqlV1MetadataColumnKind) Valid() bool {
	switch e {
	case Metadata:
		return true
	default:
		return false
	}
}

const (
	Physical SqlV1PhysicalColumnKind = "Physical"
)

func (e SqlV1PhysicalColumnKind) Valid() bool {
	switch e {
	case Physical:
		return true
	default:
		return false
	}
}

const (
	PlaintextProvider SqlV1PlaintextProviderKind = "PlaintextProvider"
)

func (e SqlV1PlaintextProviderKind) Valid() bool {
	switch e {
	case PlaintextProvider:
		return true
	default:
		return false
	}
}

const (
	StatementException SqlV1StatementExceptionKind = "StatementException"
)

func (e SqlV1StatementExceptionKind) Valid() bool {
	switch e {
	case StatementException:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementExceptionListApiVersionSqlv1 SqlV1StatementExceptionListApiVersion = "sql/v1"
)

func (e SqlV1StatementExceptionListApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementExceptionListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	StatementExceptionList SqlV1StatementExceptionListKind = "StatementExceptionList"
)

func (e SqlV1StatementExceptionListKind) Valid() bool {
	switch e {
	case StatementExceptionList:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementResultApiVersionSqlv1 SqlV1StatementResultApiVersion = "sql/v1"
)

func (e SqlV1StatementResultApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementResultApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementResultKindStatementResult SqlV1StatementResultKind = "StatementResult"
)

func (e SqlV1StatementResultKind) Valid() bool {
	switch e {
	case SqlV1StatementResultKindStatementResult:
		return true
	default:
		return false
	}
}

const (
	SqlV1ToolApiVersionSqlv1 SqlV1ToolApiVersion = "sql/v1"
)

func (e SqlV1ToolApiVersion) Valid() bool {
	switch e {
	case SqlV1ToolApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ToolKindTool SqlV1ToolKind = "Tool"
)

func (e SqlV1ToolKind) Valid() bool {
	switch e {
	case SqlV1ToolKindTool:
		return true
	default:
		return false
	}
}

const (
	SqlV1ToolListApiVersionSqlv1 SqlV1ToolListApiVersion = "sql/v1"
)

func (e SqlV1ToolListApiVersion) Valid() bool {
	switch e {
	case SqlV1ToolListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ToolListDataApiVersionSqlv1 SqlV1ToolListDataApiVersion = "sql/v1"
)

func (e SqlV1ToolListDataApiVersion) Valid() bool {
	switch e {
	case SqlV1ToolListDataApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1ToolListDataKindTool SqlV1ToolListDataKind = "Tool"
)

func (e SqlV1ToolListDataKind) Valid() bool {
	switch e {
	case SqlV1ToolListDataKindTool:
		return true
	default:
		return false
	}
}

const (
	ToolList SqlV1ToolListKind = "ToolList"
)

func (e SqlV1ToolListKind) Valid() bool {
	switch e {
	case ToolList:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1ConnectionJSONBodyApiVersionSqlv1 CreateSqlv1ConnectionJSONBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1ConnectionJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1ConnectionJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1ConnectionJSONBodyKindConnection CreateSqlv1ConnectionJSONBodyKind = "Connection"
)

func (e CreateSqlv1ConnectionJSONBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1ConnectionJSONBodyKindConnection:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Connection201JSONResponseBodyApiVersionSqlv1 CreateSqlv1Connection201JSONResponseBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1Connection201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1Connection201JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Connection201JSONResponseBodyKindConnection CreateSqlv1Connection201JSONResponseBodyKind = "Connection"
)

func (e CreateSqlv1Connection201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1Connection201JSONResponseBodyKindConnection:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Connection200JSONResponseBodyApiVersionSqlv1 GetSqlv1Connection200JSONResponseBodyApiVersion = "sql/v1"
)

func (e GetSqlv1Connection200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSqlv1Connection200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Connection200JSONResponseBodyKindConnection GetSqlv1Connection200JSONResponseBodyKind = "Connection"
)

func (e GetSqlv1Connection200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSqlv1Connection200JSONResponseBodyKindConnection:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1ConnectionJSONBodyApiVersionSqlv1 UpdateSqlv1ConnectionJSONBodyApiVersion = "sql/v1"
)

func (e UpdateSqlv1ConnectionJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSqlv1ConnectionJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1ConnectionJSONBodyKindConnection UpdateSqlv1ConnectionJSONBodyKind = "Connection"
)

func (e UpdateSqlv1ConnectionJSONBodyKind) Valid() bool {
	switch e {
	case UpdateSqlv1ConnectionJSONBodyKindConnection:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1ToolJSONBodyApiVersionSqlv1 CreateSqlv1ToolJSONBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1ToolJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1ToolJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1ToolJSONBodyKindTool CreateSqlv1ToolJSONBodyKind = "Tool"
)

func (e CreateSqlv1ToolJSONBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1ToolJSONBodyKindTool:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Tool200JSONResponseBodyApiVersionSqlv1 CreateSqlv1Tool200JSONResponseBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1Tool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1Tool200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Tool200JSONResponseBodyKindTool CreateSqlv1Tool200JSONResponseBodyKind = "Tool"
)

func (e CreateSqlv1Tool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1Tool200JSONResponseBodyKindTool:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Tool200JSONResponseBodyApiVersionSqlv1 GetSqlv1Tool200JSONResponseBodyApiVersion = "sql/v1"
)

func (e GetSqlv1Tool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSqlv1Tool200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Tool200JSONResponseBodyKindTool GetSqlv1Tool200JSONResponseBodyKind = "Tool"
)

func (e GetSqlv1Tool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSqlv1Tool200JSONResponseBodyKindTool:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1AgentJSONBodyApiVersionSqlv1 CreateSqlv1AgentJSONBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1AgentJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1AgentJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1AgentJSONBodyKindAgent CreateSqlv1AgentJSONBodyKind = "Agent"
)

func (e CreateSqlv1AgentJSONBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1AgentJSONBodyKindAgent:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Agent200JSONResponseBodyApiVersionSqlv1 CreateSqlv1Agent200JSONResponseBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1Agent200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1Agent200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Agent200JSONResponseBodyKindAgent CreateSqlv1Agent200JSONResponseBodyKind = "Agent"
)

func (e CreateSqlv1Agent200JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1Agent200JSONResponseBodyKindAgent:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1AgentJSONBodyApiVersionSqlv1 UpdateSqlv1AgentJSONBodyApiVersion = "sql/v1"
)

func (e UpdateSqlv1AgentJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSqlv1AgentJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1AgentJSONBodyKindAgent UpdateSqlv1AgentJSONBodyKind = "Agent"
)

func (e UpdateSqlv1AgentJSONBodyKind) Valid() bool {
	switch e {
	case UpdateSqlv1AgentJSONBodyKindAgent:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1MaterializedTableJSONBodyApiVersionSqlv1 CreateSqlv1MaterializedTableJSONBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1MaterializedTableJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1MaterializedTableJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1MaterializedTableJSONBodyKindMaterializedTable CreateSqlv1MaterializedTableJSONBodyKind = "MaterializedTable"
)

func (e CreateSqlv1MaterializedTableJSONBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1MaterializedTableJSONBodyKindMaterializedTable:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1MaterializedTable201JSONResponseBodyApiVersionSqlv1 CreateSqlv1MaterializedTable201JSONResponseBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1MaterializedTable201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1MaterializedTable201JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1MaterializedTable201JSONResponseBodyKindMaterializedTable CreateSqlv1MaterializedTable201JSONResponseBodyKind = "MaterializedTable"
)

func (e CreateSqlv1MaterializedTable201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1MaterializedTable201JSONResponseBodyKindMaterializedTable:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1MaterializedTableJSONBodyApiVersionSqlv1 UpdateSqlv1MaterializedTableJSONBodyApiVersion = "sql/v1"
)

func (e UpdateSqlv1MaterializedTableJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSqlv1MaterializedTableJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1MaterializedTableJSONBodyKindMaterializedTable UpdateSqlv1MaterializedTableJSONBodyKind = "MaterializedTable"
)

func (e UpdateSqlv1MaterializedTableJSONBodyKind) Valid() bool {
	switch e {
	case UpdateSqlv1MaterializedTableJSONBodyKindMaterializedTable:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1StatementResult200JSONResponseBodyApiVersionSqlv1 GetSqlv1StatementResult200JSONResponseBodyApiVersion = "sql/v1"
)

func (e GetSqlv1StatementResult200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSqlv1StatementResult200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1StatementResult200JSONResponseBodyKindStatementResult GetSqlv1StatementResult200JSONResponseBodyKind = "StatementResult"
)

func (e GetSqlv1StatementResult200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSqlv1StatementResult200JSONResponseBodyKindStatementResult:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type DataType struct {
	// ClassName The class name of the structured data type (if applicable).
	ClassName *string `json:"class_name,omitempty"`

	// ElementType The type of the element in the data type (if applicable).
	ElementType *DataType `json:"element_type,omitempty"`

	// Fields The fields of the element in the data type (if applicable).
	Fields *[]RowFieldType `json:"fields,omitempty"`

	// FractionalPrecision The fractional precision of the data type (if applicable).
	FractionalPrecision *int32 `json:"fractional_precision,omitempty"`

	// KeyType The type of the key in the data type (if applicable).
	KeyType *DataType `json:"key_type,omitempty"`

	// Length The length of the data type.
	Length *int32 `json:"length,omitempty"`

	// Nullable Indicates whether values in this column can be null.
	Nullable bool `json:"nullable"`

	// Precision The precision of the data type.
	Precision *int32 `json:"precision,omitempty"`

	// Resolution The resolution of the data type (if applicable).
	Resolution *string `json:"resolution,omitempty"`

	// Scale The scale of the data type.
	Scale *int32 `json:"scale,omitempty"`

	// Type The data type of the column.
	Type string `json:"type"`

	// ValueType The type of the value in the data type (if applicable).
	ValueType *DataType `json:"value_type,omitempty"`
}
type Error struct {
	// Code An application-specific error code, expressed as a string value.
	Code *string `json:"code,omitempty"`

	// Detail A human-readable explanation specific to this occurrence of the problem.
	Detail    *string `json:"detail,omitempty"`
	ErrorCode *int32  `json:"error_code,omitempty"`

	// Id A unique identifier for this particular occurrence of the problem.
	Id      *string `json:"id,omitempty"`
	Message *string `json:"message,omitempty"`

	// Source If this error was caused by a particular part of the API request, the source will point to the query string parameter or request body property that caused it.
	Source *struct {
		// Parameter A string indicating which query parameter caused the error.
		Parameter *string `json:"parameter,omitempty"`

		// Pointer A JSON Pointer [RFC6901] to the associated entity in the request document [e.g. "/spec" for a spec object, or "/spec/title" for a specific field].
		Pointer *string `json:"pointer,omitempty"`
	} `json:"source,omitempty"`

	// Status The HTTP status code applicable to this problem, expressed as a string value.
	Status *string `json:"status,omitempty"`

	// Title A short, human-readable summary of the problem. It **SHOULD NOT** change from occurrence to occurrence of the problem, except for purposes of localization.
	Title *string `json:"title,omitempty"`
}
type ExceptionListMeta struct {
	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self *string `json:"self,omitempty"`
}
type Failure struct {
	// Errors List of errors which caused this operation to fail
	Errors []Error `json:"errors"`
}
type ListMeta struct {
	// First A link to the first page of results. If a response does not contain a first link, then direct navigation to the first page is not supported.
	First *string `json:"first,omitempty"`

	// Last A link to the last page of results. If a response does not contain a last link, then direct navigation to the last page is not supported.
	Last *string `json:"last,omitempty"`

	// Next A link to the next page of results. If a response does not contain a next link, then there is no more data available.
	Next *string `json:"next,omitempty"`

	// Prev A link to the previous page of results. If a response does not contain a prev link, then either there is no previous data or backwards traversal through the result set is not supported.
	Prev *string `json:"prev,omitempty"`

	// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
	TotalSize *int32 `json:"total_size,omitempty"`
}
type ObjectMeta struct {
	// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
	DeletedAt *time.Time `json:"deleted_at,omitempty"`

	// ResourceName Resource Name is a Uniform Resource Identifier (URI) that is globally unique across space and time. It is represented as a Confluent Resource Name
	ResourceName *string `json:"resource_name,omitempty"`

	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self string `json:"self"`

	// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}
type ResultListMeta struct {
	// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// Next A URL that can be followed to get the next batch of results.
	Next *string `json:"next,omitempty"`

	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self *string `json:"self,omitempty"`
}
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type ArtifactV1FlinkArtifact struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1FlinkArtifactApiVersion `json:"api_version,omitempty"`

	// Class Java class or alias for the artifact as provided by developer. Deprecated
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	Class *string `json:"class,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Flink Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
	DisplayName *string `json:"display_name,omitempty"`

	// DocumentationLink Documentation link of the Flink Artifact.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment Environment the Flink Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ArtifactV1FlinkArtifactKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Region The Cloud provider region the Flink Artifact archive is uploaded.
	Region *string `json:"region,omitempty"`

	// RuntimeLanguage Runtime language of the Flink Artifact.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// Versions Versions associated with this Flink Artifact.
	Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
}
type ArtifactV1FlinkArtifactApiVersion string
type ArtifactV1FlinkArtifactKind string
type ArtifactV1FlinkArtifactVersion struct {
	// ArtifactId The Flink Artifact this version belongs to.
	ArtifactId ArtifactV1FlinkArtifact `json:"artifact_id"`

	// IsBeta Flag to specify stability of the version
	IsBeta *bool `json:"is_beta,omitempty"`

	// ReleaseNotes Release Notes of the Flink Artifact version.
	ReleaseNotes *string `json:"release_notes,omitempty"`

	// UploadSource Upload source of the Flink Artifact Version.
	UploadSource ArtifactV1FlinkArtifactVersion_UploadSource `json:"upload_source"`

	// Version Version id of the Flink Artifact.
	Version string `json:"version"`
}
type ArtifactV1FlinkArtifactVersion_UploadSource struct {
	union json.RawMessage
}
type ArtifactV1UploadSourcePresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1UploadSourcePresignedUrlApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *ArtifactV1UploadSourcePresignedUrlKind `json:"kind,omitempty"`

	// Location Location of the Flink Artifact source.
	Location *string `json:"location,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// UploadId Upload ID returned by the `/presigned-upload-url` API. This field returns an empty string in all responses.
	UploadId *string `json:"upload_id,omitempty"`
}
type ArtifactV1UploadSourcePresignedUrlApiVersion string
type ArtifactV1UploadSourcePresignedUrlKind string
type QueryV1alpha1DataType struct {
	// ClassName Class name of a structured type. Present in the Flink type model; the engine does not currently emit structured types.
	ClassName *string `json:"class_name,omitempty"`

	// ElementType Element type of an `ARRAY` or `MULTISET`.
	ElementType *QueryV1alpha1DataType `json:"elementType,omitempty"`

	// Fields Fields of a `ROW`, in declaration order.
	Fields *[]QueryV1alpha1RowFieldType `json:"fields,omitempty"`

	// FractionalPrecision Fractional-second precision of `INTERVAL_DAY_TIME`.
	FractionalPrecision *int32 `json:"fractionalPrecision,omitempty"`

	// KeyType Key type of a `MAP`.
	KeyType *QueryV1alpha1DataType `json:"keyType,omitempty"`

	// Length Declared length of `CHAR`, `VARCHAR`, `BINARY` and `VARBINARY`. Unbounded `VARCHAR` and `VARBINARY` report 2147483647.
	Length *int32 `json:"length,omitempty"`

	// Nullable Whether values of this column or field can be null.
	Nullable bool `json:"nullable"`

	// Precision Declared precision of `DECIMAL`, the `TIME`/`TIMESTAMP` types and the `INTERVAL_*` types.
	Precision *int32 `json:"precision,omitempty"`

	// Resolution Interval resolution, for example `YEAR_TO_MONTH` for `INTERVAL_YEAR_MONTH` or `DAY_TO_SECOND` for `INTERVAL_DAY_TIME`.
	Resolution *string `json:"resolution,omitempty"`

	// Scale Declared scale of `DECIMAL`.
	Scale *int32 `json:"scale,omitempty"`

	// Type The Flink logical type name of the column or field.
	Type string `json:"type"`

	// ValueType Value type of a `MAP`.
	ValueType *QueryV1alpha1DataType `json:"valueType,omitempty"`
}
type QueryV1alpha1ResultValue struct {
	union json.RawMessage
}
type QueryV1alpha1ResultValue0 = string
type QueryV1alpha1ResultValue1 = []QueryV1alpha1ResultValue
type QueryV1alpha1RowFieldType struct {
	// Description Optional field comment from the type declaration.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType QueryV1alpha1DataType `json:"fieldType"`

	// Name The name of the field.
	Name string `json:"name"`
}
type SqlV1Agent struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1AgentApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId string `json:"environment_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1AgentKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time         `json:"deleted_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user-provided name of the agent, unique within this environment.
	Name string `json:"name"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId openapi_types.UUID `json:"organization_id"`

	// Spec The specifications of the Agent.
	Spec   SqlV1AgentSpec    `json:"spec"`
	Status *SqlV1AgentStatus `json:"status,omitempty"`
}
type SqlV1AgentApiVersion string
type SqlV1AgentKind string
type SqlV1AgentList struct {
	ApiVersion SqlV1AgentListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []SqlV1Agent       `json:"data"`
	Kind SqlV1AgentListKind `json:"kind"`

	// Metadata ListMeta describes metadata that resource collections may have
	Metadata ListMeta `json:"metadata"`
}
type SqlV1AgentListApiVersion string
type SqlV1AgentListKind string
type SqlV1AgentSpec struct {
	// Description The description of the agent.
	Description *string `json:"description,omitempty"`

	// Model The name of the model the agent uses for inferencing.
	Model *string `json:"model,omitempty"`

	// Prompt The instruction prompt that guides the agent's behavior.
	Prompt *string `json:"prompt,omitempty"`

	// Properties A set of key-value option pairs that configure the agent's behavior.
	Properties *map[string]string `json:"properties,omitempty"`

	// Tools The list of tools available to the agent.
	Tools *[]string `json:"tools,omitempty"`
}
type SqlV1AgentStatus struct {
	// Phase Describes the status of the agent:
	//
	// READY: The Agent is created;
	//
	// RUNNING: The Agent is created and running in a query;
	Phase *string `json:"phase,omitempty"`
}
type SqlV1ColumnCommon struct {
	// Comment A comment or description for the column.
	Comment *string `json:"comment,omitempty"`

	// Name The name of the column.
	Name string   `json:"name"`
	Type DataType `json:"type"`
}
type SqlV1ColumnDetails struct {
	union json.RawMessage
}
type SqlV1ComputedColumn struct {
	// Comment A comment or description for the column.
	Comment *string `json:"comment,omitempty"`

	// Expression The SQL expression used to compute the column value.
	Expression string `json:"expression"`

	// Kind The kind of column.
	Kind SqlV1ComputedColumnKind `json:"kind"`
	Name interface{}             `json:"name"`
	Type interface{}             `json:"type"`

	// Virtual Indicates if the computed column is virtual.
	Virtual *bool `json:"virtual,omitempty"`
}
type SqlV1ComputedColumnKind string
type SqlV1Connection struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SqlV1ConnectionApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SqlV1ConnectionKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string `json:"name,omitempty"`

	// Spec Encapsulates the model provider access details
	Spec *SqlV1ConnectionSpec `json:"spec,omitempty"`

	// Status The status of the Connection
	Status *SqlV1ConnectionStatus `json:"status,omitempty"`
}
type SqlV1ConnectionApiVersion string
type SqlV1ConnectionKind string
type SqlV1ConnectionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1ConnectionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion SqlV1ConnectionListDataApiVersion `json:"api_version"`

		// Kind Kind defines the object this REST resource represents.
		Kind     SqlV1ConnectionListDataKind `json:"kind"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
			ResourceName    interface{} `json:"resource_name,omitempty"`
			ResourceVersion interface{} `json:"resource_version,omitempty"`
			Self            interface{} `json:"self"`
			Uid             interface{} `json:"uid,omitempty"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Name The user provided name of the resource, unique within this environment.
		Name string                 `json:"name"`
		Spec map[string]interface{} `json:"spec"`

		// Status The status of the Connection
		Status SqlV1ConnectionStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1ConnectionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`
		Self  interface{} `json:"self,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SqlV1ConnectionListApiVersion string
type SqlV1ConnectionListDataApiVersion string
type SqlV1ConnectionListDataKind string
type SqlV1ConnectionListKind string
type SqlV1ConnectionSpec struct {
	// AuthData The vendor specific authentication token details
	//
	// The contents are stored as opaque bytes given in plaintext by an EnvAdmin.
	// In future, we would support more secure methods for distributing authentication tokens.
	AuthData *SqlV1ConnectionSpec_AuthData `json:"auth_data,omitempty"`

	// ConnectionType The type of this connection.
	ConnectionType *string `json:"connection_type,omitempty"`

	// Endpoint The endpoint that is used to run model inferencing.
	Endpoint *string `json:"endpoint,omitempty"`
}
type SqlV1ConnectionSpec_AuthData struct {
	union json.RawMessage
}
type SqlV1ConnectionStatus struct {
	// Detail Details about why connection transitioned into a given status.
	Detail *string `json:"detail,omitempty"`

	// Phase Describes the status of the connection:
	//
	// READY: The Connection is usable;
	//
	// UNREACHABLE: The Connection endpoint is unreachable;
	//
	// INVALID_AUTH: The Connection auth token is invalid;
	Phase string `json:"phase"`
}
type SqlV1Constraint struct {
	Columns *[]string `json:"columns,omitempty"`

	// Enforced Whether the constraint is enforced.
	Enforced *bool   `json:"enforced,omitempty"`
	Name     *string `json:"name,omitempty"`

	// Type The type of constraint.
	Type *string `json:"type,omitempty"`
}
type SqlV1Distribution struct {
	// BucketCount The number of buckets.
	BucketCount *int32    `json:"bucket_count,omitempty"`
	Keys        *[]string `json:"keys,omitempty"`

	// Kind The kind of distribution.
	Kind string `json:"kind"`
}
type SqlV1MaterializedTable struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1MaterializedTableApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId string `json:"environment_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1MaterializedTableKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time         `json:"deleted_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user-provided name of the resource, unique within the Kafka cluster. May contain ASCII alphanumerics, '.', '_' and '-'; must not be '.' or '..'; max length 249.
	Name string `json:"name"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId openapi_types.UUID `json:"organization_id"`

	// Spec The specifications of the Materialized Table.
	Spec   SqlV1MaterializedTableSpec    `json:"spec"`
	Status *SqlV1MaterializedTableStatus `json:"status,omitempty"`
}
type SqlV1MaterializedTableApiVersion string
type SqlV1MaterializedTableKind string
type SqlV1MaterializedTableList struct {
	ApiVersion SqlV1MaterializedTableListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []SqlV1MaterializedTable       `json:"data"`
	Kind SqlV1MaterializedTableListKind `json:"kind"`

	// Metadata ListMeta describes metadata that resource collections may have
	Metadata ListMeta `json:"metadata"`
}
type SqlV1MaterializedTableListApiVersion string
type SqlV1MaterializedTableListKind string
type SqlV1MaterializedTableSpec struct {
	// Columns Details of each column in Materialized Table resource. If columns are not specified, we infer from query. If it's specified it must be compatible with the types in the query.
	Columns *[]SqlV1ColumnDetails `json:"columns,omitempty"`

	// ComputePoolId The id associated with the compute pool in context.
	// If not specified, the materialized table will use the default compute pool. The default pool is automatically determined by the system.
	ComputePoolId *string `json:"compute_pool_id,omitempty"`

	// Constraints Specify table constraints.
	Constraints *[]SqlV1Constraint `json:"constraints,omitempty"`

	// Distribution Only applicable on creation; ignored on update.
	Distribution *SqlV1Distribution `json:"distribution,omitempty"`

	// KafkaClusterId The ID of the Kafka cluster hosting the Materialized Table's topic.
	// This value must match the `kafka_cluster_id` path parameter.
	// It is immutable after creation and is ignored or rejected on update if changed.
	KafkaClusterId *string `json:"kafka_cluster_id,omitempty"`

	// Principal The id of a principal this Materialized Table query runs as.
	Principal *string `json:"principal,omitempty"`

	// Query Contains the query section (usually starting with a SELECT) of the latest Materialized Table.
	Query *string `json:"query,omitempty"`

	// SessionOptions Session configurations equivalent to the SQL 'SET' statement. Only applicable on creation; ignored on update.
	SessionOptions *map[string]string `json:"session_options,omitempty"`

	// Stopped Indicates whether the Materialized Table query should be stopped.
	Stopped *bool `json:"stopped,omitempty"`

	// TableOptions Defines configuration properties for the table, equivalent to the SQL 'WITH' clause
	TableOptions *map[string]string `json:"table_options,omitempty"`

	// Watermark Watermark strategy for the Materialized Table resource.
	Watermark *SqlV1Watermark `json:"watermark,omitempty"`
}
type SqlV1MaterializedTableStatus struct {
	// CreationStatement Entire Materialized Table statement as submitted by user e.g CREATE OR ALTER MATERIALIZED TABLE ...
	CreationStatement *string `json:"creation_statement,omitempty"`

	// Detail Optional. Human-readable description of phase.
	Detail *string `json:"detail,omitempty"`

	// LatestVersion Represents the latest submitted version of the Materialized Table. When a query evolution is accepted, `latest_version` is incremented immediately and will be greater than `version` until the new query is fully activated.
	LatestVersion *int32 `json:"latest_version,omitempty"`

	// Phase The lifecycle phase of the materialized table.
	Phase *string `json:"phase,omitempty"`

	// ScalingStatus Scaling status for this statement.
	ScalingStatus *SqlV1ScalingStatus `json:"scaling_status,omitempty"`

	// Version Represents the evolution history of the Materialized Table. The current value indicates the latest version.
	Version *int32 `json:"version,omitempty"`

	// Warnings List of warnings encountered during materialized table execution.
	Warnings *[]SqlV1MaterializedTableWarning `json:"warnings,omitempty"`
}
type SqlV1MaterializedTableVersion struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1MaterializedTableVersionApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId string `json:"environment_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1MaterializedTableVersionKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt *time.Time `json:"deleted_at,omitempty"`

		// ResourceName Resource Name is a Uniform Resource Identifier (URI) that is globally unique across space and time. It is represented as a Confluent Resource Name
		ResourceName *string     `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The resource version name, unique within the Kafka cluster.
	// Name conforms to DNS Subdomain (RFC 1123).
	Name string `json:"name"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId openapi_types.UUID `json:"organization_id"`

	// Spec The specifications of the Materialized Table Version.
	Spec SqlV1MaterializedTableVersionSpec `json:"spec"`
}
type SqlV1MaterializedTableVersionApiVersion string
type SqlV1MaterializedTableVersionKind string
type SqlV1MaterializedTableVersionList struct {
	ApiVersion SqlV1MaterializedTableVersionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []SqlV1MaterializedTableVersion       `json:"data"`
	Kind SqlV1MaterializedTableVersionListKind `json:"kind"`

	// Metadata ListMeta describes metadata that resource collections may have
	Metadata ListMeta `json:"metadata"`
}
type SqlV1MaterializedTableVersionListApiVersion string
type SqlV1MaterializedTableVersionListKind string
type SqlV1MaterializedTableVersionSpec struct {
	// Changes Changes affecting resources since the previous version.
	Changes *[]SqlV1ResourceChange `json:"changes,omitempty"`

	// Statement The full SQL statement for the materialized table as generated by SHOW CREATE MATERIALIZED TABLE at the time of the evolution.
	Statement string `json:"statement"`

	// Version The version number of the Materialized Table.
	Version int32 `json:"version"`
}
type SqlV1MaterializedTableWarning = SqlV1StatementWarning
type SqlV1MetadataColumn struct {
	// Comment A comment or description for the column.
	Comment *string `json:"comment,omitempty"`

	// Kind The kind of column.
	Kind SqlV1MetadataColumnKind `json:"kind"`

	// MetadataKey The system metadata key to reference.
	MetadataKey string      `json:"metadata_key"`
	Name        interface{} `json:"name"`
	Type        interface{} `json:"type"`

	// Virtual Indicates if the metadata column is virtual.
	Virtual *bool `json:"virtual,omitempty"`
}
type SqlV1MetadataColumnKind string
type SqlV1PhysicalColumn struct {
	// Comment A comment or description for the column.
	Comment *string `json:"comment,omitempty"`

	// Kind The kind of column.
	Kind SqlV1PhysicalColumnKind `json:"kind"`
	Name interface{}             `json:"name"`
	Type interface{}             `json:"type"`
}
type SqlV1PhysicalColumnKind string
type SqlV1PlaintextProvider struct {
	// Data Authentication token in plaintext JSON string.
	// For composite tokens, provide them as JSON.
	// This is sensitive piece of information stored as opaque bytes in an encrypted form with single level of encryption.
	//
	// Scoped to an endpoint of a `Connection` resource.
	Data *[]byte `json:"data,omitempty"`

	// Kind Plaintext Provider Kind Type
	Kind *SqlV1PlaintextProviderKind `json:"kind,omitempty"`
}
type SqlV1PlaintextProviderKind string
type SqlV1ResourceChange struct {
	// DatabaseLocator The database containing the resource. Can be either the database name or ID, depending on how it is referenced in the SQL statement text.
	DatabaseLocator string `json:"database_locator"`

	// Details Human-readable descriptions of the changes made to this resource.
	Details []string `json:"details"`

	// EnvironmentLocator The environment containing the resource. Can be either the environment name or ID, depending on how it is referenced in the SQL statement text.
	EnvironmentLocator string `json:"environment_locator"`

	// Kind The type of resource that was changed.
	Kind string `json:"kind"`

	// Name The name of the resource, unique within its scope (environment and database).
	Name string `json:"name"`
}
type SqlV1ScalingStatus struct {
	// LastUpdated The last time the scaling status was updated.
	LastUpdated *time.Time `json:"last_updated,omitempty"`

	// ScalingState OK: The statement runs at the right scale.
	//
	// PENDING_SCALE_DOWN: The statement requires less resources, and will be scaled down in the near future.
	//
	// PENDING_SCALE_UP: The statement requires more resources, and will be scaled up in the near future.
	//
	// POOL_EXHAUSTED: The statement requires more resources, but not enough resources are available.
	ScalingState *string `json:"scaling_state,omitempty"`
}
type SqlV1StatementException struct {
	// Kind Kind defines the object this REST resource represents.
	Kind *SqlV1StatementExceptionKind `json:"kind,omitempty"`

	// Message Error message of the statement exception.
	Message *string `json:"message,omitempty"`

	// Name Name of the SQL statement exception.
	Name *string `json:"name,omitempty"`

	// Timestamp The date and time at which the exception occurred. It is represented in RFC3339 format and is in UTC.
	Timestamp *time.Time `json:"timestamp,omitempty"`
}
type SqlV1StatementExceptionKind string
type SqlV1StatementExceptionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1StatementExceptionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []SqlV1StatementException `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1StatementExceptionListKind `json:"kind"`
	Metadata ExceptionListMeta               `json:"metadata"`
}
type SqlV1StatementExceptionListApiVersion string
type SqlV1StatementExceptionListKind string
type SqlV1StatementResult struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1StatementResultApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1StatementResultKind     `json:"kind"`
	Metadata ResultListMeta               `json:"metadata"`
	Results  *SqlV1StatementResultResults `json:"results,omitempty"`
}
type SqlV1StatementResultApiVersion string
type SqlV1StatementResultKind string
type SqlV1StatementResultResults struct {
	// Data A data property that contains an array of results. Each entry in the array is a separate result.
	//
	// The value of `op` attribute (if present) represents the kind of change that a row can describe in a changelog:
	//
	// `0`: represents `INSERT` (`+I`), i.e. insertion operation;
	//
	// `1`: represents `UPDATE_BEFORE` (`-U`), i.e. update operation with the previous content of the updated row.
	// This kind should occur together with `UPDATE_AFTER` for modelling an update that needs to retract
	// the previous row first. It is useful in cases of a non-idempotent update, i.e., an update of a row that is not
	// uniquely identifiable by a key;
	//
	// `2`: represents `UPDATE_AFTER` (`+U`), i.e. update operation with new content of the updated row;
	// This kind CAN occur together with `UPDATE_BEFORE` for modelling an update that
	// needs to retract the previous row first or it describes an idempotent update, i.e., an
	// update of a row that is uniquely identifiable by a key;
	//
	// `3`: represents `DELETE` (`-D`), i.e. deletion operation;
	//
	// Defaults to `0`.
	Data *[]interface{} `json:"data,omitempty"`
}
type SqlV1StatementWarning struct {
	// CreatedAt The timestamp when the warning was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt time.Time `json:"created_at"`

	// Message A human-readable string containing the description of the warning.
	Message string `json:"message"`

	// Reason A machine-readable short, upper case summary delimited by underscore.
	Reason string `json:"reason"`

	// Severity Indicates the severity of the warning.
	//
	// LOW: Indicates a low severity warning and for informing the user.
	//
	// MODERATE: Indicates a moderate severity warning and may require user action. Could cause degraded statements if certain conditions apply.
	//
	// CRITICAL: Indicates a critical severity warning and requires user action. It will cause degraded statements eventually.
	Severity SqlV1WarningSeverity `json:"severity"`
}
type SqlV1Tool struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SqlV1ToolApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SqlV1ToolKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the tool, unique within this environment.
	Name *string `json:"name,omitempty"`

	// Spec The spec of the Tool. A tool must reference either a `connection` (for MCP or A2A tools)
	// or a `function` (for function-based tools), but not both.
	Spec *SqlV1ToolSpec `json:"spec,omitempty"`

	// Status The status of the Tool
	Status *SqlV1ToolStatus `json:"status,omitempty"`
}
type SqlV1ToolApiVersion string
type SqlV1ToolKind string
type SqlV1ToolList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1ToolListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion SqlV1ToolListDataApiVersion `json:"api_version"`

		// Kind Kind defines the object this REST resource represents.
		Kind     SqlV1ToolListDataKind `json:"kind"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
			ResourceName    interface{} `json:"resource_name,omitempty"`
			ResourceVersion interface{} `json:"resource_version,omitempty"`
			Self            interface{} `json:"self"`
			Uid             interface{} `json:"uid,omitempty"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Name The user provided name of the tool, unique within this environment.
		Name string `json:"name"`

		// Spec The spec of the Tool. A tool must reference either a `connection` (for MCP or A2A tools)
		// or a `function` (for function-based tools), but not both.
		Spec SqlV1ToolSpec `json:"spec"`

		// Status The status of the Tool
		Status SqlV1ToolStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1ToolListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`
		Self  interface{} `json:"self,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SqlV1ToolListApiVersion string
type SqlV1ToolListDataApiVersion string
type SqlV1ToolListDataKind string
type SqlV1ToolListKind string
type SqlV1ToolSpec struct {
	// Comment An optional comment describing the tool.
	Comment *string `json:"comment,omitempty"`

	// Connection The name of the connection this tool uses. Required for MCP and A2A tools. Mutually exclusive with function.
	Connection *string `json:"connection,omitempty"`

	// Function The name of the function this tool wraps. Required for function-based tools. Mutually exclusive with connection.
	Function *string `json:"function,omitempty"`

	// Options A set of key-value option pairs that configure the tool's behavior.
	// Supported options vary by tool type:
	// - MCP tools: type, allowed_tools, request_timeout, max_retries, headers
	// - A2A tools: type, agent_card_path, request_timeout, max_retries
	// - Function tools: type, description
	Options *map[string]string `json:"options,omitempty"`
}
type SqlV1ToolStatus struct {
	// Detail Details about why the tool transitioned into a given status.
	Detail *string `json:"detail,omitempty"`

	// Phase Describes the status of the tool:
	//
	// ACTIVE: The Tool is usable;
	//
	// INACTIVE: The Tool is not currently active;
	//
	// ERROR: The Tool encountered an error;
	Phase string `json:"phase"`
}
type SqlV1WarningSeverity = string
type SqlV1Watermark struct {
	Column     *string `json:"column,omitempty"`
	Expression *string `json:"expression,omitempty"`
}
type ClusterId = string
type ConfigName = string
type ConsumerGroupId = string
type ConsumerId = string
type GroupId = string
type IncludePartitionLevelTruncationData = bool
type LinkConfigName = string
type LinkName = string
type MemberId = string
type MirrorTopicName = string
type PartitionId = int
type SubtopologyId = string
type TopicName = string
type ValidateOnly = bool
type BadRequestError = Failure
type ConflictError = Failure
type DefaultSystemError = Failure
type NotFoundError = Failure
type UnauthenticatedError = Failure
type UnauthorizedError = Failure
type ValidationError = Failure

func (t ArtifactV1FlinkArtifactVersion_UploadSource) AsArtifactV1UploadSourcePresignedUrl() (ArtifactV1UploadSourcePresignedUrl, error) {
	var body ArtifactV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) FromArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) MergeArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsArtifactV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t QueryV1alpha1ResultValue) AsQueryV1alpha1ResultValue0() (QueryV1alpha1ResultValue0, error) {
	var body QueryV1alpha1ResultValue0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *QueryV1alpha1ResultValue) FromQueryV1alpha1ResultValue0(v QueryV1alpha1ResultValue0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *QueryV1alpha1ResultValue) MergeQueryV1alpha1ResultValue0(v QueryV1alpha1ResultValue0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t QueryV1alpha1ResultValue) AsQueryV1alpha1ResultValue1() (QueryV1alpha1ResultValue1, error) {
	var body QueryV1alpha1ResultValue1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *QueryV1alpha1ResultValue) FromQueryV1alpha1ResultValue1(v QueryV1alpha1ResultValue1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *QueryV1alpha1ResultValue) MergeQueryV1alpha1ResultValue1(v QueryV1alpha1ResultValue1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t QueryV1alpha1ResultValue) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *QueryV1alpha1ResultValue) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t SqlV1ColumnDetails) AsSqlV1PhysicalColumn() (SqlV1PhysicalColumn, error) {
	var body SqlV1PhysicalColumn
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *SqlV1ColumnDetails) FromSqlV1PhysicalColumn(v SqlV1PhysicalColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Physical"}`))
	t.union = b
	return err
}
func (t *SqlV1ColumnDetails) MergeSqlV1PhysicalColumn(v SqlV1PhysicalColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Physical"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t SqlV1ColumnDetails) AsSqlV1MetadataColumn() (SqlV1MetadataColumn, error) {
	var body SqlV1MetadataColumn
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *SqlV1ColumnDetails) FromSqlV1MetadataColumn(v SqlV1MetadataColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Metadata"}`))
	t.union = b
	return err
}
func (t *SqlV1ColumnDetails) MergeSqlV1MetadataColumn(v SqlV1MetadataColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Metadata"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t SqlV1ColumnDetails) AsSqlV1ComputedColumn() (SqlV1ComputedColumn, error) {
	var body SqlV1ComputedColumn
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *SqlV1ColumnDetails) FromSqlV1ComputedColumn(v SqlV1ComputedColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Computed"}`))
	t.union = b
	return err
}
func (t *SqlV1ColumnDetails) MergeSqlV1ComputedColumn(v SqlV1ComputedColumn) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Computed"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t SqlV1ColumnDetails) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t SqlV1ColumnDetails) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Computed":
		return t.AsSqlV1ComputedColumn()
	case "Metadata":
		return t.AsSqlV1MetadataColumn()
	case "Physical":
		return t.AsSqlV1PhysicalColumn()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t SqlV1ColumnDetails) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *SqlV1ColumnDetails) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t SqlV1ConnectionSpec_AuthData) AsSqlV1PlaintextProvider() (SqlV1PlaintextProvider, error) {
	var body SqlV1PlaintextProvider
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *SqlV1ConnectionSpec_AuthData) FromSqlV1PlaintextProvider(v SqlV1PlaintextProvider) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"PlaintextProvider"}`))
	t.union = b
	return err
}
func (t *SqlV1ConnectionSpec_AuthData) MergeSqlV1PlaintextProvider(v SqlV1PlaintextProvider) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"PlaintextProvider"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t SqlV1ConnectionSpec_AuthData) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t SqlV1ConnectionSpec_AuthData) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PlaintextProvider":
		return t.AsSqlV1PlaintextProvider()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t SqlV1ConnectionSpec_AuthData) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *SqlV1ConnectionSpec_AuthData) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}

type RequestEditorFn func(ctx context.Context, req *http.Request) error
type HttpRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}
type Client struct {
	// The endpoint of the server conforming to this interface, with scheme,
	// https://api.deepmap.com for example. This can contain a path relative
	// to the server, such as https://api.deepmap.com/dev-test, and all the
	// paths in the swagger spec will be appended to the server.
	Server string

	// Doer for performing requests, typically a *http.Client with any
	// customized settings, such as certificate chains.
	Client HttpRequestDoer

	// A list of callbacks for modifying requests which are generated before sending over
	// the network.
	RequestEditors []RequestEditorFn
}
type ClientOption func(*Client) error

func NewClient(server string, opts ...ClientOption) (*Client, error) {
	// create a client with sane default values
	client := Client{
		Server: server,
	}
	// mutate client and add all optional params
	for _, o := range opts {
		if err := o(&client); err != nil {
			return nil, err
		}
	}
	// ensure the server URL always has a trailing slash
	if !strings.HasSuffix(client.Server, "/") {
		client.Server += "/"
	}
	// create httpClient, if not already present
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	return &client, nil
}
func WithHTTPClient(doer HttpRequestDoer) ClientOption {
	return func(c *Client) error {
		c.Client = doer
		return nil
	}
}
func WithRequestEditorFn(fn RequestEditorFn) ClientOption {
	return func(c *Client) error {
		c.RequestEditors = append(c.RequestEditors, fn)
		return nil
	}
}

type ClientInterface interface {

	// ListSqlv1Agents List all agents
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all agents.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/agents (the `ListSqlv1Agents` operationId).
	ListSqlv1Agents(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1AgentsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSqlv1Connections List of Connections
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered and paginated list of all Connections.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `ListSqlv1Connections` operationId).
	ListSqlv1Connections(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1ConnectionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1ConnectionWithBody Create a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Connection.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `CreateSqlv1Connection` operationId).
	CreateSqlv1ConnectionWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1Connection Create a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Connection.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `CreateSqlv1Connection` operationId).
	CreateSqlv1Connection(ctx context.Context, organizationId openapi_types.UUID, environmentId string, body CreateSqlv1ConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSqlv1Connection Delete a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a statement.
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `DeleteSqlv1Connection` operationId).
	DeleteSqlv1Connection(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1Connection Read a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a Connection.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `GetSqlv1Connection` operationId).
	GetSqlv1Connection(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1ConnectionWithBody Update a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a connection.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `UpdateSqlv1Connection` operationId).
	UpdateSqlv1ConnectionWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1Connection Update a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a connection.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `UpdateSqlv1Connection` operationId).
	UpdateSqlv1Connection(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, body UpdateSqlv1ConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSqlv1Tools List of Tools
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all Tools.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `ListSqlv1Tools` operationId).
	ListSqlv1Tools(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, params *ListSqlv1ToolsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1ToolWithBody Create a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Tool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `CreateSqlv1Tool` operationId).
	CreateSqlv1ToolWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1Tool Create a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Tool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `CreateSqlv1Tool` operationId).
	CreateSqlv1Tool(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, body CreateSqlv1ToolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSqlv1Tool Delete a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a Tool.
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools/{tool_name} (the `DeleteSqlv1Tool` operationId).
	DeleteSqlv1Tool(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, toolName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1Tool Read a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a Tool.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools/{tool_name} (the `GetSqlv1Tool` operationId).
	GetSqlv1Tool(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, toolName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1AgentWithBody Create an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an Agent.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents (the `CreateSqlv1Agent` operationId).
	CreateSqlv1AgentWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1Agent Create an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an Agent.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents (the `CreateSqlv1Agent` operationId).
	CreateSqlv1Agent(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, body CreateSqlv1AgentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSqlv1Agent Delete an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a specific Agent by name.
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `DeleteSqlv1Agent` operationId).
	DeleteSqlv1Agent(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1Agent Read an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific Agent by name.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `GetSqlv1Agent` operationId).
	GetSqlv1Agent(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1AgentWithBody Alter an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an Agent's mutable fields.
	// Mutable fields include: `description`, `model`, `prompt`, and `properties`.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `UpdateSqlv1Agent` operationId).
	UpdateSqlv1AgentWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1Agent Alter an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an Agent's mutable fields.
	// Mutable fields include: `description`, `model`, `prompt`, and `properties`.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `UpdateSqlv1Agent` operationId).
	UpdateSqlv1Agent(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, body UpdateSqlv1AgentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1MaterializedTableWithBody Create a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new Materialized Table.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables (the `CreateSqlv1MaterializedTable` operationId).
	CreateSqlv1MaterializedTableWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1MaterializedTable Create a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new Materialized Table.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables (the `CreateSqlv1MaterializedTable` operationId).
	CreateSqlv1MaterializedTable(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, body CreateSqlv1MaterializedTableJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSqlv1MaterializedTable Delete a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a specific Materialized Table by name.
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `DeleteSqlv1MaterializedTable` operationId).
	DeleteSqlv1MaterializedTable(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1MaterializedTable Read a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific Materialized Table by name.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `GetSqlv1MaterializedTable` operationId).
	GetSqlv1MaterializedTable(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1MaterializedTableWithBody Update/Evolve a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a Materialized Table's mutable fields.
	// Mutable fields include: `query`, `stopped`, `compute_pool_id`, `principal`, `columns`, `watermark`, `constraints` and `table_options`.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `UpdateSqlv1MaterializedTable` operationId).
	UpdateSqlv1MaterializedTableWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1MaterializedTable Update/Evolve a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a Materialized Table's mutable fields.
	// Mutable fields include: `query`, `stopped`, `compute_pool_id`, `principal`, `columns`, `watermark`, `constraints` and `table_options`.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `UpdateSqlv1MaterializedTable` operationId).
	UpdateSqlv1MaterializedTable(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, body UpdateSqlv1MaterializedTableJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSqlv1MaterializedTableVersions List all the versions of a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all versions for a specific Materialized Table.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name}/versions (the `ListSqlv1MaterializedTableVersions` operationId).
	ListSqlv1MaterializedTableVersions(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, params *ListSqlv1MaterializedTableVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1MaterializedTableVersion Read a materialized table version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific version of a Materialized Table.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name}/versions/{version} (the `GetSqlv1MaterializedTableVersion` operationId).
	GetSqlv1MaterializedTableVersion(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, version int32, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSqlv1MaterializedTables List all materialized tables
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all materialized tables.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/materialized-tables (the `ListSqlv1MaterializedTables` operationId).
	ListSqlv1MaterializedTables(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1MaterializedTablesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1StatementResult Read Statement Result
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Read Statement Result.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{name}/results (the `GetSqlv1StatementResult` operationId).
	GetSqlv1StatementResult(ctx context.Context, organizationId openapi_types.UUID, environmentId string, name string, params *GetSqlv1StatementResultParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1StatementExceptions List of Statement Exceptions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a list of the 10 most recent statement exceptions.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name}/exceptions (the `GetSqlv1StatementExceptions` operationId).
	GetSqlv1StatementExceptions(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *Client) applyEditors(ctx context.Context, req *http.Request, additionalEditors []RequestEditorFn) error {
	for _, r := range c.RequestEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	for _, r := range additionalEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

type ClientWithResponses struct {
	ClientInterface
}

func NewClientWithResponses(server string, opts ...ClientOption) (*ClientWithResponses, error) {
	client, err := NewClient(server, opts...)
	if err != nil {
		return nil, err
	}
	return &ClientWithResponses{client}, nil
}
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) error {
		newBaseURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}
		c.Server = newBaseURL.String()
		return nil
	}
}

type ClientWithResponsesInterface interface {

	// ListSqlv1AgentsWithResponse List all agents
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all agents.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/agents (the `ListSqlv1Agents` operationId).
	ListSqlv1AgentsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1AgentsParams, reqEditors ...RequestEditorFn) (*ListSqlv1AgentsResponse, error)

	// ListSqlv1ConnectionsWithResponse List of Connections
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered and paginated list of all Connections.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `ListSqlv1Connections` operationId).
	ListSqlv1ConnectionsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1ConnectionsParams, reqEditors ...RequestEditorFn) (*ListSqlv1ConnectionsResponse, error)

	// CreateSqlv1ConnectionWithBodyWithResponse Create a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Connection.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `CreateSqlv1Connection` operationId).
	CreateSqlv1ConnectionWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateSqlv1ConnectionResponse, error)

	// CreateSqlv1ConnectionWithResponse Create a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Connection.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections (the `CreateSqlv1Connection` operationId).
	CreateSqlv1ConnectionWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, body CreateSqlv1ConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateSqlv1ConnectionResponse, error)

	// DeleteSqlv1ConnectionWithResponse Delete a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a statement.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `DeleteSqlv1Connection` operationId).
	DeleteSqlv1ConnectionWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, reqEditors ...RequestEditorFn) (*DeleteSqlv1ConnectionResponse, error)

	// GetSqlv1ConnectionWithResponse Read a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a Connection.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `GetSqlv1Connection` operationId).
	GetSqlv1ConnectionWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, reqEditors ...RequestEditorFn) (*GetSqlv1ConnectionResponse, error)

	// UpdateSqlv1ConnectionWithBodyWithResponse Update a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a connection.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `UpdateSqlv1Connection` operationId).
	UpdateSqlv1ConnectionWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateSqlv1ConnectionResponse, error)

	// UpdateSqlv1ConnectionWithResponse Update a Connection
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a connection.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/connections/{connection_name} (the `UpdateSqlv1Connection` operationId).
	UpdateSqlv1ConnectionWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, connectionName string, body UpdateSqlv1ConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateSqlv1ConnectionResponse, error)

	// ListSqlv1ToolsWithResponse List of Tools
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all Tools.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `ListSqlv1Tools` operationId).
	ListSqlv1ToolsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, params *ListSqlv1ToolsParams, reqEditors ...RequestEditorFn) (*ListSqlv1ToolsResponse, error)

	// CreateSqlv1ToolWithBodyWithResponse Create a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Tool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `CreateSqlv1Tool` operationId).
	CreateSqlv1ToolWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateSqlv1ToolResponse, error)

	// CreateSqlv1ToolWithResponse Create a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a Tool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools (the `CreateSqlv1Tool` operationId).
	CreateSqlv1ToolWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, body CreateSqlv1ToolJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateSqlv1ToolResponse, error)

	// DeleteSqlv1ToolWithResponse Delete a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a Tool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools/{tool_name} (the `DeleteSqlv1Tool` operationId).
	DeleteSqlv1ToolWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, toolName string, reqEditors ...RequestEditorFn) (*DeleteSqlv1ToolResponse, error)

	// GetSqlv1ToolWithResponse Read a Tool
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a Tool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{database_name}/tools/{tool_name} (the `GetSqlv1Tool` operationId).
	GetSqlv1ToolWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, databaseName string, toolName string, reqEditors ...RequestEditorFn) (*GetSqlv1ToolResponse, error)

	// CreateSqlv1AgentWithBodyWithResponse Create an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an Agent.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents (the `CreateSqlv1Agent` operationId).
	CreateSqlv1AgentWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateSqlv1AgentResponse, error)

	// CreateSqlv1AgentWithResponse Create an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an Agent.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents (the `CreateSqlv1Agent` operationId).
	CreateSqlv1AgentWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, body CreateSqlv1AgentJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateSqlv1AgentResponse, error)

	// DeleteSqlv1AgentWithResponse Delete an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a specific Agent by name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `DeleteSqlv1Agent` operationId).
	DeleteSqlv1AgentWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, reqEditors ...RequestEditorFn) (*DeleteSqlv1AgentResponse, error)

	// GetSqlv1AgentWithResponse Read an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific Agent by name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `GetSqlv1Agent` operationId).
	GetSqlv1AgentWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, reqEditors ...RequestEditorFn) (*GetSqlv1AgentResponse, error)

	// UpdateSqlv1AgentWithBodyWithResponse Alter an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an Agent's mutable fields.
	// Mutable fields include: `description`, `model`, `prompt`, and `properties`.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `UpdateSqlv1Agent` operationId).
	UpdateSqlv1AgentWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateSqlv1AgentResponse, error)

	// UpdateSqlv1AgentWithResponse Alter an Agent
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300af91)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an Agent's mutable fields.
	// Mutable fields include: `description`, `model`, `prompt`, and `properties`.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/agents/{agent_name} (the `UpdateSqlv1Agent` operationId).
	UpdateSqlv1AgentWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, agentName string, body UpdateSqlv1AgentJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateSqlv1AgentResponse, error)

	// CreateSqlv1MaterializedTableWithBodyWithResponse Create a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new Materialized Table.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables (the `CreateSqlv1MaterializedTable` operationId).
	CreateSqlv1MaterializedTableWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateSqlv1MaterializedTableResponse, error)

	// CreateSqlv1MaterializedTableWithResponse Create a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new Materialized Table.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables (the `CreateSqlv1MaterializedTable` operationId).
	CreateSqlv1MaterializedTableWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, body CreateSqlv1MaterializedTableJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateSqlv1MaterializedTableResponse, error)

	// DeleteSqlv1MaterializedTableWithResponse Delete a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a specific Materialized Table by name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `DeleteSqlv1MaterializedTable` operationId).
	DeleteSqlv1MaterializedTableWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, reqEditors ...RequestEditorFn) (*DeleteSqlv1MaterializedTableResponse, error)

	// GetSqlv1MaterializedTableWithResponse Read a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific Materialized Table by name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `GetSqlv1MaterializedTable` operationId).
	GetSqlv1MaterializedTableWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, reqEditors ...RequestEditorFn) (*GetSqlv1MaterializedTableResponse, error)

	// UpdateSqlv1MaterializedTableWithBodyWithResponse Update/Evolve a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a Materialized Table's mutable fields.
	// Mutable fields include: `query`, `stopped`, `compute_pool_id`, `principal`, `columns`, `watermark`, `constraints` and `table_options`.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `UpdateSqlv1MaterializedTable` operationId).
	UpdateSqlv1MaterializedTableWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateSqlv1MaterializedTableResponse, error)

	// UpdateSqlv1MaterializedTableWithResponse Update/Evolve a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a Materialized Table's mutable fields.
	// Mutable fields include: `query`, `stopped`, `compute_pool_id`, `principal`, `columns`, `watermark`, `constraints` and `table_options`.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name} (the `UpdateSqlv1MaterializedTable` operationId).
	UpdateSqlv1MaterializedTableWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, body UpdateSqlv1MaterializedTableJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateSqlv1MaterializedTableResponse, error)

	// ListSqlv1MaterializedTableVersionsWithResponse List all the versions of a materialized table
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all versions for a specific Materialized Table.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name}/versions (the `ListSqlv1MaterializedTableVersions` operationId).
	ListSqlv1MaterializedTableVersionsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, params *ListSqlv1MaterializedTableVersionsParams, reqEditors ...RequestEditorFn) (*ListSqlv1MaterializedTableVersionsResponse, error)

	// GetSqlv1MaterializedTableVersionWithResponse Read a materialized table version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a specific version of a Materialized Table.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/databases/{kafka_cluster_id}/materialized-tables/{table_name}/versions/{version} (the `GetSqlv1MaterializedTableVersion` operationId).
	GetSqlv1MaterializedTableVersionWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, kafkaClusterId string, tableName string, version int32, reqEditors ...RequestEditorFn) (*GetSqlv1MaterializedTableVersionResponse, error)

	// ListSqlv1MaterializedTablesWithResponse List all materialized tables
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted and paginated list of all materialized tables.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/materialized-tables (the `ListSqlv1MaterializedTables` operationId).
	ListSqlv1MaterializedTablesWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1MaterializedTablesParams, reqEditors ...RequestEditorFn) (*ListSqlv1MaterializedTablesResponse, error)

	// GetSqlv1StatementResultWithResponse Read Statement Result
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Read Statement Result.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{name}/results (the `GetSqlv1StatementResult` operationId).
	GetSqlv1StatementResultWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, name string, params *GetSqlv1StatementResultParams, reqEditors ...RequestEditorFn) (*GetSqlv1StatementResultResponse, error)

	// GetSqlv1StatementExceptionsWithResponse List of Statement Exceptions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a list of the 10 most recent statement exceptions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name}/exceptions (the `GetSqlv1StatementExceptions` operationId).
	GetSqlv1StatementExceptionsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*GetSqlv1StatementExceptionsResponse, error)
}

func (r ListSqlv1AgentsResponse) GetJSON200() *SqlV1AgentList {
	return r.JSON200
}
func (r ListSqlv1AgentsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1AgentsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1AgentsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1AgentsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1AgentsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1AgentsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1AgentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1AgentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1AgentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSqlv1ConnectionsResponse) GetJSON200() *SqlV1ConnectionList {
	return r.JSON200
}
func (r ListSqlv1ConnectionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1ConnectionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1ConnectionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1ConnectionsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1ConnectionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1ConnectionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1ConnectionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1ConnectionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1ConnectionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSqlv1ConnectionResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateSqlv1Connection201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateSqlv1Connection201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string                `json:"name,omitempty"`
	Spec map[string]interface{} `json:"spec"`

	// Status The status of the Connection
	Status *SqlV1ConnectionStatus `json:"status,omitempty"`
} {
	return r.JSON201
}
func (r CreateSqlv1ConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSqlv1ConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSqlv1ConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSqlv1ConnectionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSqlv1ConnectionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSqlv1ConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSqlv1ConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSqlv1ConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSqlv1ConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSqlv1ConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSqlv1ConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSqlv1ConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSqlv1ConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSqlv1ConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSqlv1ConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSqlv1ConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSqlv1ConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSqlv1ConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSqlv1ConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1ConnectionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSqlv1Connection200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSqlv1Connection200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string                `json:"name,omitempty"`
	Spec map[string]interface{} `json:"spec"`

	// Status The status of the Connection
	Status *SqlV1ConnectionStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r GetSqlv1ConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1ConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1ConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1ConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1ConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1ConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1ConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1ConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1ConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateSqlv1ConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateSqlv1ConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateSqlv1ConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateSqlv1ConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateSqlv1ConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateSqlv1ConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateSqlv1ConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateSqlv1ConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateSqlv1ConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSqlv1ToolsResponse) GetJSON200() *SqlV1ToolList {
	return r.JSON200
}
func (r ListSqlv1ToolsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1ToolsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1ToolsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1ToolsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1ToolsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1ToolsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1ToolsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1ToolsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1ToolsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSqlv1ToolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateSqlv1Tool200JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateSqlv1Tool200JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the tool, unique within this environment.
	Name string `json:"name"`

	// Spec The spec of the Tool. A tool must reference either a `connection` (for MCP or A2A tools)
	// or a `function` (for function-based tools), but not both.
	Spec SqlV1ToolSpec `json:"spec"`

	// Status The status of the Tool
	Status *SqlV1ToolStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r CreateSqlv1ToolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSqlv1ToolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSqlv1ToolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSqlv1ToolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSqlv1ToolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSqlv1ToolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSqlv1ToolResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSqlv1ToolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSqlv1ToolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSqlv1ToolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSqlv1ToolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSqlv1ToolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSqlv1ToolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSqlv1ToolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSqlv1ToolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSqlv1ToolResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSqlv1ToolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSqlv1ToolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSqlv1ToolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1ToolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSqlv1Tool200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSqlv1Tool200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time  `json:"deleted_at,omitempty"`
		ResourceName    interface{} `json:"resource_name,omitempty"`
		ResourceVersion interface{} `json:"resource_version,omitempty"`
		Self            interface{} `json:"self"`
		Uid             interface{} `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user provided name of the tool, unique within this environment.
	Name string `json:"name"`

	// Spec The spec of the Tool. A tool must reference either a `connection` (for MCP or A2A tools)
	// or a `function` (for function-based tools), but not both.
	Spec SqlV1ToolSpec `json:"spec"`

	// Status The status of the Tool
	Status *SqlV1ToolStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r GetSqlv1ToolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1ToolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1ToolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1ToolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1ToolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1ToolResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1ToolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1ToolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1ToolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSqlv1AgentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CreateSqlv1Agent200JSONResponseBodyApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId string `json:"environment_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CreateSqlv1Agent200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time         `json:"deleted_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user-provided name of the agent, unique within this environment.
	Name string `json:"name"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId openapi_types.UUID     `json:"organization_id"`
	Spec           map[string]interface{} `json:"spec"`
	Status         *SqlV1AgentStatus      `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r CreateSqlv1AgentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSqlv1AgentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSqlv1AgentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSqlv1AgentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSqlv1AgentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSqlv1AgentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSqlv1AgentResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSqlv1AgentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSqlv1AgentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSqlv1AgentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSqlv1AgentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSqlv1AgentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSqlv1AgentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSqlv1AgentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSqlv1AgentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSqlv1AgentResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSqlv1AgentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSqlv1AgentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSqlv1AgentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1AgentResponse) GetJSON200() *SqlV1Agent {
	return r.JSON200
}
func (r GetSqlv1AgentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1AgentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1AgentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1AgentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1AgentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1AgentResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1AgentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1AgentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1AgentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateSqlv1AgentResponse) GetJSON200() *SqlV1Agent {
	return r.JSON200
}
func (r UpdateSqlv1AgentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateSqlv1AgentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateSqlv1AgentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateSqlv1AgentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateSqlv1AgentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateSqlv1AgentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateSqlv1AgentResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateSqlv1AgentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateSqlv1AgentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateSqlv1AgentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CreateSqlv1MaterializedTable201JSONResponseBodyApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId string `json:"environment_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CreateSqlv1MaterializedTable201JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt       *time.Time         `json:"deleted_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user-provided name of the resource, unique within the Kafka cluster. May contain ASCII alphanumerics, '.', '_' and '-'; must not be '.' or '..'; max length 249.
	Name string `json:"name"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId openapi_types.UUID           `json:"organization_id"`
	Spec           map[string]interface{}       `json:"spec"`
	Status         SqlV1MaterializedTableStatus `json:"status"`
} {
	return r.JSON201
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSqlv1MaterializedTableResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSqlv1MaterializedTableResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSqlv1MaterializedTableResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSqlv1MaterializedTableResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSqlv1MaterializedTableResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSqlv1MaterializedTableResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSqlv1MaterializedTableResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSqlv1MaterializedTableResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSqlv1MaterializedTableResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSqlv1MaterializedTableResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSqlv1MaterializedTableResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSqlv1MaterializedTableResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSqlv1MaterializedTableResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSqlv1MaterializedTableResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1MaterializedTableResponse) GetJSON200() *SqlV1MaterializedTable {
	return r.JSON200
}
func (r GetSqlv1MaterializedTableResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1MaterializedTableResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1MaterializedTableResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1MaterializedTableResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1MaterializedTableResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1MaterializedTableResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1MaterializedTableResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1MaterializedTableResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1MaterializedTableResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON200() *SqlV1MaterializedTable {
	return r.JSON200
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateSqlv1MaterializedTableResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateSqlv1MaterializedTableResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateSqlv1MaterializedTableResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateSqlv1MaterializedTableResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateSqlv1MaterializedTableResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON200() *SqlV1MaterializedTableVersionList {
	return r.JSON200
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1MaterializedTableVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1MaterializedTableVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1MaterializedTableVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1MaterializedTableVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON200() *SqlV1MaterializedTableVersion {
	return r.JSON200
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1MaterializedTableVersionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1MaterializedTableVersionResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1MaterializedTableVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1MaterializedTableVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1MaterializedTableVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON200() *SqlV1MaterializedTableList {
	return r.JSON200
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1MaterializedTablesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1MaterializedTablesResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1MaterializedTablesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1MaterializedTablesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1MaterializedTablesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1StatementResultResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSqlv1StatementResult200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSqlv1StatementResult200JSONResponseBodyKind `json:"kind"`
	Metadata ResultListMeta                                 `json:"metadata"`
	Results  map[string]interface{}                         `json:"results"`
} {
	return r.JSON200
}
func (r GetSqlv1StatementResultResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1StatementResultResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1StatementResultResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1StatementResultResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1StatementResultResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1StatementResultResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1StatementResultResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1StatementResultResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1StatementResultResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON200() *SqlV1StatementExceptionList {
	return r.JSON200
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1StatementExceptionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1StatementExceptionsResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1StatementExceptionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1StatementExceptionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1StatementExceptionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
