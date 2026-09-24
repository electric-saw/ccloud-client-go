package tableflow

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
	AzureDataLakeStorageGen2 TableflowV1AzureAdlsSpecKind = "AzureDataLakeStorageGen2"
)

func (e TableflowV1AzureAdlsSpecKind) Valid() bool {
	switch e {
	case AzureDataLakeStorageGen2:
		return true
	default:
		return false
	}
}

const (
	ByobAws TableflowV1ByobAwsSpecKind = "ByobAws"
)

func (e TableflowV1ByobAwsSpecKind) Valid() bool {
	switch e {
	case ByobAws:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationApiVersionTableflowv1 TableflowV1CatalogIntegrationApiVersion = "tableflow/v1"
)

func (e TableflowV1CatalogIntegrationApiVersion) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationKindCatalogIntegration TableflowV1CatalogIntegrationKind = "CatalogIntegration"
)

func (e TableflowV1CatalogIntegrationKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationAwsGlueSpecKindAwsGlue TableflowV1CatalogIntegrationAwsGlueSpecKind = "AwsGlue"
)

func (e TableflowV1CatalogIntegrationAwsGlueSpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationAwsGlueSpecKindAwsGlue:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationAwsGlueUpdateSpecKindAwsGlue TableflowV1CatalogIntegrationAwsGlueUpdateSpecKind = "AwsGlue"
)

func (e TableflowV1CatalogIntegrationAwsGlueUpdateSpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationAwsGlueUpdateSpecKindAwsGlue:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationListApiVersionTableflowv1 TableflowV1CatalogIntegrationListApiVersion = "tableflow/v1"
)

func (e TableflowV1CatalogIntegrationListApiVersion) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationListApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationListDataApiVersionTableflowv1 TableflowV1CatalogIntegrationListDataApiVersion = "tableflow/v1"
)

func (e TableflowV1CatalogIntegrationListDataApiVersion) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationListDataApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationListDataKindCatalogIntegration TableflowV1CatalogIntegrationListDataKind = "CatalogIntegration"
)

func (e TableflowV1CatalogIntegrationListDataKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationListDataKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationListKindCatalogIntegrationList TableflowV1CatalogIntegrationListKind = "CatalogIntegrationList"
)

func (e TableflowV1CatalogIntegrationListKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationListKindCatalogIntegrationList:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationSnowflakeSpecKindSnowflake TableflowV1CatalogIntegrationSnowflakeSpecKind = "Snowflake"
)

func (e TableflowV1CatalogIntegrationSnowflakeSpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationSnowflakeSpecKindSnowflake:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationSnowflakeUpdateSpecKindSnowflake TableflowV1CatalogIntegrationSnowflakeUpdateSpecKind = "Snowflake"
)

func (e TableflowV1CatalogIntegrationSnowflakeUpdateSpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationSnowflakeUpdateSpecKindSnowflake:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationUnitySpecKindUnity TableflowV1CatalogIntegrationUnitySpecKind = "Unity"
)

func (e TableflowV1CatalogIntegrationUnitySpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationUnitySpecKindUnity:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationUnityUpdateSpecKindUnity TableflowV1CatalogIntegrationUnityUpdateSpecKind = "Unity"
)

func (e TableflowV1CatalogIntegrationUnityUpdateSpecKind) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationUnityUpdateSpecKindUnity:
		return true
	default:
		return false
	}
}

const (
	TableflowV1CatalogIntegrationUpdateRequestApiVersionTableflowv1 TableflowV1CatalogIntegrationUpdateRequestApiVersion = "tableflow/v1"
)

func (e TableflowV1CatalogIntegrationUpdateRequestApiVersion) Valid() bool {
	switch e {
	case TableflowV1CatalogIntegrationUpdateRequestApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	CatalogIntegrationUpdateRequest TableflowV1CatalogIntegrationUpdateRequestKind = "CatalogIntegrationUpdateRequest"
)

func (e TableflowV1CatalogIntegrationUpdateRequestKind) Valid() bool {
	switch e {
	case CatalogIntegrationUpdateRequest:
		return true
	default:
		return false
	}
}

const (
	LOG TableflowV1ErrorHandlingLogMode = "LOG"
)

func (e TableflowV1ErrorHandlingLogMode) Valid() bool {
	switch e {
	case LOG:
		return true
	default:
		return false
	}
}

const (
	SKIP TableflowV1ErrorHandlingSkipMode = "SKIP"
)

func (e TableflowV1ErrorHandlingSkipMode) Valid() bool {
	switch e {
	case SKIP:
		return true
	default:
		return false
	}
}

const (
	SUSPEND TableflowV1ErrorHandlingSuspendMode = "SUSPEND"
)

func (e TableflowV1ErrorHandlingSuspendMode) Valid() bool {
	switch e {
	case SUSPEND:
		return true
	default:
		return false
	}
}

const (
	Managed TableflowV1ManagedStorageSpecKind = "Managed"
)

func (e TableflowV1ManagedStorageSpecKind) Valid() bool {
	switch e {
	case Managed:
		return true
	default:
		return false
	}
}

const (
	TableflowV1RegionApiVersionTableflowv1 TableflowV1RegionApiVersion = "tableflow/v1"
)

func (e TableflowV1RegionApiVersion) Valid() bool {
	switch e {
	case TableflowV1RegionApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1RegionKindRegion TableflowV1RegionKind = "Region"
)

func (e TableflowV1RegionKind) Valid() bool {
	switch e {
	case TableflowV1RegionKindRegion:
		return true
	default:
		return false
	}
}

const (
	TableflowV1RegionListApiVersionTableflowv1 TableflowV1RegionListApiVersion = "tableflow/v1"
)

func (e TableflowV1RegionListApiVersion) Valid() bool {
	switch e {
	case TableflowV1RegionListApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1RegionListDataApiVersionTableflowv1 TableflowV1RegionListDataApiVersion = "tableflow/v1"
)

func (e TableflowV1RegionListDataApiVersion) Valid() bool {
	switch e {
	case TableflowV1RegionListDataApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1RegionListDataKindRegion TableflowV1RegionListDataKind = "Region"
)

func (e TableflowV1RegionListDataKind) Valid() bool {
	switch e {
	case TableflowV1RegionListDataKindRegion:
		return true
	default:
		return false
	}
}

const (
	RegionList TableflowV1RegionListKind = "RegionList"
)

func (e TableflowV1RegionListKind) Valid() bool {
	switch e {
	case RegionList:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicApiVersionTableflowv1 TableflowV1TableflowTopicApiVersion = "tableflow/v1"
)

func (e TableflowV1TableflowTopicApiVersion) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicKindTableflowTopic TableflowV1TableflowTopicKind = "TableflowTopic"
)

func (e TableflowV1TableflowTopicKind) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicListApiVersionTableflowv1 TableflowV1TableflowTopicListApiVersion = "tableflow/v1"
)

func (e TableflowV1TableflowTopicListApiVersion) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicListApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicListDataApiVersionTableflowv1 TableflowV1TableflowTopicListDataApiVersion = "tableflow/v1"
)

func (e TableflowV1TableflowTopicListDataApiVersion) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicListDataApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicListDataKindTableflowTopic TableflowV1TableflowTopicListDataKind = "TableflowTopic"
)

func (e TableflowV1TableflowTopicListDataKind) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicListDataKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	TableflowV1TableflowTopicListKindTableflowTopicList TableflowV1TableflowTopicListKind = "TableflowTopicList"
)

func (e TableflowV1TableflowTopicListKind) Valid() bool {
	switch e {
	case TableflowV1TableflowTopicListKindTableflowTopicList:
		return true
	default:
		return false
	}
}

const (
	ListTableflowV1CatalogIntegrations200JSONResponseBodyApiVersionTableflowv1 ListTableflowV1CatalogIntegrations200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e ListTableflowV1CatalogIntegrations200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListTableflowV1CatalogIntegrations200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	ListTableflowV1CatalogIntegrations200JSONResponseBodyKindCatalogIntegrationList ListTableflowV1CatalogIntegrations200JSONResponseBodyKind = "CatalogIntegrationList"
)

func (e ListTableflowV1CatalogIntegrations200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListTableflowV1CatalogIntegrations200JSONResponseBodyKindCatalogIntegrationList:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1CatalogIntegrationJSONBodyApiVersionTableflowv1 CreateTableflowV1CatalogIntegrationJSONBodyApiVersion = "tableflow/v1"
)

func (e CreateTableflowV1CatalogIntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateTableflowV1CatalogIntegrationJSONBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1CatalogIntegrationJSONBodyKindCatalogIntegration CreateTableflowV1CatalogIntegrationJSONBodyKind = "CatalogIntegration"
)

func (e CreateTableflowV1CatalogIntegrationJSONBodyKind) Valid() bool {
	switch e {
	case CreateTableflowV1CatalogIntegrationJSONBodyKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1CatalogIntegration202JSONResponseBodyApiVersionTableflowv1 CreateTableflowV1CatalogIntegration202JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e CreateTableflowV1CatalogIntegration202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateTableflowV1CatalogIntegration202JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1CatalogIntegration202JSONResponseBodyKindCatalogIntegration CreateTableflowV1CatalogIntegration202JSONResponseBodyKind = "CatalogIntegration"
)

func (e CreateTableflowV1CatalogIntegration202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateTableflowV1CatalogIntegration202JSONResponseBodyKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	GetTableflowV1CatalogIntegration200JSONResponseBodyApiVersionTableflowv1 GetTableflowV1CatalogIntegration200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e GetTableflowV1CatalogIntegration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetTableflowV1CatalogIntegration200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	GetTableflowV1CatalogIntegration200JSONResponseBodyKindCatalogIntegration GetTableflowV1CatalogIntegration200JSONResponseBodyKind = "CatalogIntegration"
)

func (e GetTableflowV1CatalogIntegration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetTableflowV1CatalogIntegration200JSONResponseBodyKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1CatalogIntegration200JSONResponseBodyApiVersionTableflowv1 UpdateTableflowV1CatalogIntegration200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e UpdateTableflowV1CatalogIntegration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateTableflowV1CatalogIntegration200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1CatalogIntegration200JSONResponseBodyKindCatalogIntegration UpdateTableflowV1CatalogIntegration200JSONResponseBodyKind = "CatalogIntegration"
)

func (e UpdateTableflowV1CatalogIntegration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateTableflowV1CatalogIntegration200JSONResponseBodyKindCatalogIntegration:
		return true
	default:
		return false
	}
}

const (
	ListTableflowV1TableflowTopics200JSONResponseBodyApiVersionTableflowv1 ListTableflowV1TableflowTopics200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e ListTableflowV1TableflowTopics200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListTableflowV1TableflowTopics200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	ListTableflowV1TableflowTopics200JSONResponseBodyKindTableflowTopicList ListTableflowV1TableflowTopics200JSONResponseBodyKind = "TableflowTopicList"
)

func (e ListTableflowV1TableflowTopics200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListTableflowV1TableflowTopics200JSONResponseBodyKindTableflowTopicList:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1TableflowTopicJSONBodyApiVersionTableflowv1 CreateTableflowV1TableflowTopicJSONBodyApiVersion = "tableflow/v1"
)

func (e CreateTableflowV1TableflowTopicJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateTableflowV1TableflowTopicJSONBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1TableflowTopicJSONBodyKindTableflowTopic CreateTableflowV1TableflowTopicJSONBodyKind = "TableflowTopic"
)

func (e CreateTableflowV1TableflowTopicJSONBodyKind) Valid() bool {
	switch e {
	case CreateTableflowV1TableflowTopicJSONBodyKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1TableflowTopic202JSONResponseBodyApiVersionTableflowv1 CreateTableflowV1TableflowTopic202JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e CreateTableflowV1TableflowTopic202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateTableflowV1TableflowTopic202JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	CreateTableflowV1TableflowTopic202JSONResponseBodyKindTableflowTopic CreateTableflowV1TableflowTopic202JSONResponseBodyKind = "TableflowTopic"
)

func (e CreateTableflowV1TableflowTopic202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateTableflowV1TableflowTopic202JSONResponseBodyKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	GetTableflowV1TableflowTopic200JSONResponseBodyApiVersionTableflowv1 GetTableflowV1TableflowTopic200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e GetTableflowV1TableflowTopic200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetTableflowV1TableflowTopic200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	GetTableflowV1TableflowTopic200JSONResponseBodyKindTableflowTopic GetTableflowV1TableflowTopic200JSONResponseBodyKind = "TableflowTopic"
)

func (e GetTableflowV1TableflowTopic200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetTableflowV1TableflowTopic200JSONResponseBodyKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1TableflowTopicJSONBodyApiVersionTableflowv1 UpdateTableflowV1TableflowTopicJSONBodyApiVersion = "tableflow/v1"
)

func (e UpdateTableflowV1TableflowTopicJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateTableflowV1TableflowTopicJSONBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1TableflowTopicJSONBodyKindTableflowTopic UpdateTableflowV1TableflowTopicJSONBodyKind = "TableflowTopic"
)

func (e UpdateTableflowV1TableflowTopicJSONBodyKind) Valid() bool {
	switch e {
	case UpdateTableflowV1TableflowTopicJSONBodyKindTableflowTopic:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1TableflowTopic200JSONResponseBodyApiVersionTableflowv1 UpdateTableflowV1TableflowTopic200JSONResponseBodyApiVersion = "tableflow/v1"
)

func (e UpdateTableflowV1TableflowTopic200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateTableflowV1TableflowTopic200JSONResponseBodyApiVersionTableflowv1:
		return true
	default:
		return false
	}
}

const (
	UpdateTableflowV1TableflowTopic200JSONResponseBodyKindTableflowTopic UpdateTableflowV1TableflowTopic200JSONResponseBodyKind = "TableflowTopic"
)

func (e UpdateTableflowV1TableflowTopic200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateTableflowV1TableflowTopic200JSONResponseBodyKindTableflowTopic:
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
type EnvScopedObjectReference struct {
	// Environment Environment of the referred resource, if env-scoped
	Environment *string `json:"environment,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type Failure struct {
	// Errors List of errors which caused this operation to fail
	Errors []Error `json:"errors"`
}
type GlobalObjectReference struct {
	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type MultipleSearchFilter = []string
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
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type SearchFilter = string
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
type TableflowV1AzureAdlsSpec struct {
	// ContainerName Container name
	ContainerName string `json:"container_name"`

	// Kind The storage type.
	Kind TableflowV1AzureAdlsSpecKind `json:"kind"`

	// ProviderIntegrationId The provider integration id
	ProviderIntegrationId string `json:"provider_integration_id"`

	// StorageAccountName Storage Account Name
	StorageAccountName string `json:"storage_account_name"`

	// StorageRegion Storage account region
	StorageRegion *string `json:"storage_region,omitempty"`

	// TablePath The current storage path where the data and metadata is stored for this table
	TablePath *string `json:"table_path,omitempty"`
}
type TableflowV1AzureAdlsSpecKind string
type TableflowV1ByobAwsSpec struct {
	// BucketName Bucket name
	BucketName string `json:"bucket_name"`

	// BucketRegion Bucket region
	BucketRegion *string `json:"bucket_region,omitempty"`

	// Kind The storage type
	Kind TableflowV1ByobAwsSpecKind `json:"kind"`

	// ProviderIntegrationId The provider integration id
	ProviderIntegrationId string `json:"provider_integration_id"`

	// TablePath The current storage path where the data and metadata is stored for this table
	TablePath *string `json:"table_path,omitempty"`
}
type TableflowV1ByobAwsSpecKind string
type TableflowV1CatalogIntegration struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *TableflowV1CatalogIntegrationApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *TableflowV1CatalogIntegrationKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Catalog Integration
	Spec *TableflowV1CatalogIntegrationSpec `json:"spec,omitempty"`

	// Status The status of the Catalog Integration
	Status *TableflowV1CatalogIntegrationStatus `json:"status,omitempty"`
}
type TableflowV1CatalogIntegrationApiVersion string
type TableflowV1CatalogIntegrationKind string
type TableflowV1CatalogIntegrationAwsGlueSpec struct {
	// CustomDatabase The custom database name to use in AWS Glue.
	CustomDatabase *string `json:"custom_database,omitempty"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationAwsGlueSpecKind `json:"kind"`

	// ProviderIntegrationId The provider integration id.
	ProviderIntegrationId string `json:"provider_integration_id"`
}
type TableflowV1CatalogIntegrationAwsGlueSpecKind string
type TableflowV1CatalogIntegrationAwsGlueUpdateSpec struct {
	// CustomDatabase The custom database name to use in AWS Glue.
	CustomDatabase *string `json:"custom_database,omitempty"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationAwsGlueUpdateSpecKind `json:"kind"`
}
type TableflowV1CatalogIntegrationAwsGlueUpdateSpecKind string
type TableflowV1CatalogIntegrationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion TableflowV1CatalogIntegrationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *TableflowV1CatalogIntegrationListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *TableflowV1CatalogIntegrationListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
		Spec map[string]interface{} `json:"spec"`

		// Status The status of the Catalog Integration
		Status *TableflowV1CatalogIntegrationStatus `json:"status,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     TableflowV1CatalogIntegrationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type TableflowV1CatalogIntegrationListApiVersion string
type TableflowV1CatalogIntegrationListDataApiVersion string
type TableflowV1CatalogIntegrationListDataKind string
type TableflowV1CatalogIntegrationListKind string
type TableflowV1CatalogIntegrationSnowflakeSpec struct {
	// AllowedScope Allowed scope of the Snowflake Open Catalog.
	AllowedScope string `json:"allowed_scope"`

	// ClientId The client ID of the catalog integration.
	ClientId string `json:"client_id"`

	// ClientSecret The client secret of the catalog integration.
	ClientSecret string `json:"client_secret"`

	// CustomNamespace The custom namespace to use in Snowflake Open Catalog.
	CustomNamespace *string `json:"custom_namespace,omitempty"`

	// Endpoint The catalog integration connection endpoint for Snowflake Open Catalog.
	Endpoint string `json:"endpoint"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationSnowflakeSpecKind `json:"kind"`

	// Warehouse Warehouse name of the Snowflake Open Catalog.
	Warehouse string `json:"warehouse"`
}
type TableflowV1CatalogIntegrationSnowflakeSpecKind string
type TableflowV1CatalogIntegrationSnowflakeUpdateSpec struct {
	// AllowedScope Allowed scope of the Snowflake Open Catalog.
	AllowedScope *string `json:"allowed_scope,omitempty"`

	// ClientId The client ID of the catalog integration.
	ClientId *string `json:"client_id,omitempty"`

	// ClientSecret The client secret of the catalog integration.
	ClientSecret *string `json:"client_secret,omitempty"`

	// CustomNamespace The custom namespace to use in Snowflake Open Catalog.
	CustomNamespace *string `json:"custom_namespace,omitempty"`

	// Endpoint The catalog integration connection endpoint for Snowflake Open Catalog.
	Endpoint *string `json:"endpoint,omitempty"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationSnowflakeUpdateSpecKind `json:"kind"`

	// Warehouse Warehouse name of the Snowflake Open Catalog.
	Warehouse *string `json:"warehouse,omitempty"`
}
type TableflowV1CatalogIntegrationSnowflakeUpdateSpecKind string
type TableflowV1CatalogIntegrationSpec struct {
	// Config The integration config
	Config *TableflowV1CatalogIntegrationSpec_Config `json:"config,omitempty"`

	// DisplayName The name of the catalog integration
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which the target Kafka cluster belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// KafkaCluster The kafka cluster of the topic for which Tableflow is enabled
	KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

	// Suspended Indicates whether the Catalog Integration should be suspended. The API allows setting it only to `false` i.e., to resume the Catalog Integration. Pausing the Catalog Integration on-demand is not currently supported.
	Suspended *bool `json:"suspended,omitempty"`
}
type TableflowV1CatalogIntegrationSpec_Config struct {
	union json.RawMessage
}
type TableflowV1CatalogIntegrationStatus struct {
	// ErrorMessage Displayable error message if catalog integration is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// LastSyncAt The date and time at which the catalog was last synced. It is represented in RFC3339 format and is in UTC.
	LastSyncAt *string `json:"last_sync_at,omitempty"`

	// Phase The lifecycle phase of the catalog integration:
	//
	//   PENDING: sync to catalog integration is pending;
	//
	//   CONNECTED: catalog integration is connected and syncing;
	//
	//   FAILED: catalog integration failed.
	Phase *string `json:"phase,omitempty"`
}
type TableflowV1CatalogIntegrationUnitySpec struct {
	// CatalogName The name of the catalog within Unity Catalog.
	CatalogName string `json:"catalog_name"`

	// ClientId The OAuth client ID used to authenticate with the Unity Catalog.
	ClientId string `json:"client_id"`

	// ClientSecret The OAuth client secret used for authentication with the Unity Catalog.
	ClientSecret string `json:"client_secret"`

	// CustomSchema The custom schema name to use in Unity Catalog.
	CustomSchema *string `json:"custom_schema,omitempty"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationUnitySpecKind `json:"kind"`

	// WorkspaceEndpoint The Databricks workspace URL associated with the Unity Catalog.
	WorkspaceEndpoint string `json:"workspace_endpoint"`
}
type TableflowV1CatalogIntegrationUnitySpecKind string
type TableflowV1CatalogIntegrationUnityUpdateSpec struct {
	// CatalogName The name of the catalog within Unity Catalog.
	CatalogName *string `json:"catalog_name,omitempty"`

	// ClientId The OAuth client ID used to authenticate with the Unity Catalog.
	ClientId *string `json:"client_id,omitempty"`

	// ClientSecret The OAuth client secret used for authentication with the Unity Catalog.
	ClientSecret *string `json:"client_secret,omitempty"`

	// CustomSchema The custom schema name to use in Unity Catalog.
	CustomSchema *string `json:"custom_schema,omitempty"`

	// Kind The type of the catalog integration.
	Kind TableflowV1CatalogIntegrationUnityUpdateSpecKind `json:"kind"`

	// WorkspaceEndpoint The Databricks workspace URL associated with the Unity Catalog.
	WorkspaceEndpoint *string `json:"workspace_endpoint,omitempty"`
}
type TableflowV1CatalogIntegrationUnityUpdateSpecKind string
type TableflowV1CatalogIntegrationUpdateRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *TableflowV1CatalogIntegrationUpdateRequestApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *TableflowV1CatalogIntegrationUpdateRequestKind `json:"kind,omitempty"`
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
	Spec *TableflowV1CatalogIntegrationUpdateSpec `json:"spec,omitempty"`
}
type TableflowV1CatalogIntegrationUpdateRequestApiVersion string
type TableflowV1CatalogIntegrationUpdateRequestKind string
type TableflowV1CatalogIntegrationUpdateSpec struct {
	// Config The integration config
	Config *TableflowV1CatalogIntegrationUpdateSpec_Config `json:"config,omitempty"`

	// DisplayName The name of the catalog integration
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which the target Kafka cluster belongs.
	Environment GlobalObjectReference `json:"environment"`

	// KafkaCluster The kafka cluster of the topic for which Tableflow is enabled
	KafkaCluster EnvScopedObjectReference `json:"kafka_cluster"`

	// Suspended Indicates whether the Catalog Integration should be suspended.
	Suspended *bool `json:"suspended,omitempty"`
}
type TableflowV1CatalogIntegrationUpdateSpec_Config struct {
	union json.RawMessage
}
type TableflowV1CatalogSyncStatus struct {
	// CatalogIntegrationId The ID of the catalog integration
	CatalogIntegrationId *string `json:"catalog_integration_id,omitempty"`

	// CatalogType The type of the external catalog
	CatalogType *string `json:"catalog_type,omitempty"`

	// ErrorMessage Error message if the sync failed. This field is only present when `sync_status` is `FAILED`.
	ErrorMessage *string `json:"error_message,omitempty"`

	// SyncStatus The current synchronization status:
	//
	//   PENDING: sync is pending;
	//
	//   SYNCED: successfully synced;
	//
	//   FAILED: sync failed;
	//
	//   DISCONNECTED: catalog integration is disconnected.
	SyncStatus *string `json:"sync_status,omitempty"`
}
type TableflowV1ErrorHandlingLog struct {
	// Mode The error handling mode for the Tableflow enabled topic.
	//
	// In this mode, the bad records are logged to a dead-letter queue (DLQ) topic and the
	//
	// materialization continues with the next record.
	Mode TableflowV1ErrorHandlingLogMode `json:"mode"`

	// Target The topic to which the bad records will be logged in case of `LOG` error handling mode.
	//
	// Creates the topic if it doesn't already exist; otherwise, the operation is idempotent and no action is taken.
	//
	// Default topic is `error_log`.
	Target *string `json:"target,omitempty"`
}
type TableflowV1ErrorHandlingLogMode string
type TableflowV1ErrorHandlingSkip struct {
	// Mode The error handling mode for the Tableflow enabled topic.
	//
	// In this mode, the bad records are skipped and the materialization continues with the next record.
	Mode TableflowV1ErrorHandlingSkipMode `json:"mode"`
}
type TableflowV1ErrorHandlingSkipMode string
type TableflowV1ErrorHandlingSuspend struct {
	// Mode The error handling mode for the Tableflow enabled topic.
	//
	// In this mode, the materialization of the topic is suspended in case of record failures.
	Mode TableflowV1ErrorHandlingSuspendMode `json:"mode"`
}
type TableflowV1ErrorHandlingSuspendMode string
type TableflowV1ManagedStorageSpec struct {
	// Kind The storage type.
	Kind TableflowV1ManagedStorageSpecKind `json:"kind"`

	// TablePath The current storage path where the data and metadata is stored for this table
	TablePath *string `json:"table_path,omitempty"`
}
type TableflowV1ManagedStorageSpecKind string
type TableflowV1Region struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *TableflowV1RegionApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider that hosts the region.
	Cloud *string `json:"cloud,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *TableflowV1RegionKind `json:"kind,omitempty"`
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

	// Region The cloud service provider region.
	Region *string `json:"region,omitempty"`
}
type TableflowV1RegionApiVersion string
type TableflowV1RegionKind string
type TableflowV1RegionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion TableflowV1RegionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *TableflowV1RegionListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider that hosts the region.
		Cloud string `json:"cloud"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *TableflowV1RegionListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Region The cloud service provider region.
		Region string `json:"region"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     TableflowV1RegionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type TableflowV1RegionListApiVersion string
type TableflowV1RegionListDataApiVersion string
type TableflowV1RegionListDataKind string
type TableflowV1RegionListKind string
type TableflowV1TableFlowTopicConfigsSpec struct {
	// DataRetentionMs The maximum age, in milliseconds, of data to retain in the table for the Tableflow-enabled topic.
	//
	// The minimum allowed non-zero value is "2592000000" milliseconds (equivalent to 30 days).
	//
	// Set to "0" to disable data retention (keep all data indefinitely).
	//
	// Note - The attribute is in a [Limited Availability lifecycle stage](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	DataRetentionMs *string `json:"data_retention_ms,omitempty"`

	// EnableCompaction This flag determines whether to enable compaction for the Tableflow enabled topic.
	EnableCompaction *bool `json:"enable_compaction,omitempty"`

	// EnablePartitioning This flag determines whether to enable partitioning for the Tableflow enabled topic.
	EnablePartitioning *bool `json:"enable_partitioning,omitempty"`

	// ErrorHandling The error mode to handle record failures in the Tableflow enabled topic during materialization.
	//
	// for `SKIP`, we skip the bad records and move to the next record,
	//
	// for `SUSPEND`, we suspend the materialization of the topic,
	//
	// and for `LOG`, we log the bad records to the DLQ and continue processing the rest of the records.
	ErrorHandling *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling `json:"error_handling,omitempty"`

	// RecordFailureStrategy The strategy to handle record failures in the Tableflow enabled topic during materialization.
	//
	// For `SKIP`, we skip the bad records and move to the next record,
	//
	// and for `SUSPEND`, we suspend the materialization of the topic.
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	RecordFailureStrategy *string `json:"record_failure_strategy,omitempty"`

	// RetentionMs The maximum age, in milliseconds, of snapshots (for Iceberg) or versions (for Delta)
	// to retain in the table for the Tableflow-enabled topic (snapshot/version expiration).
	//
	// The default value is "604800000" milliseconds (equivalent to 7 days).
	//
	// The minimum allowed value is "86400000" milliseconds (equivalent to 24 hours).
	RetentionMs *string `json:"retention_ms,omitempty"`
}
type TableflowV1TableFlowTopicConfigsSpec_ErrorHandling struct {
	union json.RawMessage
}
type TableflowV1TableflowTopic struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *TableflowV1TableflowTopicApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *TableflowV1TableflowTopicKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Tableflow Topic
	Spec *TableflowV1TableflowTopicSpec `json:"spec,omitempty"`

	// Status The status of the Tableflow Topic
	Status *TableflowV1TableflowTopicStatus `json:"status,omitempty"`
}
type TableflowV1TableflowTopicApiVersion string
type TableflowV1TableflowTopicKind string
type TableflowV1TableflowTopicList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion TableflowV1TableflowTopicListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *TableflowV1TableflowTopicListDataApiVersion `json:"api_version,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *TableflowV1TableflowTopicListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
		Spec map[string]interface{} `json:"spec"`

		// Status The status of the Tableflow Topic
		Status TableflowV1TableflowTopicStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     TableflowV1TableflowTopicListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type TableflowV1TableflowTopicListApiVersion string
type TableflowV1TableflowTopicListDataApiVersion string
type TableflowV1TableflowTopicListDataKind string
type TableflowV1TableflowTopicListKind string
type TableflowV1TableflowTopicSpec struct {
	// Config The config for the Tableflow enabled topic
	Config *TableflowV1TableFlowTopicConfigsSpec `json:"config,omitempty"`

	// DisplayName The name of the Kafka topic for which Tableflow is enabled.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which the target Kafka cluster belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// KafkaCluster The kafka cluster of the topic for which Tableflow is enabled
	KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

	// Storage The storage config
	Storage *TableflowV1TableflowTopicSpec_Storage `json:"storage,omitempty"`

	// Suspended Indicates whether the Tableflow should be suspended. The API allows setting it only to `false` i.e., to resume the Tableflow. Pausing the Tableflow on-demand is not currently supported.
	Suspended *bool `json:"suspended,omitempty"`

	// TableFormats The supported table formats for the Tableflow-enabled topic.
	TableFormats *[]string `json:"table_formats,omitempty"`
}
type TableflowV1TableflowTopicSpec_Storage struct {
	union json.RawMessage
}
type TableflowV1TableflowTopicStatus struct {
	// CatalogSyncStatuses List of associated catalogs and their synchronization statuses for this Tableflow topic.
	CatalogSyncStatuses *[]TableflowV1CatalogSyncStatus `json:"catalog_sync_statuses,omitempty"`

	// ErrorMessage Displayable error message if Tableflow topic is in an error state
	ErrorMessage *string `json:"error_message,omitempty"`

	// FailingTableFormats List of failing table formats for the Tableflow-enabled topic, including error details.
	FailingTableFormats *[]struct {
		// ErrorMessage The error message for the failing table format.
		ErrorMessage string `json:"error_message"`

		// Format The name of the table format (e.g., DELTA, ICEBERG).
		Format string `json:"format"`
	} `json:"failing_table_formats,omitempty"`

	// Phase The lifecycle phase of the Tableflow:
	//
	//   PENDING: Tableflow setup is pending;
	//
	//   RUNNING: Tableflow is currently running;
	//
	//   FAILED: Tableflow failed
	Phase *string `json:"phase,omitempty"`

	// WriteMode The write mode for the Tableflow-enabled topic, determining how data is written to the table.
	WriteMode string `json:"write_mode"`
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
func (t TableflowV1CatalogIntegrationSpec_Config) AsTableflowV1CatalogIntegrationAwsGlueSpec() (TableflowV1CatalogIntegrationAwsGlueSpec, error) {
	var body TableflowV1CatalogIntegrationAwsGlueSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) FromTableflowV1CatalogIntegrationAwsGlueSpec(v TableflowV1CatalogIntegrationAwsGlueSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsGlue"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) MergeTableflowV1CatalogIntegrationAwsGlueSpec(v TableflowV1CatalogIntegrationAwsGlueSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsGlue"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationSpec_Config) AsTableflowV1CatalogIntegrationSnowflakeSpec() (TableflowV1CatalogIntegrationSnowflakeSpec, error) {
	var body TableflowV1CatalogIntegrationSnowflakeSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) FromTableflowV1CatalogIntegrationSnowflakeSpec(v TableflowV1CatalogIntegrationSnowflakeSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Snowflake"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) MergeTableflowV1CatalogIntegrationSnowflakeSpec(v TableflowV1CatalogIntegrationSnowflakeSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Snowflake"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationSpec_Config) AsTableflowV1CatalogIntegrationUnitySpec() (TableflowV1CatalogIntegrationUnitySpec, error) {
	var body TableflowV1CatalogIntegrationUnitySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) FromTableflowV1CatalogIntegrationUnitySpec(v TableflowV1CatalogIntegrationUnitySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Unity"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) MergeTableflowV1CatalogIntegrationUnitySpec(v TableflowV1CatalogIntegrationUnitySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Unity"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t TableflowV1CatalogIntegrationSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsGlue":
		return t.AsTableflowV1CatalogIntegrationAwsGlueSpec()
	case "Snowflake":
		return t.AsTableflowV1CatalogIntegrationSnowflakeSpec()
	case "Unity":
		return t.AsTableflowV1CatalogIntegrationUnitySpec()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t TableflowV1CatalogIntegrationSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *TableflowV1CatalogIntegrationSpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) AsTableflowV1CatalogIntegrationAwsGlueUpdateSpec() (TableflowV1CatalogIntegrationAwsGlueUpdateSpec, error) {
	var body TableflowV1CatalogIntegrationAwsGlueUpdateSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) FromTableflowV1CatalogIntegrationAwsGlueUpdateSpec(v TableflowV1CatalogIntegrationAwsGlueUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsGlue"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) MergeTableflowV1CatalogIntegrationAwsGlueUpdateSpec(v TableflowV1CatalogIntegrationAwsGlueUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsGlue"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) AsTableflowV1CatalogIntegrationSnowflakeUpdateSpec() (TableflowV1CatalogIntegrationSnowflakeUpdateSpec, error) {
	var body TableflowV1CatalogIntegrationSnowflakeUpdateSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) FromTableflowV1CatalogIntegrationSnowflakeUpdateSpec(v TableflowV1CatalogIntegrationSnowflakeUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Snowflake"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) MergeTableflowV1CatalogIntegrationSnowflakeUpdateSpec(v TableflowV1CatalogIntegrationSnowflakeUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Snowflake"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) AsTableflowV1CatalogIntegrationUnityUpdateSpec() (TableflowV1CatalogIntegrationUnityUpdateSpec, error) {
	var body TableflowV1CatalogIntegrationUnityUpdateSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) FromTableflowV1CatalogIntegrationUnityUpdateSpec(v TableflowV1CatalogIntegrationUnityUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Unity"}`))
	t.union = b
	return err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) MergeTableflowV1CatalogIntegrationUnityUpdateSpec(v TableflowV1CatalogIntegrationUnityUpdateSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Unity"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsGlue":
		return t.AsTableflowV1CatalogIntegrationAwsGlueUpdateSpec()
	case "Snowflake":
		return t.AsTableflowV1CatalogIntegrationSnowflakeUpdateSpec()
	case "Unity":
		return t.AsTableflowV1CatalogIntegrationUnityUpdateSpec()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t TableflowV1CatalogIntegrationUpdateSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *TableflowV1CatalogIntegrationUpdateSpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) AsTableflowV1ErrorHandlingSuspend() (TableflowV1ErrorHandlingSuspend, error) {
	var body TableflowV1ErrorHandlingSuspend
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) FromTableflowV1ErrorHandlingSuspend(v TableflowV1ErrorHandlingSuspend) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"SUSPEND"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) MergeTableflowV1ErrorHandlingSuspend(v TableflowV1ErrorHandlingSuspend) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"SUSPEND"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) AsTableflowV1ErrorHandlingSkip() (TableflowV1ErrorHandlingSkip, error) {
	var body TableflowV1ErrorHandlingSkip
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) FromTableflowV1ErrorHandlingSkip(v TableflowV1ErrorHandlingSkip) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"SKIP"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) MergeTableflowV1ErrorHandlingSkip(v TableflowV1ErrorHandlingSkip) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"SKIP"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) AsTableflowV1ErrorHandlingLog() (TableflowV1ErrorHandlingLog, error) {
	var body TableflowV1ErrorHandlingLog
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) FromTableflowV1ErrorHandlingLog(v TableflowV1ErrorHandlingLog) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"LOG"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) MergeTableflowV1ErrorHandlingLog(v TableflowV1ErrorHandlingLog) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"mode":"LOG"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"mode"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "LOG":
		return t.AsTableflowV1ErrorHandlingLog()
	case "SKIP":
		return t.AsTableflowV1ErrorHandlingSkip()
	case "SUSPEND":
		return t.AsTableflowV1ErrorHandlingSuspend()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *TableflowV1TableFlowTopicConfigsSpec_ErrorHandling) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t TableflowV1TableflowTopicSpec_Storage) AsTableflowV1ByobAwsSpec() (TableflowV1ByobAwsSpec, error) {
	var body TableflowV1ByobAwsSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableflowTopicSpec_Storage) FromTableflowV1ByobAwsSpec(v TableflowV1ByobAwsSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"ByobAws"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableflowTopicSpec_Storage) MergeTableflowV1ByobAwsSpec(v TableflowV1ByobAwsSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"ByobAws"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableflowTopicSpec_Storage) AsTableflowV1ManagedStorageSpec() (TableflowV1ManagedStorageSpec, error) {
	var body TableflowV1ManagedStorageSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableflowTopicSpec_Storage) FromTableflowV1ManagedStorageSpec(v TableflowV1ManagedStorageSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Managed"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableflowTopicSpec_Storage) MergeTableflowV1ManagedStorageSpec(v TableflowV1ManagedStorageSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Managed"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableflowTopicSpec_Storage) AsTableflowV1AzureAdlsSpec() (TableflowV1AzureAdlsSpec, error) {
	var body TableflowV1AzureAdlsSpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *TableflowV1TableflowTopicSpec_Storage) FromTableflowV1AzureAdlsSpec(v TableflowV1AzureAdlsSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureDataLakeStorageGen2"}`))
	t.union = b
	return err
}
func (t *TableflowV1TableflowTopicSpec_Storage) MergeTableflowV1AzureAdlsSpec(v TableflowV1AzureAdlsSpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureDataLakeStorageGen2"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t TableflowV1TableflowTopicSpec_Storage) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t TableflowV1TableflowTopicSpec_Storage) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureDataLakeStorageGen2":
		return t.AsTableflowV1AzureAdlsSpec()
	case "ByobAws":
		return t.AsTableflowV1ByobAwsSpec()
	case "Managed":
		return t.AsTableflowV1ManagedStorageSpec()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t TableflowV1TableflowTopicSpec_Storage) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *TableflowV1TableflowTopicSpec_Storage) UnmarshalJSON(b []byte) error {
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

	// ListTableflowV1CatalogIntegrations List of Catalog Integrations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all catalog integrations.
	//
	// Corresponds with GET /tableflow/v1/catalog-integrations (the `ListTableflowV1CatalogIntegrations` operationId).
	ListTableflowV1CatalogIntegrations(ctx context.Context, params *ListTableflowV1CatalogIntegrationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTableflowV1CatalogIntegrationWithBody Create a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a catalog integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /tableflow/v1/catalog-integrations (the `CreateTableflowV1CatalogIntegration` operationId).
	CreateTableflowV1CatalogIntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTableflowV1CatalogIntegration Create a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a catalog integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /tableflow/v1/catalog-integrations (the `CreateTableflowV1CatalogIntegration` operationId).
	CreateTableflowV1CatalogIntegration(ctx context.Context, body CreateTableflowV1CatalogIntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteTableflowV1CatalogIntegration Delete a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a catalog integration.
	//
	// Corresponds with DELETE /tableflow/v1/catalog-integrations/{id} (the `DeleteTableflowV1CatalogIntegration` operationId).
	DeleteTableflowV1CatalogIntegration(ctx context.Context, id string, params *DeleteTableflowV1CatalogIntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetTableflowV1CatalogIntegration Read a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a catalog integration.
	//
	// Corresponds with GET /tableflow/v1/catalog-integrations/{id} (the `GetTableflowV1CatalogIntegration` operationId).
	GetTableflowV1CatalogIntegration(ctx context.Context, id string, params *GetTableflowV1CatalogIntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTableflowV1CatalogIntegrationWithBody Update a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a catalog integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /tableflow/v1/catalog-integrations/{id} (the `UpdateTableflowV1CatalogIntegration` operationId).
	UpdateTableflowV1CatalogIntegrationWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTableflowV1CatalogIntegration Update a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a catalog integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /tableflow/v1/catalog-integrations/{id} (the `UpdateTableflowV1CatalogIntegration` operationId).
	UpdateTableflowV1CatalogIntegration(ctx context.Context, id string, body UpdateTableflowV1CatalogIntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListTableflowV1Regions List of Regions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all regions.
	//
	// Corresponds with GET /tableflow/v1/regions (the `ListTableflowV1Regions` operationId).
	ListTableflowV1Regions(ctx context.Context, params *ListTableflowV1RegionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListTableflowV1TableflowTopics List of Tableflow Topics
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all tableflow topics.
	//
	// Corresponds with GET /tableflow/v1/tableflow-topics (the `ListTableflowV1TableflowTopics` operationId).
	ListTableflowV1TableflowTopics(ctx context.Context, params *ListTableflowV1TableflowTopicsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTableflowV1TableflowTopicWithBody Create a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a tableflow topic.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /tableflow/v1/tableflow-topics (the `CreateTableflowV1TableflowTopic` operationId).
	CreateTableflowV1TableflowTopicWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTableflowV1TableflowTopic Create a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a tableflow topic.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /tableflow/v1/tableflow-topics (the `CreateTableflowV1TableflowTopic` operationId).
	CreateTableflowV1TableflowTopic(ctx context.Context, body CreateTableflowV1TableflowTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteTableflowV1TableflowTopic Delete a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a tableflow topic.
	//
	// Corresponds with DELETE /tableflow/v1/tableflow-topics/{display_name} (the `DeleteTableflowV1TableflowTopic` operationId).
	DeleteTableflowV1TableflowTopic(ctx context.Context, displayName string, params *DeleteTableflowV1TableflowTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetTableflowV1TableflowTopic Read a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a tableflow topic.
	//
	// Corresponds with GET /tableflow/v1/tableflow-topics/{display_name} (the `GetTableflowV1TableflowTopic` operationId).
	GetTableflowV1TableflowTopic(ctx context.Context, displayName string, params *GetTableflowV1TableflowTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTableflowV1TableflowTopicWithBody Update a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a tableflow topic.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /tableflow/v1/tableflow-topics/{display_name} (the `UpdateTableflowV1TableflowTopic` operationId).
	UpdateTableflowV1TableflowTopicWithBody(ctx context.Context, displayName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTableflowV1TableflowTopic Update a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a tableflow topic.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /tableflow/v1/tableflow-topics/{display_name} (the `UpdateTableflowV1TableflowTopic` operationId).
	UpdateTableflowV1TableflowTopic(ctx context.Context, displayName string, body UpdateTableflowV1TableflowTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListTableflowV1CatalogIntegrationsWithResponse List of Catalog Integrations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all catalog integrations.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /tableflow/v1/catalog-integrations (the `ListTableflowV1CatalogIntegrations` operationId).
	ListTableflowV1CatalogIntegrationsWithResponse(ctx context.Context, params *ListTableflowV1CatalogIntegrationsParams, reqEditors ...RequestEditorFn) (*ListTableflowV1CatalogIntegrationsResponse, error)

	// CreateTableflowV1CatalogIntegrationWithBodyWithResponse Create a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a catalog integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /tableflow/v1/catalog-integrations (the `CreateTableflowV1CatalogIntegration` operationId).
	CreateTableflowV1CatalogIntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateTableflowV1CatalogIntegrationResponse, error)

	// CreateTableflowV1CatalogIntegrationWithResponse Create a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a catalog integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /tableflow/v1/catalog-integrations (the `CreateTableflowV1CatalogIntegration` operationId).
	CreateTableflowV1CatalogIntegrationWithResponse(ctx context.Context, body CreateTableflowV1CatalogIntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateTableflowV1CatalogIntegrationResponse, error)

	// DeleteTableflowV1CatalogIntegrationWithResponse Delete a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a catalog integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /tableflow/v1/catalog-integrations/{id} (the `DeleteTableflowV1CatalogIntegration` operationId).
	DeleteTableflowV1CatalogIntegrationWithResponse(ctx context.Context, id string, params *DeleteTableflowV1CatalogIntegrationParams, reqEditors ...RequestEditorFn) (*DeleteTableflowV1CatalogIntegrationResponse, error)

	// GetTableflowV1CatalogIntegrationWithResponse Read a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a catalog integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /tableflow/v1/catalog-integrations/{id} (the `GetTableflowV1CatalogIntegration` operationId).
	GetTableflowV1CatalogIntegrationWithResponse(ctx context.Context, id string, params *GetTableflowV1CatalogIntegrationParams, reqEditors ...RequestEditorFn) (*GetTableflowV1CatalogIntegrationResponse, error)

	// UpdateTableflowV1CatalogIntegrationWithBodyWithResponse Update a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a catalog integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /tableflow/v1/catalog-integrations/{id} (the `UpdateTableflowV1CatalogIntegration` operationId).
	UpdateTableflowV1CatalogIntegrationWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateTableflowV1CatalogIntegrationResponse, error)

	// UpdateTableflowV1CatalogIntegrationWithResponse Update a Catalog Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a catalog integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /tableflow/v1/catalog-integrations/{id} (the `UpdateTableflowV1CatalogIntegration` operationId).
	UpdateTableflowV1CatalogIntegrationWithResponse(ctx context.Context, id string, body UpdateTableflowV1CatalogIntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTableflowV1CatalogIntegrationResponse, error)

	// ListTableflowV1RegionsWithResponse List of Regions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all regions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /tableflow/v1/regions (the `ListTableflowV1Regions` operationId).
	ListTableflowV1RegionsWithResponse(ctx context.Context, params *ListTableflowV1RegionsParams, reqEditors ...RequestEditorFn) (*ListTableflowV1RegionsResponse, error)

	// ListTableflowV1TableflowTopicsWithResponse List of Tableflow Topics
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all tableflow topics.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /tableflow/v1/tableflow-topics (the `ListTableflowV1TableflowTopics` operationId).
	ListTableflowV1TableflowTopicsWithResponse(ctx context.Context, params *ListTableflowV1TableflowTopicsParams, reqEditors ...RequestEditorFn) (*ListTableflowV1TableflowTopicsResponse, error)

	// CreateTableflowV1TableflowTopicWithBodyWithResponse Create a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a tableflow topic.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /tableflow/v1/tableflow-topics (the `CreateTableflowV1TableflowTopic` operationId).
	CreateTableflowV1TableflowTopicWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateTableflowV1TableflowTopicResponse, error)

	// CreateTableflowV1TableflowTopicWithResponse Create a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a tableflow topic.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /tableflow/v1/tableflow-topics (the `CreateTableflowV1TableflowTopic` operationId).
	CreateTableflowV1TableflowTopicWithResponse(ctx context.Context, body CreateTableflowV1TableflowTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateTableflowV1TableflowTopicResponse, error)

	// DeleteTableflowV1TableflowTopicWithResponse Delete a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a tableflow topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /tableflow/v1/tableflow-topics/{display_name} (the `DeleteTableflowV1TableflowTopic` operationId).
	DeleteTableflowV1TableflowTopicWithResponse(ctx context.Context, displayName string, params *DeleteTableflowV1TableflowTopicParams, reqEditors ...RequestEditorFn) (*DeleteTableflowV1TableflowTopicResponse, error)

	// GetTableflowV1TableflowTopicWithResponse Read a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a tableflow topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /tableflow/v1/tableflow-topics/{display_name} (the `GetTableflowV1TableflowTopic` operationId).
	GetTableflowV1TableflowTopicWithResponse(ctx context.Context, displayName string, params *GetTableflowV1TableflowTopicParams, reqEditors ...RequestEditorFn) (*GetTableflowV1TableflowTopicResponse, error)

	// UpdateTableflowV1TableflowTopicWithBodyWithResponse Update a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a tableflow topic.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /tableflow/v1/tableflow-topics/{display_name} (the `UpdateTableflowV1TableflowTopic` operationId).
	UpdateTableflowV1TableflowTopicWithBodyWithResponse(ctx context.Context, displayName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateTableflowV1TableflowTopicResponse, error)

	// UpdateTableflowV1TableflowTopicWithResponse Update a Tableflow Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a tableflow topic.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /tableflow/v1/tableflow-topics/{display_name} (the `UpdateTableflowV1TableflowTopic` operationId).
	UpdateTableflowV1TableflowTopicWithResponse(ctx context.Context, displayName string, body UpdateTableflowV1TableflowTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTableflowV1TableflowTopicResponse, error)
}

func (r ListTableflowV1CatalogIntegrationsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListTableflowV1CatalogIntegrations200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment  interface{} `json:"environment,omitempty"`
			KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListTableflowV1CatalogIntegrations200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
} {
	return r.JSON200
}
func (r ListTableflowV1CatalogIntegrationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListTableflowV1CatalogIntegrationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListTableflowV1CatalogIntegrationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListTableflowV1CatalogIntegrationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListTableflowV1CatalogIntegrationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListTableflowV1CatalogIntegrationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListTableflowV1CatalogIntegrationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListTableflowV1CatalogIntegrationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateTableflowV1CatalogIntegration202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateTableflowV1CatalogIntegration202JSONResponseBodyKind `json:"kind,omitempty"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Catalog Integration
	Status *TableflowV1CatalogIntegrationStatus `json:"status,omitempty"`
} {
	return r.JSON202
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateTableflowV1CatalogIntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r CreateTableflowV1CatalogIntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateTableflowV1CatalogIntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateTableflowV1CatalogIntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteTableflowV1CatalogIntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteTableflowV1CatalogIntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteTableflowV1CatalogIntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteTableflowV1CatalogIntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetTableflowV1CatalogIntegration200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetTableflowV1CatalogIntegration200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Catalog Integration
	Status *TableflowV1CatalogIntegrationStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetTableflowV1CatalogIntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetTableflowV1CatalogIntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r GetTableflowV1CatalogIntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetTableflowV1CatalogIntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetTableflowV1CatalogIntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateTableflowV1CatalogIntegration200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateTableflowV1CatalogIntegration200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Catalog Integration
	Status *TableflowV1CatalogIntegrationStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateTableflowV1CatalogIntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateTableflowV1CatalogIntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateTableflowV1CatalogIntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateTableflowV1CatalogIntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListTableflowV1RegionsResponse) GetJSON200() *TableflowV1RegionList {
	return r.JSON200
}
func (r ListTableflowV1RegionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListTableflowV1RegionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListTableflowV1RegionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListTableflowV1RegionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListTableflowV1RegionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListTableflowV1RegionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListTableflowV1RegionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListTableflowV1RegionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListTableflowV1TableflowTopicsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListTableflowV1TableflowTopics200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment  interface{} `json:"environment,omitempty"`
			KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListTableflowV1TableflowTopics200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
} {
	return r.JSON200
}
func (r ListTableflowV1TableflowTopicsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListTableflowV1TableflowTopicsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListTableflowV1TableflowTopicsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListTableflowV1TableflowTopicsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListTableflowV1TableflowTopicsResponse) GetBody() []byte {
	return r.Body
}
func (r ListTableflowV1TableflowTopicsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListTableflowV1TableflowTopicsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListTableflowV1TableflowTopicsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateTableflowV1TableflowTopic202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateTableflowV1TableflowTopic202JSONResponseBodyKind `json:"kind,omitempty"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Tableflow Topic
	Status TableflowV1TableflowTopicStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateTableflowV1TableflowTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateTableflowV1TableflowTopicResponse) GetBody() []byte {
	return r.Body
}
func (r CreateTableflowV1TableflowTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateTableflowV1TableflowTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateTableflowV1TableflowTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteTableflowV1TableflowTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteTableflowV1TableflowTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteTableflowV1TableflowTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteTableflowV1TableflowTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteTableflowV1TableflowTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteTableflowV1TableflowTopicResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteTableflowV1TableflowTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteTableflowV1TableflowTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteTableflowV1TableflowTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetTableflowV1TableflowTopic200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetTableflowV1TableflowTopic200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Tableflow Topic
	Status TableflowV1TableflowTopicStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetTableflowV1TableflowTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetTableflowV1TableflowTopicResponse) GetBody() []byte {
	return r.Body
}
func (r GetTableflowV1TableflowTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetTableflowV1TableflowTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetTableflowV1TableflowTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateTableflowV1TableflowTopic200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateTableflowV1TableflowTopic200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Tableflow Topic
	Status TableflowV1TableflowTopicStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateTableflowV1TableflowTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateTableflowV1TableflowTopicResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateTableflowV1TableflowTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateTableflowV1TableflowTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateTableflowV1TableflowTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
