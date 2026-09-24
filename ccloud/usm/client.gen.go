package usm

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
	UsmV1ConnectClusterApiVersionUsmv1 UsmV1ConnectClusterApiVersion = "usm/v1"
)

func (e UsmV1ConnectClusterApiVersion) Valid() bool {
	switch e {
	case UsmV1ConnectClusterApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1ConnectClusterKindConnectCluster UsmV1ConnectClusterKind = "ConnectCluster"
)

func (e UsmV1ConnectClusterKind) Valid() bool {
	switch e {
	case UsmV1ConnectClusterKindConnectCluster:
		return true
	default:
		return false
	}
}

const (
	UsmV1ConnectClusterListApiVersionUsmv1 UsmV1ConnectClusterListApiVersion = "usm/v1"
)

func (e UsmV1ConnectClusterListApiVersion) Valid() bool {
	switch e {
	case UsmV1ConnectClusterListApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1ConnectClusterListDataApiVersionUsmv1 UsmV1ConnectClusterListDataApiVersion = "usm/v1"
)

func (e UsmV1ConnectClusterListDataApiVersion) Valid() bool {
	switch e {
	case UsmV1ConnectClusterListDataApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1ConnectClusterListDataKindConnectCluster UsmV1ConnectClusterListDataKind = "ConnectCluster"
)

func (e UsmV1ConnectClusterListDataKind) Valid() bool {
	switch e {
	case UsmV1ConnectClusterListDataKindConnectCluster:
		return true
	default:
		return false
	}
}

const (
	UsmV1ConnectClusterListKindConnectClusterList UsmV1ConnectClusterListKind = "ConnectClusterList"
)

func (e UsmV1ConnectClusterListKind) Valid() bool {
	switch e {
	case UsmV1ConnectClusterListKindConnectClusterList:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterApiVersionUsmv1 UsmV1KafkaClusterApiVersion = "usm/v1"
)

func (e UsmV1KafkaClusterApiVersion) Valid() bool {
	switch e {
	case UsmV1KafkaClusterApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterKindKafkaCluster UsmV1KafkaClusterKind = "KafkaCluster"
)

func (e UsmV1KafkaClusterKind) Valid() bool {
	switch e {
	case UsmV1KafkaClusterKindKafkaCluster:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterListApiVersionUsmv1 UsmV1KafkaClusterListApiVersion = "usm/v1"
)

func (e UsmV1KafkaClusterListApiVersion) Valid() bool {
	switch e {
	case UsmV1KafkaClusterListApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterListDataApiVersionUsmv1 UsmV1KafkaClusterListDataApiVersion = "usm/v1"
)

func (e UsmV1KafkaClusterListDataApiVersion) Valid() bool {
	switch e {
	case UsmV1KafkaClusterListDataApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterListDataKindKafkaCluster UsmV1KafkaClusterListDataKind = "KafkaCluster"
)

func (e UsmV1KafkaClusterListDataKind) Valid() bool {
	switch e {
	case UsmV1KafkaClusterListDataKindKafkaCluster:
		return true
	default:
		return false
	}
}

const (
	UsmV1KafkaClusterListKindKafkaClusterList UsmV1KafkaClusterListKind = "KafkaClusterList"
)

func (e UsmV1KafkaClusterListKind) Valid() bool {
	switch e {
	case UsmV1KafkaClusterListKindKafkaClusterList:
		return true
	default:
		return false
	}
}

const (
	ListUsmV1ConnectClusters200JSONResponseBodyApiVersionUsmv1 ListUsmV1ConnectClusters200JSONResponseBodyApiVersion = "usm/v1"
)

func (e ListUsmV1ConnectClusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListUsmV1ConnectClusters200JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	ListUsmV1ConnectClusters200JSONResponseBodyKindConnectClusterList ListUsmV1ConnectClusters200JSONResponseBodyKind = "ConnectClusterList"
)

func (e ListUsmV1ConnectClusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListUsmV1ConnectClusters200JSONResponseBodyKindConnectClusterList:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1ConnectClusterJSONBodyApiVersionUsmv1 CreateUsmV1ConnectClusterJSONBodyApiVersion = "usm/v1"
)

func (e CreateUsmV1ConnectClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateUsmV1ConnectClusterJSONBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1ConnectClusterJSONBodyKindConnectCluster CreateUsmV1ConnectClusterJSONBodyKind = "ConnectCluster"
)

func (e CreateUsmV1ConnectClusterJSONBodyKind) Valid() bool {
	switch e {
	case CreateUsmV1ConnectClusterJSONBodyKindConnectCluster:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1ConnectCluster201JSONResponseBodyApiVersionUsmv1 CreateUsmV1ConnectCluster201JSONResponseBodyApiVersion = "usm/v1"
)

func (e CreateUsmV1ConnectCluster201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateUsmV1ConnectCluster201JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1ConnectCluster201JSONResponseBodyKindConnectCluster CreateUsmV1ConnectCluster201JSONResponseBodyKind = "ConnectCluster"
)

func (e CreateUsmV1ConnectCluster201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateUsmV1ConnectCluster201JSONResponseBodyKindConnectCluster:
		return true
	default:
		return false
	}
}

const (
	GetUsmV1ConnectCluster200JSONResponseBodyApiVersionUsmv1 GetUsmV1ConnectCluster200JSONResponseBodyApiVersion = "usm/v1"
)

func (e GetUsmV1ConnectCluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetUsmV1ConnectCluster200JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	GetUsmV1ConnectCluster200JSONResponseBodyKindConnectCluster GetUsmV1ConnectCluster200JSONResponseBodyKind = "ConnectCluster"
)

func (e GetUsmV1ConnectCluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetUsmV1ConnectCluster200JSONResponseBodyKindConnectCluster:
		return true
	default:
		return false
	}
}

const (
	ListUsmV1KafkaClusters200JSONResponseBodyApiVersionUsmv1 ListUsmV1KafkaClusters200JSONResponseBodyApiVersion = "usm/v1"
)

func (e ListUsmV1KafkaClusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListUsmV1KafkaClusters200JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	ListUsmV1KafkaClusters200JSONResponseBodyKindKafkaClusterList ListUsmV1KafkaClusters200JSONResponseBodyKind = "KafkaClusterList"
)

func (e ListUsmV1KafkaClusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListUsmV1KafkaClusters200JSONResponseBodyKindKafkaClusterList:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1KafkaClusterJSONBodyApiVersionUsmv1 CreateUsmV1KafkaClusterJSONBodyApiVersion = "usm/v1"
)

func (e CreateUsmV1KafkaClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateUsmV1KafkaClusterJSONBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1KafkaClusterJSONBodyKindKafkaCluster CreateUsmV1KafkaClusterJSONBodyKind = "KafkaCluster"
)

func (e CreateUsmV1KafkaClusterJSONBodyKind) Valid() bool {
	switch e {
	case CreateUsmV1KafkaClusterJSONBodyKindKafkaCluster:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1KafkaCluster201JSONResponseBodyApiVersionUsmv1 CreateUsmV1KafkaCluster201JSONResponseBodyApiVersion = "usm/v1"
)

func (e CreateUsmV1KafkaCluster201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateUsmV1KafkaCluster201JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	CreateUsmV1KafkaCluster201JSONResponseBodyKindKafkaCluster CreateUsmV1KafkaCluster201JSONResponseBodyKind = "KafkaCluster"
)

func (e CreateUsmV1KafkaCluster201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateUsmV1KafkaCluster201JSONResponseBodyKindKafkaCluster:
		return true
	default:
		return false
	}
}

const (
	GetUsmV1KafkaCluster200JSONResponseBodyApiVersionUsmv1 GetUsmV1KafkaCluster200JSONResponseBodyApiVersion = "usm/v1"
)

func (e GetUsmV1KafkaCluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetUsmV1KafkaCluster200JSONResponseBodyApiVersionUsmv1:
		return true
	default:
		return false
	}
}

const (
	GetUsmV1KafkaCluster200JSONResponseBodyKindKafkaCluster GetUsmV1KafkaCluster200JSONResponseBodyKind = "KafkaCluster"
)

func (e GetUsmV1KafkaCluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetUsmV1KafkaCluster200JSONResponseBodyKindKafkaCluster:
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
type UsmV1ConnectCluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *UsmV1ConnectClusterApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider where the metadata for the Connect Cluster should be stored.
	// This field is optional. If provided, 'region' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the cloud provider of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Cloud *string `json:"cloud,omitempty"`

	// ConfluentPlatformConnectClusterId The unique identifier of the Connect cluster within the Confluent Platform environment.
	ConfluentPlatformConnectClusterId *string `json:"confluent_platform_connect_cluster_id,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// KafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	KafkaClusterId *string `json:"kafka_cluster_id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *UsmV1ConnectClusterKind `json:"kind,omitempty"`

	// Region The home region of the Confluent Platform Connect cluster where the metadata should be stored.
	// This field is optional. If provided, 'cloud' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the home region of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Region *string `json:"region,omitempty"`

	// UsmKafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	UsmKafkaClusterId *string `json:"usm_kafka_cluster_id,omitempty"`
}
type UsmV1ConnectClusterApiVersion string
type UsmV1ConnectClusterKind string
type UsmV1ConnectClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UsmV1ConnectClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *UsmV1ConnectClusterListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider where the metadata for the Connect Cluster should be stored.
		// This field is optional. If provided, 'region' must also be provided.
		// If neither 'cloud' nor 'region' are provided, the cloud provider of the associated
		// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
		Cloud *string `json:"cloud,omitempty"`

		// ConfluentPlatformConnectClusterId The unique identifier of the Connect cluster within the Confluent Platform environment.
		ConfluentPlatformConnectClusterId string `json:"confluent_platform_connect_cluster_id"`

		// Environment The environment to which this belongs.
		Environment EnvScopedObjectReference `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// KafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
		KafkaClusterId string `json:"kafka_cluster_id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *UsmV1ConnectClusterListDataKind `json:"kind,omitempty"`

		// Region The home region of the Confluent Platform Connect cluster where the metadata should be stored.
		// This field is optional. If provided, 'cloud' must also be provided.
		// If neither 'cloud' nor 'region' are provided, the home region of the associated
		// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
		Region *string `json:"region,omitempty"`

		// UsmKafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
		UsmKafkaClusterId *string `json:"usm_kafka_cluster_id,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UsmV1ConnectClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type UsmV1ConnectClusterListApiVersion string
type UsmV1ConnectClusterListDataApiVersion string
type UsmV1ConnectClusterListDataKind string
type UsmV1ConnectClusterListKind string
type UsmV1KafkaCluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *UsmV1KafkaClusterApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider where the metadata for the Kafka Cluster should be stored.
	Cloud *string `json:"cloud,omitempty"`

	// ConfluentPlatformKafkaClusterId The unique identifier of the Kafka cluster within the Confluent Platform environment.
	ConfluentPlatformKafkaClusterId *string `json:"confluent_platform_kafka_cluster_id,omitempty"`

	// DisplayName A human-readable name for the Confluent Platform Kafka cluster.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *UsmV1KafkaClusterKind `json:"kind,omitempty"`

	// Region The home region of the Confluent Platform Kafka cluster where the metadata should be stored.
	Region *string `json:"region,omitempty"`
}
type UsmV1KafkaClusterApiVersion string
type UsmV1KafkaClusterKind string
type UsmV1KafkaClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UsmV1KafkaClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *UsmV1KafkaClusterListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider where the metadata for the Kafka Cluster should be stored.
		Cloud string `json:"cloud"`

		// ConfluentPlatformKafkaClusterId The unique identifier of the Kafka cluster within the Confluent Platform environment.
		ConfluentPlatformKafkaClusterId string `json:"confluent_platform_kafka_cluster_id"`

		// DisplayName A human-readable name for the Confluent Platform Kafka cluster.
		DisplayName string `json:"display_name"`

		// Environment The environment to which this belongs.
		Environment EnvScopedObjectReference `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *UsmV1KafkaClusterListDataKind `json:"kind,omitempty"`

		// Region The home region of the Confluent Platform Kafka cluster where the metadata should be stored.
		Region string `json:"region"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UsmV1KafkaClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type UsmV1KafkaClusterListApiVersion string
type UsmV1KafkaClusterListDataApiVersion string
type UsmV1KafkaClusterListDataKind string
type UsmV1KafkaClusterListKind string
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

type RequestEditorFn func(ctx context.Context, req *http.Request) error
type HttpRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}
type oasClient struct {
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
type ClientOption func(*oasClient) error

func NewClient(server string, opts ...ClientOption) (*oasClient, error) {
	// create a client with sane default values
	client := oasClient{
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
	return func(c *oasClient) error {
		c.Client = doer
		return nil
	}
}
func WithRequestEditorFn(fn RequestEditorFn) ClientOption {
	return func(c *oasClient) error {
		c.RequestEditors = append(c.RequestEditors, fn)
		return nil
	}
}

type clientInterface interface {

	// ListUsmV1ConnectClusters List of Connect Clusters
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all connect clusters.
	//
	// Corresponds with GET /usm/v1/connect-clusters (the `ListUsmV1ConnectClusters` operationId).
	listUsmV1ConnectClusters(ctx context.Context, params *ListUsmV1ConnectClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateUsmV1ConnectClusterWithBody Create a Connect Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /usm/v1/connect-clusters (the `CreateUsmV1ConnectCluster` operationId).
	createUsmV1ConnectClusterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateUsmV1ConnectCluster Create a Connect Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /usm/v1/connect-clusters (the `CreateUsmV1ConnectCluster` operationId).
	createUsmV1ConnectCluster(ctx context.Context, body CreateUsmV1ConnectClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteUsmV1ConnectCluster Delete a Connect Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a connect cluster.
	//
	// Corresponds with DELETE /usm/v1/connect-clusters/{id} (the `DeleteUsmV1ConnectCluster` operationId).
	deleteUsmV1ConnectCluster(ctx context.Context, id string, params *DeleteUsmV1ConnectClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetUsmV1ConnectCluster Read a Connect Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a connect cluster.
	//
	// Corresponds with GET /usm/v1/connect-clusters/{id} (the `GetUsmV1ConnectCluster` operationId).
	getUsmV1ConnectCluster(ctx context.Context, id string, params *GetUsmV1ConnectClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListUsmV1KafkaClusters List of Kafka Clusters
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all kafka clusters.
	//
	// Corresponds with GET /usm/v1/kafka-clusters (the `ListUsmV1KafkaClusters` operationId).
	listUsmV1KafkaClusters(ctx context.Context, params *ListUsmV1KafkaClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateUsmV1KafkaClusterWithBody Create a Kafka Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a kafka cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /usm/v1/kafka-clusters (the `CreateUsmV1KafkaCluster` operationId).
	createUsmV1KafkaClusterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateUsmV1KafkaCluster Create a Kafka Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a kafka cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /usm/v1/kafka-clusters (the `CreateUsmV1KafkaCluster` operationId).
	createUsmV1KafkaCluster(ctx context.Context, body CreateUsmV1KafkaClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteUsmV1KafkaCluster Delete a Kafka Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a kafka cluster.
	//
	// Corresponds with DELETE /usm/v1/kafka-clusters/{id} (the `DeleteUsmV1KafkaCluster` operationId).
	deleteUsmV1KafkaCluster(ctx context.Context, id string, params *DeleteUsmV1KafkaClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetUsmV1KafkaCluster Read a Kafka Cluster
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a kafka cluster.
	//
	// Corresponds with GET /usm/v1/kafka-clusters/{id} (the `GetUsmV1KafkaCluster` operationId).
	getUsmV1KafkaCluster(ctx context.Context, id string, params *GetUsmV1KafkaClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *oasClient) applyEditors(ctx context.Context, req *http.Request, additionalEditors []RequestEditorFn) error {
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
	clientInterface
}

func NewClientWithResponses(server string, opts ...ClientOption) (*ClientWithResponses, error) {
	client, err := NewClient(server, opts...)
	if err != nil {
		return nil, err
	}
	return &ClientWithResponses{client}, nil
}
func WithBaseURL(baseURL string) ClientOption {
	return func(c *oasClient) error {
		newBaseURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}
		c.Server = newBaseURL.String()
		return nil
	}
}
func (r ListUsmV1ConnectClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListUsmV1ConnectClusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment interface{} `json:"environment,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListUsmV1ConnectClusters200JSONResponseBodyKind `json:"kind"`
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
func (r ListUsmV1ConnectClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListUsmV1ConnectClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListUsmV1ConnectClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListUsmV1ConnectClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListUsmV1ConnectClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListUsmV1ConnectClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListUsmV1ConnectClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListUsmV1ConnectClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateUsmV1ConnectCluster201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider where the metadata for the Connect Cluster should be stored.
	// This field is optional. If provided, 'region' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the cloud provider of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Cloud *string `json:"cloud,omitempty"`

	// ConfluentPlatformConnectClusterId The unique identifier of the Connect cluster within the Confluent Platform environment.
	ConfluentPlatformConnectClusterId string      `json:"confluent_platform_connect_cluster_id"`
	Environment                       interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// KafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	KafkaClusterId string `json:"kafka_cluster_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreateUsmV1ConnectCluster201JSONResponseBodyKind `json:"kind,omitempty"`

	// Region The home region of the Confluent Platform Connect cluster where the metadata should be stored.
	// This field is optional. If provided, 'cloud' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the home region of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Region *string `json:"region,omitempty"`

	// UsmKafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	UsmKafkaClusterId *string `json:"usm_kafka_cluster_id,omitempty"`
} {
	return r.JSON201
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateUsmV1ConnectClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateUsmV1ConnectClusterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateUsmV1ConnectClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateUsmV1ConnectClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateUsmV1ConnectClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteUsmV1ConnectClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteUsmV1ConnectClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteUsmV1ConnectClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteUsmV1ConnectClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteUsmV1ConnectClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteUsmV1ConnectClusterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteUsmV1ConnectClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteUsmV1ConnectClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteUsmV1ConnectClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetUsmV1ConnectClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetUsmV1ConnectCluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud The cloud service provider where the metadata for the Connect Cluster should be stored.
	// This field is optional. If provided, 'region' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the cloud provider of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Cloud *string `json:"cloud,omitempty"`

	// ConfluentPlatformConnectClusterId The unique identifier of the Connect cluster within the Confluent Platform environment.
	ConfluentPlatformConnectClusterId string      `json:"confluent_platform_connect_cluster_id"`
	Environment                       interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// KafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	KafkaClusterId string `json:"kafka_cluster_id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetUsmV1ConnectCluster200JSONResponseBodyKind `json:"kind"`

	// Region The home region of the Confluent Platform Connect cluster where the metadata should be stored.
	// This field is optional. If provided, 'cloud' must also be provided.
	// If neither 'cloud' nor 'region' are provided, the home region of the associated
	// metadata Kafka cluster (identified by 'kafka_cluster_id') will be used as a fallback.
	Region *string `json:"region,omitempty"`

	// UsmKafkaClusterId The unique identifier of the metadata Kafka cluster for the Connect Cluster.
	UsmKafkaClusterId *string `json:"usm_kafka_cluster_id,omitempty"`
} {
	return r.JSON200
}
func (r GetUsmV1ConnectClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetUsmV1ConnectClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetUsmV1ConnectClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetUsmV1ConnectClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetUsmV1ConnectClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetUsmV1ConnectClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetUsmV1ConnectClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetUsmV1ConnectClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetUsmV1ConnectClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListUsmV1KafkaClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListUsmV1KafkaClusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment interface{} `json:"environment,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListUsmV1KafkaClusters200JSONResponseBodyKind `json:"kind"`
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
func (r ListUsmV1KafkaClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListUsmV1KafkaClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListUsmV1KafkaClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListUsmV1KafkaClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListUsmV1KafkaClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListUsmV1KafkaClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListUsmV1KafkaClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListUsmV1KafkaClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateUsmV1KafkaCluster201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider where the metadata for the Kafka Cluster should be stored.
	Cloud string `json:"cloud"`

	// ConfluentPlatformKafkaClusterId The unique identifier of the Kafka cluster within the Confluent Platform environment.
	ConfluentPlatformKafkaClusterId string `json:"confluent_platform_kafka_cluster_id"`

	// DisplayName A human-readable name for the Confluent Platform Kafka cluster.
	DisplayName string      `json:"display_name"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreateUsmV1KafkaCluster201JSONResponseBodyKind `json:"kind,omitempty"`

	// Region The home region of the Confluent Platform Kafka cluster where the metadata should be stored.
	Region string `json:"region"`
} {
	return r.JSON201
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateUsmV1KafkaClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateUsmV1KafkaClusterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateUsmV1KafkaClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateUsmV1KafkaClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateUsmV1KafkaClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r DeleteUsmV1KafkaClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteUsmV1KafkaClusterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteUsmV1KafkaClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteUsmV1KafkaClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteUsmV1KafkaClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetUsmV1KafkaClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetUsmV1KafkaCluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud The cloud service provider where the metadata for the Kafka Cluster should be stored.
	Cloud string `json:"cloud"`

	// ConfluentPlatformKafkaClusterId The unique identifier of the Kafka cluster within the Confluent Platform environment.
	ConfluentPlatformKafkaClusterId string `json:"confluent_platform_kafka_cluster_id"`

	// DisplayName A human-readable name for the Confluent Platform Kafka cluster.
	DisplayName string      `json:"display_name"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetUsmV1KafkaCluster200JSONResponseBodyKind `json:"kind"`

	// Region The home region of the Confluent Platform Kafka cluster where the metadata should be stored.
	Region string `json:"region"`
} {
	return r.JSON200
}
func (r GetUsmV1KafkaClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetUsmV1KafkaClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetUsmV1KafkaClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetUsmV1KafkaClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetUsmV1KafkaClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetUsmV1KafkaClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetUsmV1KafkaClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetUsmV1KafkaClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetUsmV1KafkaClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
