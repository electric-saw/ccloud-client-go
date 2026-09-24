package ccl

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
	CclV1CustomCodeLoggingApiVersionCclv1 CclV1CustomCodeLoggingApiVersion = "ccl/v1"
)

func (e CclV1CustomCodeLoggingApiVersion) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	CclV1CustomCodeLoggingKindCustomCodeLogging CclV1CustomCodeLoggingKind = "CustomCodeLogging"
)

func (e CclV1CustomCodeLoggingKind) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	CclV1CustomCodeLoggingListApiVersionCclv1 CclV1CustomCodeLoggingListApiVersion = "ccl/v1"
)

func (e CclV1CustomCodeLoggingListApiVersion) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingListApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	CclV1CustomCodeLoggingListDataApiVersionCclv1 CclV1CustomCodeLoggingListDataApiVersion = "ccl/v1"
)

func (e CclV1CustomCodeLoggingListDataApiVersion) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingListDataApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	CclV1CustomCodeLoggingListDataKindCustomCodeLogging CclV1CustomCodeLoggingListDataKind = "CustomCodeLogging"
)

func (e CclV1CustomCodeLoggingListDataKind) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingListDataKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	CclV1CustomCodeLoggingListKindCustomCodeLoggingList CclV1CustomCodeLoggingListKind = "CustomCodeLoggingList"
)

func (e CclV1CustomCodeLoggingListKind) Valid() bool {
	switch e {
	case CclV1CustomCodeLoggingListKindCustomCodeLoggingList:
		return true
	default:
		return false
	}
}

const (
	Kafka CclV1KafkaDestinationSettingsKind = "Kafka"
)

func (e CclV1KafkaDestinationSettingsKind) Valid() bool {
	switch e {
	case Kafka:
		return true
	default:
		return false
	}
}

const (
	ListCclV1CustomCodeLoggings200JSONResponseBodyApiVersionCclv1 ListCclV1CustomCodeLoggings200JSONResponseBodyApiVersion = "ccl/v1"
)

func (e ListCclV1CustomCodeLoggings200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCclV1CustomCodeLoggings200JSONResponseBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	ListCclV1CustomCodeLoggings200JSONResponseBodyKindCustomCodeLoggingList ListCclV1CustomCodeLoggings200JSONResponseBodyKind = "CustomCodeLoggingList"
)

func (e ListCclV1CustomCodeLoggings200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCclV1CustomCodeLoggings200JSONResponseBodyKindCustomCodeLoggingList:
		return true
	default:
		return false
	}
}

const (
	CreateCclV1CustomCodeLoggingJSONBodyApiVersionCclv1 CreateCclV1CustomCodeLoggingJSONBodyApiVersion = "ccl/v1"
)

func (e CreateCclV1CustomCodeLoggingJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCclV1CustomCodeLoggingJSONBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	CreateCclV1CustomCodeLoggingJSONBodyKindCustomCodeLogging CreateCclV1CustomCodeLoggingJSONBodyKind = "CustomCodeLogging"
)

func (e CreateCclV1CustomCodeLoggingJSONBodyKind) Valid() bool {
	switch e {
	case CreateCclV1CustomCodeLoggingJSONBodyKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	CreateCclV1CustomCodeLogging201JSONResponseBodyApiVersionCclv1 CreateCclV1CustomCodeLogging201JSONResponseBodyApiVersion = "ccl/v1"
)

func (e CreateCclV1CustomCodeLogging201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCclV1CustomCodeLogging201JSONResponseBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	CreateCclV1CustomCodeLogging201JSONResponseBodyKindCustomCodeLogging CreateCclV1CustomCodeLogging201JSONResponseBodyKind = "CustomCodeLogging"
)

func (e CreateCclV1CustomCodeLogging201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCclV1CustomCodeLogging201JSONResponseBodyKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	GetCclV1CustomCodeLogging200JSONResponseBodyApiVersionCclv1 GetCclV1CustomCodeLogging200JSONResponseBodyApiVersion = "ccl/v1"
)

func (e GetCclV1CustomCodeLogging200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCclV1CustomCodeLogging200JSONResponseBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	GetCclV1CustomCodeLogging200JSONResponseBodyKindCustomCodeLogging GetCclV1CustomCodeLogging200JSONResponseBodyKind = "CustomCodeLogging"
)

func (e GetCclV1CustomCodeLogging200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCclV1CustomCodeLogging200JSONResponseBodyKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	UpdateCclV1CustomCodeLoggingJSONBodyApiVersionCclv1 UpdateCclV1CustomCodeLoggingJSONBodyApiVersion = "ccl/v1"
)

func (e UpdateCclV1CustomCodeLoggingJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCclV1CustomCodeLoggingJSONBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCclV1CustomCodeLoggingJSONBodyKindCustomCodeLogging UpdateCclV1CustomCodeLoggingJSONBodyKind = "CustomCodeLogging"
)

func (e UpdateCclV1CustomCodeLoggingJSONBodyKind) Valid() bool {
	switch e {
	case UpdateCclV1CustomCodeLoggingJSONBodyKindCustomCodeLogging:
		return true
	default:
		return false
	}
}

const (
	UpdateCclV1CustomCodeLogging200JSONResponseBodyApiVersionCclv1 UpdateCclV1CustomCodeLogging200JSONResponseBodyApiVersion = "ccl/v1"
)

func (e UpdateCclV1CustomCodeLogging200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCclV1CustomCodeLogging200JSONResponseBodyApiVersionCclv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCclV1CustomCodeLogging200JSONResponseBodyKindCustomCodeLogging UpdateCclV1CustomCodeLogging200JSONResponseBodyKind = "CustomCodeLogging"
)

func (e UpdateCclV1CustomCodeLogging200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateCclV1CustomCodeLogging200JSONResponseBodyKindCustomCodeLogging:
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
type CclV1CustomCodeLogging struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CclV1CustomCodeLoggingApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Code Logging is sent.
	Cloud *string `json:"cloud,omitempty"`

	// DestinationSettings Destination Settings of the Custom Code Logging.
	DestinationSettings *CclV1CustomCodeLogging_DestinationSettings `json:"destination_settings,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CclV1CustomCodeLoggingKind `json:"kind,omitempty"`
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

	// Region The Cloud provider region the Custom Code Logging is sent.
	Region *string `json:"region,omitempty"`
}
type CclV1CustomCodeLoggingApiVersion string
type CclV1CustomCodeLogging_DestinationSettings struct {
	union json.RawMessage
}
type CclV1CustomCodeLoggingKind string
type CclV1CustomCodeLoggingList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CclV1CustomCodeLoggingListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CclV1CustomCodeLoggingListDataApiVersion `json:"api_version,omitempty"`

		// Cloud Cloud provider where the Custom Code Logging is sent.
		Cloud string `json:"cloud"`

		// DestinationSettings Destination Settings of the Custom Code Logging.
		DestinationSettings CclV1CustomCodeLoggingList_Data_DestinationSettings `json:"destination_settings"`

		// Environment The environment to which this belongs.
		Environment EnvScopedObjectReference `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CclV1CustomCodeLoggingListDataKind `json:"kind,omitempty"`
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

		// Region The Cloud provider region the Custom Code Logging is sent.
		Region string `json:"region"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CclV1CustomCodeLoggingListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CclV1CustomCodeLoggingListApiVersion string
type CclV1CustomCodeLoggingListDataApiVersion string
type CclV1CustomCodeLoggingList_Data_DestinationSettings struct {
	union json.RawMessage
}
type CclV1CustomCodeLoggingListDataKind string
type CclV1CustomCodeLoggingListKind string
type CclV1KafkaDestinationSettings struct {
	// ClusterId The kafka cluster id where Custom Code Logging is sent.
	ClusterId string `json:"cluster_id"`

	// Kind The destination where Custom Code Logging is sent.
	Kind CclV1KafkaDestinationSettingsKind `json:"kind"`

	// LogLevel Minimum log level for Custom Code Logging.
	LogLevel *string `json:"log_level,omitempty"`

	// Topic The kafka topic where Custom Code Logging is sent.
	Topic string `json:"topic"`
}
type CclV1KafkaDestinationSettingsKind string
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
func (t CclV1CustomCodeLogging_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CclV1CustomCodeLogging_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *CclV1CustomCodeLogging_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CclV1CustomCodeLogging_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CclV1CustomCodeLogging_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CclV1CustomCodeLogging_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CclV1CustomCodeLogging_DestinationSettings) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CclV1CustomCodeLoggingList_Data_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CclV1CustomCodeLoggingList_Data_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *CclV1CustomCodeLoggingList_Data_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CclV1CustomCodeLoggingList_Data_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CclV1CustomCodeLoggingList_Data_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CclV1CustomCodeLoggingList_Data_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CclV1CustomCodeLoggingList_Data_DestinationSettings) UnmarshalJSON(b []byte) error {
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
func (t CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateCclV1CustomCodeLoggingJSONBody_DestinationSettings) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdateCclV1CustomCodeLoggingJSONBody_DestinationSettings) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) AsCclV1KafkaDestinationSettings() (CclV1KafkaDestinationSettings, error) {
	var body CclV1KafkaDestinationSettings
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) FromCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	t.union = b
	return err
}
func (t *UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) MergeCclV1KafkaDestinationSettings(v CclV1KafkaDestinationSettings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Kafka"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Kafka":
		return t.AsCclV1KafkaDestinationSettings()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings) UnmarshalJSON(b []byte) error {
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

	// ListCclV1CustomCodeLoggings List of Custom Code Loggings
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all custom code loggings.
	//
	// Corresponds with GET /ccl/v1/custom-code-loggings (the `ListCclV1CustomCodeLoggings` operationId).
	listCclV1CustomCodeLoggings(ctx context.Context, params *ListCclV1CustomCodeLoggingsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCclV1CustomCodeLoggingWithBody Create a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create a custom code logging.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /ccl/v1/custom-code-loggings (the `CreateCclV1CustomCodeLogging` operationId).
	createCclV1CustomCodeLoggingWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCclV1CustomCodeLogging Create a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create a custom code logging.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /ccl/v1/custom-code-loggings (the `CreateCclV1CustomCodeLogging` operationId).
	createCclV1CustomCodeLogging(ctx context.Context, body CreateCclV1CustomCodeLoggingJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCclV1CustomCodeLogging Delete a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to delete a custom code logging.
	//
	// Corresponds with DELETE /ccl/v1/custom-code-loggings/{id} (the `DeleteCclV1CustomCodeLogging` operationId).
	deleteCclV1CustomCodeLogging(ctx context.Context, id string, params *DeleteCclV1CustomCodeLoggingParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCclV1CustomCodeLogging Read a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read a custom code logging.
	//
	// Corresponds with GET /ccl/v1/custom-code-loggings/{id} (the `GetCclV1CustomCodeLogging` operationId).
	getCclV1CustomCodeLogging(ctx context.Context, id string, params *GetCclV1CustomCodeLoggingParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCclV1CustomCodeLoggingWithBody Update a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a custom code logging.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /ccl/v1/custom-code-loggings/{id} (the `UpdateCclV1CustomCodeLogging` operationId).
	updateCclV1CustomCodeLoggingWithBody(ctx context.Context, id string, params *UpdateCclV1CustomCodeLoggingParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCclV1CustomCodeLogging Update a Custom Code Logging
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Custom Code Logging API EA](https://img.shields.io/badge/-Request%20Access%20To%20Custom%20Code%20Logging%20API%20EA-%23bc8540)](mailto:ccloud-api-access+ccl-v1-early-access@confluent.io?subject=Request%20to%20join%20ccl/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20ccl/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a custom code logging.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /ccl/v1/custom-code-loggings/{id} (the `UpdateCclV1CustomCodeLogging` operationId).
	updateCclV1CustomCodeLogging(ctx context.Context, id string, params *UpdateCclV1CustomCodeLoggingParams, body UpdateCclV1CustomCodeLoggingJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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
func (r ListCclV1CustomCodeLoggingsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCclV1CustomCodeLoggings200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment interface{} `json:"environment,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCclV1CustomCodeLoggings200JSONResponseBodyKind `json:"kind"`
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
func (r ListCclV1CustomCodeLoggingsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCclV1CustomCodeLoggingsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCclV1CustomCodeLoggingsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCclV1CustomCodeLoggingsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCclV1CustomCodeLoggingsResponse) GetBody() []byte {
	return r.Body
}
func (r ListCclV1CustomCodeLoggingsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCclV1CustomCodeLoggingsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCclV1CustomCodeLoggingsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCclV1CustomCodeLogging201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Code Logging is sent.
	Cloud string `json:"cloud"`

	// DestinationSettings Destination Settings of the Custom Code Logging.
	DestinationSettings CreateCclV1CustomCodeLogging201JSONResponseBody_DestinationSettings `json:"destination_settings"`
	Environment         interface{}                                                         `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateCclV1CustomCodeLogging201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// Region The Cloud provider region the Custom Code Logging is sent.
	Region string `json:"region"`
} {
	return r.JSON201
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCclV1CustomCodeLoggingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCclV1CustomCodeLoggingResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCclV1CustomCodeLoggingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCclV1CustomCodeLoggingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCclV1CustomCodeLoggingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCclV1CustomCodeLoggingResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCclV1CustomCodeLoggingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCclV1CustomCodeLoggingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCclV1CustomCodeLoggingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCclV1CustomCodeLogging200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Custom Code Logging is sent.
	Cloud string `json:"cloud"`

	// DestinationSettings Destination Settings of the Custom Code Logging.
	DestinationSettings GetCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings `json:"destination_settings"`
	Environment         interface{}                                                      `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCclV1CustomCodeLogging200JSONResponseBodyKind `json:"kind"`
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

	// Region The Cloud provider region the Custom Code Logging is sent.
	Region string `json:"region"`
} {
	return r.JSON200
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCclV1CustomCodeLoggingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCclV1CustomCodeLoggingResponse) GetBody() []byte {
	return r.Body
}
func (r GetCclV1CustomCodeLoggingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCclV1CustomCodeLoggingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCclV1CustomCodeLoggingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateCclV1CustomCodeLogging200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Custom Code Logging is sent.
	Cloud string `json:"cloud"`

	// DestinationSettings Destination Settings of the Custom Code Logging.
	DestinationSettings UpdateCclV1CustomCodeLogging200JSONResponseBody_DestinationSettings `json:"destination_settings"`
	Environment         interface{}                                                         `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateCclV1CustomCodeLogging200JSONResponseBodyKind `json:"kind"`
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

	// Region The Cloud provider region the Custom Code Logging is sent.
	Region string `json:"region"`
} {
	return r.JSON200
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateCclV1CustomCodeLoggingResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateCclV1CustomCodeLoggingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateCclV1CustomCodeLoggingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateCclV1CustomCodeLoggingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
