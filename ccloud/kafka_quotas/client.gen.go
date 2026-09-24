package kafka_quotas

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
	KafkaQuotasV1ClientQuotaApiVersionKafkaQuotasv1 KafkaQuotasV1ClientQuotaApiVersion = "kafka-quotas/v1"
)

func (e KafkaQuotasV1ClientQuotaApiVersion) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	KafkaQuotasV1ClientQuotaKindClientQuota KafkaQuotasV1ClientQuotaKind = "ClientQuota"
)

func (e KafkaQuotasV1ClientQuotaKind) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaKindClientQuota:
		return true
	default:
		return false
	}
}

const (
	KafkaQuotasV1ClientQuotaListApiVersionKafkaQuotasv1 KafkaQuotasV1ClientQuotaListApiVersion = "kafka-quotas/v1"
)

func (e KafkaQuotasV1ClientQuotaListApiVersion) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaListApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	KafkaQuotasV1ClientQuotaListDataApiVersionKafkaQuotasv1 KafkaQuotasV1ClientQuotaListDataApiVersion = "kafka-quotas/v1"
)

func (e KafkaQuotasV1ClientQuotaListDataApiVersion) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaListDataApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	KafkaQuotasV1ClientQuotaListDataKindClientQuota KafkaQuotasV1ClientQuotaListDataKind = "ClientQuota"
)

func (e KafkaQuotasV1ClientQuotaListDataKind) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaListDataKindClientQuota:
		return true
	default:
		return false
	}
}

const (
	KafkaQuotasV1ClientQuotaListKindClientQuotaList KafkaQuotasV1ClientQuotaListKind = "ClientQuotaList"
)

func (e KafkaQuotasV1ClientQuotaListKind) Valid() bool {
	switch e {
	case KafkaQuotasV1ClientQuotaListKindClientQuotaList:
		return true
	default:
		return false
	}
}

const (
	ListKafkaQuotasV1ClientQuotas200JSONResponseBodyApiVersionKafkaQuotasv1 ListKafkaQuotasV1ClientQuotas200JSONResponseBodyApiVersion = "kafka-quotas/v1"
)

func (e ListKafkaQuotasV1ClientQuotas200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListKafkaQuotasV1ClientQuotas200JSONResponseBodyApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	ListKafkaQuotasV1ClientQuotas200JSONResponseBodyKindClientQuotaList ListKafkaQuotasV1ClientQuotas200JSONResponseBodyKind = "ClientQuotaList"
)

func (e ListKafkaQuotasV1ClientQuotas200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListKafkaQuotasV1ClientQuotas200JSONResponseBodyKindClientQuotaList:
		return true
	default:
		return false
	}
}

const (
	CreateKafkaQuotasV1ClientQuota202JSONResponseBodyApiVersionKafkaQuotasv1 CreateKafkaQuotasV1ClientQuota202JSONResponseBodyApiVersion = "kafka-quotas/v1"
)

func (e CreateKafkaQuotasV1ClientQuota202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateKafkaQuotasV1ClientQuota202JSONResponseBodyApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	CreateKafkaQuotasV1ClientQuota202JSONResponseBodyKindClientQuota CreateKafkaQuotasV1ClientQuota202JSONResponseBodyKind = "ClientQuota"
)

func (e CreateKafkaQuotasV1ClientQuota202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateKafkaQuotasV1ClientQuota202JSONResponseBodyKindClientQuota:
		return true
	default:
		return false
	}
}

const (
	GetKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersionKafkaQuotasv1 GetKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion = "kafka-quotas/v1"
)

func (e GetKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	GetKafkaQuotasV1ClientQuota200JSONResponseBodyKindClientQuota GetKafkaQuotasV1ClientQuota200JSONResponseBodyKind = "ClientQuota"
)

func (e GetKafkaQuotasV1ClientQuota200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetKafkaQuotasV1ClientQuota200JSONResponseBodyKindClientQuota:
		return true
	default:
		return false
	}
}

const (
	UpdateKafkaQuotasV1ClientQuotaJSONBodyApiVersionKafkaQuotasv1 UpdateKafkaQuotasV1ClientQuotaJSONBodyApiVersion = "kafka-quotas/v1"
)

func (e UpdateKafkaQuotasV1ClientQuotaJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateKafkaQuotasV1ClientQuotaJSONBodyApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	UpdateKafkaQuotasV1ClientQuotaJSONBodyKindClientQuota UpdateKafkaQuotasV1ClientQuotaJSONBodyKind = "ClientQuota"
)

func (e UpdateKafkaQuotasV1ClientQuotaJSONBodyKind) Valid() bool {
	switch e {
	case UpdateKafkaQuotasV1ClientQuotaJSONBodyKindClientQuota:
		return true
	default:
		return false
	}
}

const (
	UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersionKafkaQuotasv1 UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion = "kafka-quotas/v1"
)

func (e UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersionKafkaQuotasv1:
		return true
	default:
		return false
	}
}

const (
	UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyKindClientQuota UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyKind = "ClientQuota"
)

func (e UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyKindClientQuota:
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
type KafkaQuotasV1ClientQuota struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *KafkaQuotasV1ClientQuotaApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *KafkaQuotasV1ClientQuotaKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Client Quota
	Spec *KafkaQuotasV1ClientQuotaSpec `json:"spec,omitempty"`
}
type KafkaQuotasV1ClientQuotaApiVersion string
type KafkaQuotasV1ClientQuotaKind string
type KafkaQuotasV1ClientQuotaList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion KafkaQuotasV1ClientQuotaListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *KafkaQuotasV1ClientQuotaListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *KafkaQuotasV1ClientQuotaListDataKind `json:"kind,omitempty"`
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
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     KafkaQuotasV1ClientQuotaListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type KafkaQuotasV1ClientQuotaListApiVersion string
type KafkaQuotasV1ClientQuotaListDataApiVersion string
type KafkaQuotasV1ClientQuotaListDataKind string
type KafkaQuotasV1ClientQuotaListKind string
type KafkaQuotasV1ClientQuotaSpec struct {
	// Cluster The ID of the Dedicated Kafka cluster where the client quota is applied.
	Cluster *EnvScopedObjectReference `json:"cluster,omitempty"`

	// Description A human readable description for the client quota.
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the client quota.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Principals A list of principals to apply a client quota to.
	// Use `"<default>"` to apply a client quota to all service accounts
	// (see [Control application usage with Client Quotas](https://docs.confluent.io/cloud/current/clusters/client-quotas.html#control-application-usage-with-client-quotas) for more details).
	Principals *[]GlobalObjectReference `json:"principals,omitempty"`

	// Throughput Throughput for the client quota.
	Throughput *KafkaQuotasV1Throughput `json:"throughput,omitempty"`
}
type KafkaQuotasV1Throughput struct {
	// EgressByteRate Egress throughput limit for principals specified in bytes per second.
	EgressByteRate string `json:"egress_byte_rate"`

	// IngressByteRate Ingress throughput limit for principals specified in bytes per second.
	IngressByteRate string `json:"ingress_byte_rate"`
}
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

	// ListKafkaQuotasV1ClientQuotas List of Client Quotas
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all client quotas.
	//
	// Corresponds with GET /kafka-quotas/v1/client-quotas (the `ListKafkaQuotasV1ClientQuotas` operationId).
	listKafkaQuotasV1ClientQuotas(ctx context.Context, params *ListKafkaQuotasV1ClientQuotasParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaQuotasV1ClientQuotaWithBody Create a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a client quota.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka-quotas/v1/client-quotas (the `CreateKafkaQuotasV1ClientQuota` operationId).
	createKafkaQuotasV1ClientQuotaWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaQuotasV1ClientQuota Create a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a client quota.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka-quotas/v1/client-quotas (the `CreateKafkaQuotasV1ClientQuota` operationId).
	createKafkaQuotasV1ClientQuota(ctx context.Context, body CreateKafkaQuotasV1ClientQuotaJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaQuotasV1ClientQuota Delete a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a client quota.
	//
	// Corresponds with DELETE /kafka-quotas/v1/client-quotas/{id} (the `DeleteKafkaQuotasV1ClientQuota` operationId).
	deleteKafkaQuotasV1ClientQuota(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaQuotasV1ClientQuota Read a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a client quota.
	//
	// Corresponds with GET /kafka-quotas/v1/client-quotas/{id} (the `GetKafkaQuotasV1ClientQuota` operationId).
	getKafkaQuotasV1ClientQuota(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaQuotasV1ClientQuotaWithBody Update a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a client quota.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /kafka-quotas/v1/client-quotas/{id} (the `UpdateKafkaQuotasV1ClientQuota` operationId).
	updateKafkaQuotasV1ClientQuotaWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaQuotasV1ClientQuota Update a Client Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a client quota.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /kafka-quotas/v1/client-quotas/{id} (the `UpdateKafkaQuotasV1ClientQuota` operationId).
	updateKafkaQuotasV1ClientQuota(ctx context.Context, id string, body UpdateKafkaQuotasV1ClientQuotaJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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
func (r ListKafkaQuotasV1ClientQuotasResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListKafkaQuotasV1ClientQuotas200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Cluster     interface{} `json:"cluster,omitempty"`
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListKafkaQuotasV1ClientQuotas200JSONResponseBodyKind `json:"kind"`
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
func (r ListKafkaQuotasV1ClientQuotasResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListKafkaQuotasV1ClientQuotasResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListKafkaQuotasV1ClientQuotasResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListKafkaQuotasV1ClientQuotasResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListKafkaQuotasV1ClientQuotasResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaQuotasV1ClientQuotasResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaQuotasV1ClientQuotasResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaQuotasV1ClientQuotasResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateKafkaQuotasV1ClientQuota202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateKafkaQuotasV1ClientQuota202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Cluster     interface{} `json:"cluster,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON202
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKafkaQuotasV1ClientQuotaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaQuotasV1ClientQuotaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetKafkaQuotasV1ClientQuota200JSONResponseBodyKind `json:"kind"`
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
		Cluster     interface{} `json:"cluster,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetKafkaQuotasV1ClientQuotaResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaQuotasV1ClientQuotaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaQuotasV1ClientQuotaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaQuotasV1ClientQuotaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateKafkaQuotasV1ClientQuota200JSONResponseBodyKind `json:"kind"`
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
		Cluster     interface{} `json:"cluster,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaQuotasV1ClientQuotaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
