package ksqldbcm

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
	KsqldbcmV2ClusterApiVersionKsqldbcmv2 KsqldbcmV2ClusterApiVersion = "ksqldbcm/v2"
)

func (e KsqldbcmV2ClusterApiVersion) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	KsqldbcmV2ClusterKindCluster KsqldbcmV2ClusterKind = "Cluster"
)

func (e KsqldbcmV2ClusterKind) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterKindCluster:
		return true
	default:
		return false
	}
}

const (
	KsqldbcmV2ClusterListApiVersionKsqldbcmv2 KsqldbcmV2ClusterListApiVersion = "ksqldbcm/v2"
)

func (e KsqldbcmV2ClusterListApiVersion) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterListApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	KsqldbcmV2ClusterListDataApiVersionKsqldbcmv2 KsqldbcmV2ClusterListDataApiVersion = "ksqldbcm/v2"
)

func (e KsqldbcmV2ClusterListDataApiVersion) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterListDataApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	KsqldbcmV2ClusterListDataKindCluster KsqldbcmV2ClusterListDataKind = "Cluster"
)

func (e KsqldbcmV2ClusterListDataKind) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterListDataKindCluster:
		return true
	default:
		return false
	}
}

const (
	KsqldbcmV2ClusterListKindClusterList KsqldbcmV2ClusterListKind = "ClusterList"
)

func (e KsqldbcmV2ClusterListKind) Valid() bool {
	switch e {
	case KsqldbcmV2ClusterListKindClusterList:
		return true
	default:
		return false
	}
}

const (
	ListKsqldbcmV2Clusters200JSONResponseBodyApiVersionKsqldbcmv2 ListKsqldbcmV2Clusters200JSONResponseBodyApiVersion = "ksqldbcm/v2"
)

func (e ListKsqldbcmV2Clusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListKsqldbcmV2Clusters200JSONResponseBodyApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	ListKsqldbcmV2Clusters200JSONResponseBodyKindClusterList ListKsqldbcmV2Clusters200JSONResponseBodyKind = "ClusterList"
)

func (e ListKsqldbcmV2Clusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListKsqldbcmV2Clusters200JSONResponseBodyKindClusterList:
		return true
	default:
		return false
	}
}

const (
	CreateKsqldbcmV2ClusterJSONBodyApiVersionKsqldbcmv2 CreateKsqldbcmV2ClusterJSONBodyApiVersion = "ksqldbcm/v2"
)

func (e CreateKsqldbcmV2ClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateKsqldbcmV2ClusterJSONBodyApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	CreateKsqldbcmV2ClusterJSONBodyKindCluster CreateKsqldbcmV2ClusterJSONBodyKind = "Cluster"
)

func (e CreateKsqldbcmV2ClusterJSONBodyKind) Valid() bool {
	switch e {
	case CreateKsqldbcmV2ClusterJSONBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	CreateKsqldbcmV2Cluster202JSONResponseBodyApiVersionKsqldbcmv2 CreateKsqldbcmV2Cluster202JSONResponseBodyApiVersion = "ksqldbcm/v2"
)

func (e CreateKsqldbcmV2Cluster202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateKsqldbcmV2Cluster202JSONResponseBodyApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	CreateKsqldbcmV2Cluster202JSONResponseBodyKindCluster CreateKsqldbcmV2Cluster202JSONResponseBodyKind = "Cluster"
)

func (e CreateKsqldbcmV2Cluster202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateKsqldbcmV2Cluster202JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	GetKsqldbcmV2Cluster200JSONResponseBodyApiVersionKsqldbcmv2 GetKsqldbcmV2Cluster200JSONResponseBodyApiVersion = "ksqldbcm/v2"
)

func (e GetKsqldbcmV2Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetKsqldbcmV2Cluster200JSONResponseBodyApiVersionKsqldbcmv2:
		return true
	default:
		return false
	}
}

const (
	GetKsqldbcmV2Cluster200JSONResponseBodyKindCluster GetKsqldbcmV2Cluster200JSONResponseBodyKind = "Cluster"
)

func (e GetKsqldbcmV2Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetKsqldbcmV2Cluster200JSONResponseBodyKindCluster:
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
type TypedGlobalObjectReference struct {
	// ApiVersion API group and version of the referred resource
	ApiVersion *string `json:"api_version,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Kind Kind of the referred resource
	Kind *string `json:"kind,omitempty"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type KsqldbcmV2Cluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *KsqldbcmV2ClusterApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *KsqldbcmV2ClusterKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Cluster
	Spec *KsqldbcmV2ClusterSpec `json:"spec,omitempty"`

	// Status The status of the Cluster
	Status *KsqldbcmV2ClusterStatus `json:"status,omitempty"`
}
type KsqldbcmV2ClusterApiVersion string
type KsqldbcmV2ClusterKind string
type KsqldbcmV2ClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion KsqldbcmV2ClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *KsqldbcmV2ClusterListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *KsqldbcmV2ClusterListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Cluster
		Status KsqldbcmV2ClusterStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     KsqldbcmV2ClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type KsqldbcmV2ClusterListApiVersion string
type KsqldbcmV2ClusterListDataApiVersion string
type KsqldbcmV2ClusterListDataKind string
type KsqldbcmV2ClusterListKind string
type KsqldbcmV2ClusterSpec struct {
	// CredentialIdentity The credential_identity to which this belongs. The credential_identity can be one of iam.v2.User, iam.v2.ServiceAccount.
	CredentialIdentity *TypedGlobalObjectReference `json:"credential_identity,omitempty"`

	// Csu The number of CSUs (Confluent Streaming Units) in a ksqlDB cluster.
	Csu *int32 `json:"csu,omitempty"`

	// DisplayName The name of the ksqlDB cluster.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// KafkaCluster The kafka_cluster to which this belongs.
	KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

	// UseDetailedProcessingLog This flag controls whether you want to include the row data in the processing log topic. Turn it off if you
	// don't want to emit sensitive information to the processing log
	UseDetailedProcessingLog *bool `json:"use_detailed_processing_log,omitempty"`
}
type KsqldbcmV2ClusterStatus struct {
	// HttpEndpoint The dataplane endpoint of the ksqlDB cluster.
	HttpEndpoint *string `json:"http_endpoint,omitempty"`

	// IsPaused Tells you if the cluster has been paused
	IsPaused bool `json:"is_paused"`

	// Phase Status of the ksqlDB cluster.
	Phase string `json:"phase"`

	// Storage Amount of storage (in GB) provisioned to this cluster
	Storage int32 `json:"storage"`

	// TopicPrefix Topic name prefix used by this ksqlDB cluster. Used to assign ACLs for this ksqlDB cluster to use.
	TopicPrefix *string `json:"topic_prefix,omitempty"`
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
type OverQuotaError = Failure
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

	// ListKsqldbcmV2Clusters List of Clusters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Corresponds with GET /ksqldbcm/v2/clusters (the `ListKsqldbcmV2Clusters` operationId).
	ListKsqldbcmV2Clusters(ctx context.Context, params *ListKsqldbcmV2ClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKsqldbcmV2ClusterWithBody Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /ksqldbcm/v2/clusters (the `CreateKsqldbcmV2Cluster` operationId).
	CreateKsqldbcmV2ClusterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKsqldbcmV2Cluster Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /ksqldbcm/v2/clusters (the `CreateKsqldbcmV2Cluster` operationId).
	CreateKsqldbcmV2Cluster(ctx context.Context, body CreateKsqldbcmV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKsqldbcmV2Cluster Delete a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a cluster.
	//
	// Corresponds with DELETE /ksqldbcm/v2/clusters/{id} (the `DeleteKsqldbcmV2Cluster` operationId).
	DeleteKsqldbcmV2Cluster(ctx context.Context, id string, params *DeleteKsqldbcmV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKsqldbcmV2Cluster Read a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Corresponds with GET /ksqldbcm/v2/clusters/{id} (the `GetKsqldbcmV2Cluster` operationId).
	GetKsqldbcmV2Cluster(ctx context.Context, id string, params *GetKsqldbcmV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListKsqldbcmV2ClustersWithResponse List of Clusters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ksqldbcm/v2/clusters (the `ListKsqldbcmV2Clusters` operationId).
	ListKsqldbcmV2ClustersWithResponse(ctx context.Context, params *ListKsqldbcmV2ClustersParams, reqEditors ...RequestEditorFn) (*ListKsqldbcmV2ClustersResponse, error)

	// CreateKsqldbcmV2ClusterWithBodyWithResponse Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ksqldbcm/v2/clusters (the `CreateKsqldbcmV2Cluster` operationId).
	CreateKsqldbcmV2ClusterWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKsqldbcmV2ClusterResponse, error)

	// CreateKsqldbcmV2ClusterWithResponse Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ksqldbcm/v2/clusters (the `CreateKsqldbcmV2Cluster` operationId).
	CreateKsqldbcmV2ClusterWithResponse(ctx context.Context, body CreateKsqldbcmV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKsqldbcmV2ClusterResponse, error)

	// DeleteKsqldbcmV2ClusterWithResponse Delete a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /ksqldbcm/v2/clusters/{id} (the `DeleteKsqldbcmV2Cluster` operationId).
	DeleteKsqldbcmV2ClusterWithResponse(ctx context.Context, id string, params *DeleteKsqldbcmV2ClusterParams, reqEditors ...RequestEditorFn) (*DeleteKsqldbcmV2ClusterResponse, error)

	// GetKsqldbcmV2ClusterWithResponse Read a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ksqldbcm/v2/clusters/{id} (the `GetKsqldbcmV2Cluster` operationId).
	GetKsqldbcmV2ClusterWithResponse(ctx context.Context, id string, params *GetKsqldbcmV2ClusterParams, reqEditors ...RequestEditorFn) (*GetKsqldbcmV2ClusterResponse, error)
}

func (r ListKsqldbcmV2ClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListKsqldbcmV2Clusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			CredentialIdentity interface{} `json:"credential_identity,omitempty"`
			Environment        interface{} `json:"environment,omitempty"`
			KafkaCluster       interface{} `json:"kafka_cluster,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListKsqldbcmV2Clusters200JSONResponseBodyKind `json:"kind"`
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
func (r ListKsqldbcmV2ClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListKsqldbcmV2ClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListKsqldbcmV2ClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListKsqldbcmV2ClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListKsqldbcmV2ClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListKsqldbcmV2ClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKsqldbcmV2ClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKsqldbcmV2ClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateKsqldbcmV2Cluster202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateKsqldbcmV2Cluster202JSONResponseBodyKind `json:"kind,omitempty"`
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
		CredentialIdentity interface{} `json:"credential_identity,omitempty"`
		Environment        interface{} `json:"environment,omitempty"`
		KafkaCluster       interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status KsqldbcmV2ClusterStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateKsqldbcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateKsqldbcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKsqldbcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKsqldbcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKsqldbcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKsqldbcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteKsqldbcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteKsqldbcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteKsqldbcmV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteKsqldbcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteKsqldbcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKsqldbcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKsqldbcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKsqldbcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetKsqldbcmV2Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetKsqldbcmV2Cluster200JSONResponseBodyKind `json:"kind"`
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
		CredentialIdentity interface{} `json:"credential_identity,omitempty"`
		Environment        interface{} `json:"environment,omitempty"`
		KafkaCluster       interface{} `json:"kafka_cluster,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status KsqldbcmV2ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetKsqldbcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetKsqldbcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetKsqldbcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKsqldbcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKsqldbcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
