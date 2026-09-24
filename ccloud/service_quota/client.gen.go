package service_quota

import (
	"context"
	"encoding/json"
	"errors"
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
	ServiceQuotaV1AppliedQuotaApiVersionServiceQuotav1 ServiceQuotaV1AppliedQuotaApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1AppliedQuotaApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1AppliedQuotaKindAppliedQuota ServiceQuotaV1AppliedQuotaKind = "AppliedQuota"
)

func (e ServiceQuotaV1AppliedQuotaKind) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaKindAppliedQuota:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1AppliedQuotaListApiVersionServiceQuotav1 ServiceQuotaV1AppliedQuotaListApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1AppliedQuotaListApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaListApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1AppliedQuotaListDataApiVersionServiceQuotav1 ServiceQuotaV1AppliedQuotaListDataApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1AppliedQuotaListDataApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaListDataApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1AppliedQuotaListDataKindAppliedQuota ServiceQuotaV1AppliedQuotaListDataKind = "AppliedQuota"
)

func (e ServiceQuotaV1AppliedQuotaListDataKind) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaListDataKindAppliedQuota:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1AppliedQuotaListKindAppliedQuotaList ServiceQuotaV1AppliedQuotaListKind = "AppliedQuotaList"
)

func (e ServiceQuotaV1AppliedQuotaListKind) Valid() bool {
	switch e {
	case ServiceQuotaV1AppliedQuotaListKindAppliedQuotaList:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1ScopeApiVersionServiceQuotav1 ServiceQuotaV1ScopeApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1ScopeApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1ScopeApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1ScopeKindScope ServiceQuotaV1ScopeKind = "Scope"
)

func (e ServiceQuotaV1ScopeKind) Valid() bool {
	switch e {
	case ServiceQuotaV1ScopeKindScope:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1ScopeListApiVersionServiceQuotav1 ServiceQuotaV1ScopeListApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1ScopeListApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1ScopeListApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1ScopeListDataApiVersionServiceQuotav1 ServiceQuotaV1ScopeListDataApiVersion = "service-quota/v1"
)

func (e ServiceQuotaV1ScopeListDataApiVersion) Valid() bool {
	switch e {
	case ServiceQuotaV1ScopeListDataApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ServiceQuotaV1ScopeListDataKindScope ServiceQuotaV1ScopeListDataKind = "Scope"
)

func (e ServiceQuotaV1ScopeListDataKind) Valid() bool {
	switch e {
	case ServiceQuotaV1ScopeListDataKindScope:
		return true
	default:
		return false
	}
}

const (
	ScopeList ServiceQuotaV1ScopeListKind = "ScopeList"
)

func (e ServiceQuotaV1ScopeListKind) Valid() bool {
	switch e {
	case ScopeList:
		return true
	default:
		return false
	}
}

const (
	ListServiceQuotaV1AppliedQuotas200JSONResponseBodyApiVersionServiceQuotav1 ListServiceQuotaV1AppliedQuotas200JSONResponseBodyApiVersion = "service-quota/v1"
)

func (e ListServiceQuotaV1AppliedQuotas200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListServiceQuotaV1AppliedQuotas200JSONResponseBodyApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	ListServiceQuotaV1AppliedQuotas200JSONResponseBodyKindAppliedQuotaList ListServiceQuotaV1AppliedQuotas200JSONResponseBodyKind = "AppliedQuotaList"
)

func (e ListServiceQuotaV1AppliedQuotas200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListServiceQuotaV1AppliedQuotas200JSONResponseBodyKindAppliedQuotaList:
		return true
	default:
		return false
	}
}

const (
	GetServiceQuotaV1AppliedQuota200JSONResponseBodyApiVersionServiceQuotav1 GetServiceQuotaV1AppliedQuota200JSONResponseBodyApiVersion = "service-quota/v1"
)

func (e GetServiceQuotaV1AppliedQuota200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetServiceQuotaV1AppliedQuota200JSONResponseBodyApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	GetServiceQuotaV1AppliedQuota200JSONResponseBodyKindAppliedQuota GetServiceQuotaV1AppliedQuota200JSONResponseBodyKind = "AppliedQuota"
)

func (e GetServiceQuotaV1AppliedQuota200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetServiceQuotaV1AppliedQuota200JSONResponseBodyKindAppliedQuota:
		return true
	default:
		return false
	}
}

const (
	GetServiceQuotaV1Scope200JSONResponseBodyApiVersionServiceQuotav1 GetServiceQuotaV1Scope200JSONResponseBodyApiVersion = "service-quota/v1"
)

func (e GetServiceQuotaV1Scope200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetServiceQuotaV1Scope200JSONResponseBodyApiVersionServiceQuotav1:
		return true
	default:
		return false
	}
}

const (
	GetServiceQuotaV1Scope200JSONResponseBodyKindScope GetServiceQuotaV1Scope200JSONResponseBodyKind = "Scope"
)

func (e GetServiceQuotaV1Scope200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetServiceQuotaV1Scope200JSONResponseBodyKindScope:
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
type ServiceQuotaV1AppliedQuota struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ServiceQuotaV1AppliedQuotaApiVersion `json:"api_version,omitempty"`

	// AppliedLimit The latest applied service quota value, taking into account any limit adjustments.
	AppliedLimit *int32 `json:"applied_limit,omitempty"`

	// DefaultLimit The default service quota value.
	DefaultLimit *int32 `json:"default_limit,omitempty"`

	// DisplayName A human-readable name for the quota type name.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment ID the quota is associated with.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// KafkaCluster The kafka cluster ID the quota is associated with.
	KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ServiceQuotaV1AppliedQuotaKind `json:"kind,omitempty"`
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

	// Network The network ID the quota is associated with.
	Network *EnvScopedObjectReference `json:"network,omitempty"`

	// Organization A unique organization id to associate a specific organization to this quota.
	Organization *GlobalObjectReference `json:"organization,omitempty"`

	// Scope The applied scope that this quota belongs to.
	Scope *string `json:"scope,omitempty"`

	// Usage Show the quota usage value if the quota usage is available for this quota.
	Usage *int32 `json:"usage,omitempty"`

	// User The user associated with this object.
	User *GlobalObjectReference `json:"user,omitempty"`
}
type ServiceQuotaV1AppliedQuotaApiVersion string
type ServiceQuotaV1AppliedQuotaKind string
type ServiceQuotaV1AppliedQuotaList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ServiceQuotaV1AppliedQuotaListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ServiceQuotaV1AppliedQuotaListDataApiVersion `json:"api_version,omitempty"`

		// AppliedLimit The latest applied service quota value, taking into account any limit adjustments.
		AppliedLimit int32 `json:"applied_limit"`

		// DefaultLimit The default service quota value.
		DefaultLimit int32 `json:"default_limit"`

		// DisplayName A human-readable name for the quota type name.
		DisplayName string `json:"display_name"`

		// Environment The environment ID the quota is associated with.
		Environment *GlobalObjectReference `json:"environment,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// KafkaCluster The kafka cluster ID the quota is associated with.
		KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *ServiceQuotaV1AppliedQuotaListDataKind `json:"kind,omitempty"`
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

		// Network The network ID the quota is associated with.
		Network *EnvScopedObjectReference `json:"network,omitempty"`

		// Organization A unique organization id to associate a specific organization to this quota.
		Organization *GlobalObjectReference `json:"organization,omitempty"`

		// Scope The applied scope that this quota belongs to.
		Scope string `json:"scope"`

		// Usage Show the quota usage value if the quota usage is available for this quota.
		Usage *int32 `json:"usage,omitempty"`

		// User The user associated with this object.
		User *GlobalObjectReference `json:"user,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ServiceQuotaV1AppliedQuotaListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ServiceQuotaV1AppliedQuotaListApiVersion string
type ServiceQuotaV1AppliedQuotaListDataApiVersion string
type ServiceQuotaV1AppliedQuotaListDataKind string
type ServiceQuotaV1AppliedQuotaListKind string
type ServiceQuotaV1Scope struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ServiceQuotaV1ScopeApiVersion `json:"api_version,omitempty"`

	// Description the quota scope for listing quotas queries
	Description *string `json:"description,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ServiceQuotaV1ScopeKind `json:"kind,omitempty"`
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
}
type ServiceQuotaV1ScopeApiVersion string
type ServiceQuotaV1ScopeKind string
type ServiceQuotaV1ScopeList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ServiceQuotaV1ScopeListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ServiceQuotaV1ScopeListDataApiVersion `json:"api_version,omitempty"`

		// Description the quota scope for listing quotas queries
		Description string `json:"description"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *ServiceQuotaV1ScopeListDataKind `json:"kind,omitempty"`
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
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ServiceQuotaV1ScopeListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ServiceQuotaV1ScopeListApiVersion string
type ServiceQuotaV1ScopeListDataApiVersion string
type ServiceQuotaV1ScopeListDataKind string
type ServiceQuotaV1ScopeListKind string
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
type DefaultSystemError = Failure
type NotFoundError = Failure
type UnauthenticatedError = Failure
type UnauthorizedError = Failure

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

	// ListServiceQuotaV1AppliedQuotas List of Applied Quotas
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all applied quotas.
	//
	// Shows all quotas for a given scope.
	//
	// Corresponds with GET /service-quota/v1/applied-quotas (the `ListServiceQuotaV1AppliedQuotas` operationId).
	ListServiceQuotaV1AppliedQuotas(ctx context.Context, params *ListServiceQuotaV1AppliedQuotasParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetServiceQuotaV1AppliedQuota Read an Applied Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an applied quota.
	//
	// Corresponds with GET /service-quota/v1/applied-quotas/{id} (the `GetServiceQuotaV1AppliedQuota` operationId).
	GetServiceQuotaV1AppliedQuota(ctx context.Context, id string, params *GetServiceQuotaV1AppliedQuotaParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListServiceQuotaV1Scopes List of Scopes
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all scopes.
	//
	// Corresponds with GET /service-quota/v1/scopes (the `ListServiceQuotaV1Scopes` operationId).
	ListServiceQuotaV1Scopes(ctx context.Context, params *ListServiceQuotaV1ScopesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetServiceQuotaV1Scope Read a Scope
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a scope.
	//
	// Corresponds with GET /service-quota/v1/scopes/{id} (the `GetServiceQuotaV1Scope` operationId).
	GetServiceQuotaV1Scope(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListServiceQuotaV1AppliedQuotasWithResponse List of Applied Quotas
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all applied quotas.
	//
	// Shows all quotas for a given scope.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /service-quota/v1/applied-quotas (the `ListServiceQuotaV1AppliedQuotas` operationId).
	ListServiceQuotaV1AppliedQuotasWithResponse(ctx context.Context, params *ListServiceQuotaV1AppliedQuotasParams, reqEditors ...RequestEditorFn) (*ListServiceQuotaV1AppliedQuotasResponse, error)

	// GetServiceQuotaV1AppliedQuotaWithResponse Read an Applied Quota
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an applied quota.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /service-quota/v1/applied-quotas/{id} (the `GetServiceQuotaV1AppliedQuota` operationId).
	GetServiceQuotaV1AppliedQuotaWithResponse(ctx context.Context, id string, params *GetServiceQuotaV1AppliedQuotaParams, reqEditors ...RequestEditorFn) (*GetServiceQuotaV1AppliedQuotaResponse, error)

	// ListServiceQuotaV1ScopesWithResponse List of Scopes
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all scopes.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /service-quota/v1/scopes (the `ListServiceQuotaV1Scopes` operationId).
	ListServiceQuotaV1ScopesWithResponse(ctx context.Context, params *ListServiceQuotaV1ScopesParams, reqEditors ...RequestEditorFn) (*ListServiceQuotaV1ScopesResponse, error)

	// GetServiceQuotaV1ScopeWithResponse Read a Scope
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a scope.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /service-quota/v1/scopes/{id} (the `GetServiceQuotaV1Scope` operationId).
	GetServiceQuotaV1ScopeWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetServiceQuotaV1ScopeResponse, error)
}

func (r ListServiceQuotaV1AppliedQuotasResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListServiceQuotaV1AppliedQuotas200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment  interface{} `json:"environment,omitempty"`
		KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
		Network      interface{} `json:"network,omitempty"`
		Organization interface{} `json:"organization,omitempty"`
		User         interface{} `json:"user,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListServiceQuotaV1AppliedQuotas200JSONResponseBodyKind `json:"kind"`
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
func (r ListServiceQuotaV1AppliedQuotasResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListServiceQuotaV1AppliedQuotasResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListServiceQuotaV1AppliedQuotasResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListServiceQuotaV1AppliedQuotasResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListServiceQuotaV1AppliedQuotasResponse) GetBody() []byte {
	return r.Body
}
func (r ListServiceQuotaV1AppliedQuotasResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListServiceQuotaV1AppliedQuotasResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListServiceQuotaV1AppliedQuotasResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetServiceQuotaV1AppliedQuota200JSONResponseBodyApiVersion `json:"api_version"`

	// AppliedLimit The latest applied service quota value, taking into account any limit adjustments.
	AppliedLimit int32 `json:"applied_limit"`

	// DefaultLimit The default service quota value.
	DefaultLimit int32 `json:"default_limit"`

	// DisplayName A human-readable name for the quota type name.
	DisplayName string      `json:"display_name"`
	Environment interface{} `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id           string      `json:"id"`
	KafkaCluster interface{} `json:"kafka_cluster,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetServiceQuotaV1AppliedQuota200JSONResponseBodyKind `json:"kind"`
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
	Network      interface{} `json:"network,omitempty"`
	Organization interface{} `json:"organization,omitempty"`

	// Scope The applied scope that this quota belongs to.
	Scope string `json:"scope"`

	// Usage Show the quota usage value if the quota usage is available for this quota.
	Usage *int32      `json:"usage,omitempty"`
	User  interface{} `json:"user,omitempty"`
} {
	return r.JSON200
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetServiceQuotaV1AppliedQuotaResponse) GetBody() []byte {
	return r.Body
}
func (r GetServiceQuotaV1AppliedQuotaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetServiceQuotaV1AppliedQuotaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetServiceQuotaV1AppliedQuotaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListServiceQuotaV1ScopesResponse) GetJSON200() *ServiceQuotaV1ScopeList {
	return r.JSON200
}
func (r ListServiceQuotaV1ScopesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListServiceQuotaV1ScopesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListServiceQuotaV1ScopesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListServiceQuotaV1ScopesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListServiceQuotaV1ScopesResponse) GetBody() []byte {
	return r.Body
}
func (r ListServiceQuotaV1ScopesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListServiceQuotaV1ScopesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListServiceQuotaV1ScopesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetServiceQuotaV1Scope200JSONResponseBodyApiVersion `json:"api_version"`

	// Description the quota scope for listing quotas queries
	Description string `json:"description"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetServiceQuotaV1Scope200JSONResponseBodyKind `json:"kind"`
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
} {
	return r.JSON200
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetServiceQuotaV1ScopeResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetServiceQuotaV1ScopeResponse) GetBody() []byte {
	return r.Body
}
func (r GetServiceQuotaV1ScopeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetServiceQuotaV1ScopeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetServiceQuotaV1ScopeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
