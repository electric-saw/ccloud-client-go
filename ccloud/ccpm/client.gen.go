package ccpm

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
	ArtifactV1UploadSourcePresignedUrlKindPresignedUrl ArtifactV1UploadSourcePresignedUrlKind = "PresignedUrl"
)

func (e ArtifactV1UploadSourcePresignedUrlKind) Valid() bool {
	switch e {
	case ArtifactV1UploadSourcePresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginApiVersionCcpmv1 CcpmV1CustomConnectPluginApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginKindCustomConnectPlugin CcpmV1CustomConnectPluginKind = "CustomConnectPlugin"
)

func (e CcpmV1CustomConnectPluginKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginListApiVersionCcpmv1 CcpmV1CustomConnectPluginListApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginListApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginListApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginListDataApiVersionCcpmv1 CcpmV1CustomConnectPluginListDataApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginListDataApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginListDataApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginListDataKindCustomConnectPlugin CcpmV1CustomConnectPluginListDataKind = "CustomConnectPlugin"
)

func (e CcpmV1CustomConnectPluginListDataKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginListDataKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginListKindCustomConnectPluginList CcpmV1CustomConnectPluginListKind = "CustomConnectPluginList"
)

func (e CcpmV1CustomConnectPluginListKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginListKindCustomConnectPluginList:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionApiVersionCcpmv1 CcpmV1CustomConnectPluginVersionApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginVersionApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionKindCustomConnectPluginVersion CcpmV1CustomConnectPluginVersionKind = "CustomConnectPluginVersion"
)

func (e CcpmV1CustomConnectPluginVersionKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionKindCustomConnectPluginVersion:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionListApiVersionCcpmv1 CcpmV1CustomConnectPluginVersionListApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginVersionListApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionListApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionListDataApiVersionCcpmv1 CcpmV1CustomConnectPluginVersionListDataApiVersion = "ccpm/v1"
)

func (e CcpmV1CustomConnectPluginVersionListDataApiVersion) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionListDataApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionListDataKindCustomConnectPluginVersion CcpmV1CustomConnectPluginVersionListDataKind = "CustomConnectPluginVersion"
)

func (e CcpmV1CustomConnectPluginVersionListDataKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionListDataKindCustomConnectPluginVersion:
		return true
	default:
		return false
	}
}

const (
	CcpmV1CustomConnectPluginVersionListKindCustomConnectPluginVersionList CcpmV1CustomConnectPluginVersionListKind = "CustomConnectPluginVersionList"
)

func (e CcpmV1CustomConnectPluginVersionListKind) Valid() bool {
	switch e {
	case CcpmV1CustomConnectPluginVersionListKindCustomConnectPluginVersionList:
		return true
	default:
		return false
	}
}

const (
	CcpmV1PresignedUrlApiVersionCcpmv1 CcpmV1PresignedUrlApiVersion = "ccpm/v1"
)

func (e CcpmV1PresignedUrlApiVersion) Valid() bool {
	switch e {
	case CcpmV1PresignedUrlApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CcpmV1PresignedUrlKindPresignedUrl CcpmV1PresignedUrlKind = "PresignedUrl"
)

func (e CcpmV1PresignedUrlKind) Valid() bool {
	switch e {
	case CcpmV1PresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	ListCcpmV1CustomConnectPlugins200JSONResponseBodyApiVersionCcpmv1 ListCcpmV1CustomConnectPlugins200JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e ListCcpmV1CustomConnectPlugins200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCcpmV1CustomConnectPlugins200JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	ListCcpmV1CustomConnectPlugins200JSONResponseBodyKindCustomConnectPluginList ListCcpmV1CustomConnectPlugins200JSONResponseBodyKind = "CustomConnectPluginList"
)

func (e ListCcpmV1CustomConnectPlugins200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCcpmV1CustomConnectPlugins200JSONResponseBodyKindCustomConnectPluginList:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginJSONBodyApiVersionCcpmv1 CreateCcpmV1CustomConnectPluginJSONBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1CustomConnectPluginJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginJSONBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginJSONBodyKindCustomConnectPlugin CreateCcpmV1CustomConnectPluginJSONBodyKind = "CustomConnectPlugin"
)

func (e CreateCcpmV1CustomConnectPluginJSONBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginJSONBodyKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPlugin202JSONResponseBodyApiVersionCcpmv1 CreateCcpmV1CustomConnectPlugin202JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1CustomConnectPlugin202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPlugin202JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPlugin202JSONResponseBodyKindCustomConnectPlugin CreateCcpmV1CustomConnectPlugin202JSONResponseBodyKind = "CustomConnectPlugin"
)

func (e CreateCcpmV1CustomConnectPlugin202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPlugin202JSONResponseBodyKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	GetCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersionCcpmv1 GetCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e GetCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	GetCcpmV1CustomConnectPlugin200JSONResponseBodyKindCustomConnectPlugin GetCcpmV1CustomConnectPlugin200JSONResponseBodyKind = "CustomConnectPlugin"
)

func (e GetCcpmV1CustomConnectPlugin200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCcpmV1CustomConnectPlugin200JSONResponseBodyKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	UpdateCcpmV1CustomConnectPluginJSONBodyApiVersionCcpmv1 UpdateCcpmV1CustomConnectPluginJSONBodyApiVersion = "ccpm/v1"
)

func (e UpdateCcpmV1CustomConnectPluginJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCcpmV1CustomConnectPluginJSONBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCcpmV1CustomConnectPluginJSONBodyKindCustomConnectPlugin UpdateCcpmV1CustomConnectPluginJSONBodyKind = "CustomConnectPlugin"
)

func (e UpdateCcpmV1CustomConnectPluginJSONBodyKind) Valid() bool {
	switch e {
	case UpdateCcpmV1CustomConnectPluginJSONBodyKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersionCcpmv1 UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyKindCustomConnectPlugin UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyKind = "CustomConnectPlugin"
)

func (e UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyKindCustomConnectPlugin:
		return true
	default:
		return false
	}
}

const (
	ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyApiVersionCcpmv1 ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyKindCustomConnectPluginVersionList ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyKind = "CustomConnectPluginVersionList"
)

func (e ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyKindCustomConnectPluginVersionList:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginVersionJSONBodyApiVersionCcpmv1 CreateCcpmV1CustomConnectPluginVersionJSONBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1CustomConnectPluginVersionJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginVersionJSONBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginVersionJSONBodyKindCustomConnectPluginVersion CreateCcpmV1CustomConnectPluginVersionJSONBodyKind = "CustomConnectPluginVersion"
)

func (e CreateCcpmV1CustomConnectPluginVersionJSONBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginVersionJSONBodyKindCustomConnectPluginVersion:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyApiVersionCcpmv1 CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyKindCustomConnectPluginVersion CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyKind = "CustomConnectPluginVersion"
)

func (e CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyKindCustomConnectPluginVersion:
		return true
	default:
		return false
	}
}

const (
	GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyApiVersionCcpmv1 GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyKindCustomConnectPluginVersion GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyKind = "CustomConnectPluginVersion"
)

func (e GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyKindCustomConnectPluginVersion:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1PresignedUrlJSONBodyApiVersionCcpmv1 CreateCcpmV1PresignedUrlJSONBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1PresignedUrlJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1PresignedUrlJSONBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1PresignedUrlJSONBodyKindPresignedUrl CreateCcpmV1PresignedUrlJSONBodyKind = "PresignedUrl"
)

func (e CreateCcpmV1PresignedUrlJSONBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1PresignedUrlJSONBodyKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1PresignedUrl201JSONResponseBodyApiVersionCcpmv1 CreateCcpmV1PresignedUrl201JSONResponseBodyApiVersion = "ccpm/v1"
)

func (e CreateCcpmV1PresignedUrl201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCcpmV1PresignedUrl201JSONResponseBodyApiVersionCcpmv1:
		return true
	default:
		return false
	}
}

const (
	CreateCcpmV1PresignedUrl201JSONResponseBodyKindPresignedUrl CreateCcpmV1PresignedUrl201JSONResponseBodyKind = "PresignedUrl"
)

func (e CreateCcpmV1PresignedUrl201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCcpmV1PresignedUrl201JSONResponseBodyKindPresignedUrl:
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
type CcpmV1ConnectorClass struct {
	// ClassName Java class or alias for connector. You can get connector class from connector documentation provided by developer.
	ClassName string `json:"class_name"`

	// Type Type of the connector class. Should be either `SOURCE` or `SINK`.
	Type string `json:"type"`
}
type CcpmV1CustomConnectPlugin struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CcpmV1CustomConnectPluginApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CcpmV1CustomConnectPluginKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Custom Connect Plugin
	Spec *CcpmV1CustomConnectPluginSpec `json:"spec,omitempty"`
}
type CcpmV1CustomConnectPluginApiVersion string
type CcpmV1CustomConnectPluginKind string
type CcpmV1CustomConnectPluginList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CcpmV1CustomConnectPluginListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CcpmV1CustomConnectPluginListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CcpmV1CustomConnectPluginListDataKind `json:"kind,omitempty"`
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
	Kind     CcpmV1CustomConnectPluginListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CcpmV1CustomConnectPluginListApiVersion string
type CcpmV1CustomConnectPluginListDataApiVersion string
type CcpmV1CustomConnectPluginListDataKind string
type CcpmV1CustomConnectPluginListKind string
type CcpmV1CustomConnectPluginSpec struct {
	// Cloud Cloud provider where the Custom Connect Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// Description Description of Custom Connect Plugin.
	Description *string `json:"description,omitempty"`

	// DisplayName Display name of Custom Connect Plugin.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// RuntimeLanguage Runtime language of Custom Connect Plugin.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`
}
type CcpmV1CustomConnectPluginVersion struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CcpmV1CustomConnectPluginVersionApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CcpmV1CustomConnectPluginVersionKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Custom Connect Plugin Version
	Spec *CcpmV1CustomConnectPluginVersionSpec `json:"spec,omitempty"`

	// Status The status of the Custom Connect Plugin Version
	Status *CcpmV1CustomConnectPluginVersionStatus `json:"status,omitempty"`
}
type CcpmV1CustomConnectPluginVersionApiVersion string
type CcpmV1CustomConnectPluginVersionKind string
type CcpmV1CustomConnectPluginVersionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CcpmV1CustomConnectPluginVersionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CcpmV1CustomConnectPluginVersionListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CcpmV1CustomConnectPluginVersionListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Custom Connect Plugin Version
		Status CcpmV1CustomConnectPluginVersionStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CcpmV1CustomConnectPluginVersionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CcpmV1CustomConnectPluginVersionListApiVersion string
type CcpmV1CustomConnectPluginVersionListDataApiVersion string
type CcpmV1CustomConnectPluginVersionListDataKind string
type CcpmV1CustomConnectPluginVersionListKind string
type CcpmV1CustomConnectPluginVersionSpec struct {
	// ConnectorClasses List of connector classes.
	// The connector class must be a valid Java class name or alias for the connector.
	// You can get the connector class from the connector documentation provided by the developer.
	ConnectorClasses *[]CcpmV1ConnectorClass `json:"connector_classes,omitempty"`

	// ContentFormat Archive format of Custom Connect Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// DocumentationLink Document link of Custom Connect Plugin.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
	// value when setting up connector.
	SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

	// UploadSource Upload source of Custom Connect Plugin Version. Only required in `create` request,
	// will be ignored in `read`, `update` or `list`.
	UploadSource *CcpmV1CustomConnectPluginVersionSpec_UploadSource `json:"upload_source,omitempty"`

	// Version Version of the Custom Connect Plugin.
	// The version must comply with SemVer (e.g., `1.2.3`, `1.2.3-beta`, `1.2.3-rc.123`, `1.2.3-rc.123+build.456`).
	Version *string `json:"version,omitempty"`
}
type CcpmV1CustomConnectPluginVersionSpec_UploadSource struct {
	union json.RawMessage
}
type CcpmV1CustomConnectPluginVersionStatus struct {
	// ErrorMessage Displayable error message if version is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase Phase of the Custom Connect Plugin Version.
	Phase string `json:"phase"`
}
type CcpmV1PresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CcpmV1PresignedUrlApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connect Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Custom Connect Plugin archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CcpmV1PresignedUrlKind `json:"kind,omitempty"`

	// UploadFormData Upload form data of the Custom Connect Plugin. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Custom Connect Plugin archive.
	UploadUrl *string `json:"upload_url,omitempty"`
}
type CcpmV1PresignedUrlApiVersion string
type CcpmV1PresignedUrlKind string
type CcpmV1UploadSourcePresignedUrl struct {
	// Location Location of the Custom Connect Plugin source.
	Location string `json:"location"`

	// UploadId Upload ID returned by the `/presigned-upload-url` API. This field returns an empty string in all responses.
	UploadId string `json:"upload_id"`
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
func (t CcpmV1CustomConnectPluginVersionSpec_UploadSource) AsCcpmV1UploadSourcePresignedUrl() (CcpmV1UploadSourcePresignedUrl, error) {
	var body CcpmV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CcpmV1CustomConnectPluginVersionSpec_UploadSource) FromCcpmV1UploadSourcePresignedUrl(v CcpmV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *CcpmV1CustomConnectPluginVersionSpec_UploadSource) MergeCcpmV1UploadSourcePresignedUrl(v CcpmV1UploadSourcePresignedUrl) error {
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
func (t CcpmV1CustomConnectPluginVersionSpec_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CcpmV1CustomConnectPluginVersionSpec_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsCcpmV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CcpmV1CustomConnectPluginVersionSpec_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CcpmV1CustomConnectPluginVersionSpec_UploadSource) UnmarshalJSON(b []byte) error {
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

type ClientInterface interface {

	// ListCcpmV1CustomConnectPlugins List of Custom Connect Plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connect plugins.
	//
	// If no `cloud` filter is specified, returns custom connect plugins from all clouds.
	//
	// Corresponds with GET /ccpm/v1/plugins (the `ListCcpmV1CustomConnectPlugins` operationId).
	ListCcpmV1CustomConnectPlugins(ctx context.Context, params *ListCcpmV1CustomConnectPluginsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1CustomConnectPluginWithBody Create a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /ccpm/v1/plugins (the `CreateCcpmV1CustomConnectPlugin` operationId).
	CreateCcpmV1CustomConnectPluginWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1CustomConnectPlugin Create a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /ccpm/v1/plugins (the `CreateCcpmV1CustomConnectPlugin` operationId).
	CreateCcpmV1CustomConnectPlugin(ctx context.Context, body CreateCcpmV1CustomConnectPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCcpmV1CustomConnectPlugin Delete a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connect plugin.
	//
	// Corresponds with DELETE /ccpm/v1/plugins/{id} (the `DeleteCcpmV1CustomConnectPlugin` operationId).
	DeleteCcpmV1CustomConnectPlugin(ctx context.Context, id string, params *DeleteCcpmV1CustomConnectPluginParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCcpmV1CustomConnectPlugin Read a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connect plugin.
	//
	// Corresponds with GET /ccpm/v1/plugins/{id} (the `GetCcpmV1CustomConnectPlugin` operationId).
	GetCcpmV1CustomConnectPlugin(ctx context.Context, id string, params *GetCcpmV1CustomConnectPluginParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCcpmV1CustomConnectPluginWithBody Update a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connect plugin.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /ccpm/v1/plugins/{id} (the `UpdateCcpmV1CustomConnectPlugin` operationId).
	UpdateCcpmV1CustomConnectPluginWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCcpmV1CustomConnectPlugin Update a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connect plugin.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /ccpm/v1/plugins/{id} (the `UpdateCcpmV1CustomConnectPlugin` operationId).
	UpdateCcpmV1CustomConnectPlugin(ctx context.Context, id string, body UpdateCcpmV1CustomConnectPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListCcpmV1CustomConnectPluginVersions List of Custom Connect Plugin Versions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connect plugin versions.
	//
	// Corresponds with GET /ccpm/v1/plugins/{plugin_id}/versions (the `ListCcpmV1CustomConnectPluginVersions` operationId).
	ListCcpmV1CustomConnectPluginVersions(ctx context.Context, pluginId string, params *ListCcpmV1CustomConnectPluginVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1CustomConnectPluginVersionWithBody Create a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin version.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /ccpm/v1/plugins/{plugin_id}/versions (the `CreateCcpmV1CustomConnectPluginVersion` operationId).
	CreateCcpmV1CustomConnectPluginVersionWithBody(ctx context.Context, pluginId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1CustomConnectPluginVersion Create a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin version.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /ccpm/v1/plugins/{plugin_id}/versions (the `CreateCcpmV1CustomConnectPluginVersion` operationId).
	CreateCcpmV1CustomConnectPluginVersion(ctx context.Context, pluginId string, body CreateCcpmV1CustomConnectPluginVersionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCcpmV1CustomConnectPluginVersion Delete a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connect plugin version.
	//
	// Corresponds with DELETE /ccpm/v1/plugins/{plugin_id}/versions/{id} (the `DeleteCcpmV1CustomConnectPluginVersion` operationId).
	DeleteCcpmV1CustomConnectPluginVersion(ctx context.Context, pluginId string, id string, params *DeleteCcpmV1CustomConnectPluginVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCcpmV1CustomConnectPluginVersion Read a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connect plugin version.
	//
	// Corresponds with GET /ccpm/v1/plugins/{plugin_id}/versions/{id} (the `GetCcpmV1CustomConnectPluginVersion` operationId).
	GetCcpmV1CustomConnectPluginVersion(ctx context.Context, pluginId string, id string, params *GetCcpmV1CustomConnectPluginVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1PresignedUrlWithBody Request a presigned upload URL for a new Custom Connect Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connect Plugin archive.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /ccpm/v1/presigned-upload-url (the `CreateCcpmV1PresignedUrl` operationId).
	CreateCcpmV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCcpmV1PresignedUrl Request a presigned upload URL for a new Custom Connect Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connect Plugin archive.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /ccpm/v1/presigned-upload-url (the `CreateCcpmV1PresignedUrl` operationId).
	CreateCcpmV1PresignedUrl(ctx context.Context, body CreateCcpmV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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
	return func(c *oasClient) error {
		newBaseURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}
		c.Server = newBaseURL.String()
		return nil
	}
}

type ClientWithResponsesInterface interface {

	// ListCcpmV1CustomConnectPluginsWithResponse List of Custom Connect Plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connect plugins.
	//
	// If no `cloud` filter is specified, returns custom connect plugins from all clouds.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ccpm/v1/plugins (the `ListCcpmV1CustomConnectPlugins` operationId).
	ListCcpmV1CustomConnectPluginsWithResponse(ctx context.Context, params *ListCcpmV1CustomConnectPluginsParams, reqEditors ...RequestEditorFn) (*ListCcpmV1CustomConnectPluginsResponse, error)

	// CreateCcpmV1CustomConnectPluginWithBodyWithResponse Create a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/plugins (the `CreateCcpmV1CustomConnectPlugin` operationId).
	CreateCcpmV1CustomConnectPluginWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCcpmV1CustomConnectPluginResponse, error)

	// CreateCcpmV1CustomConnectPluginWithResponse Create a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/plugins (the `CreateCcpmV1CustomConnectPlugin` operationId).
	CreateCcpmV1CustomConnectPluginWithResponse(ctx context.Context, body CreateCcpmV1CustomConnectPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCcpmV1CustomConnectPluginResponse, error)

	// DeleteCcpmV1CustomConnectPluginWithResponse Delete a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connect plugin.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /ccpm/v1/plugins/{id} (the `DeleteCcpmV1CustomConnectPlugin` operationId).
	DeleteCcpmV1CustomConnectPluginWithResponse(ctx context.Context, id string, params *DeleteCcpmV1CustomConnectPluginParams, reqEditors ...RequestEditorFn) (*DeleteCcpmV1CustomConnectPluginResponse, error)

	// GetCcpmV1CustomConnectPluginWithResponse Read a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connect plugin.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ccpm/v1/plugins/{id} (the `GetCcpmV1CustomConnectPlugin` operationId).
	GetCcpmV1CustomConnectPluginWithResponse(ctx context.Context, id string, params *GetCcpmV1CustomConnectPluginParams, reqEditors ...RequestEditorFn) (*GetCcpmV1CustomConnectPluginResponse, error)

	// UpdateCcpmV1CustomConnectPluginWithBodyWithResponse Update a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connect plugin.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /ccpm/v1/plugins/{id} (the `UpdateCcpmV1CustomConnectPlugin` operationId).
	UpdateCcpmV1CustomConnectPluginWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateCcpmV1CustomConnectPluginResponse, error)

	// UpdateCcpmV1CustomConnectPluginWithResponse Update a Custom Connect Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connect plugin.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /ccpm/v1/plugins/{id} (the `UpdateCcpmV1CustomConnectPlugin` operationId).
	UpdateCcpmV1CustomConnectPluginWithResponse(ctx context.Context, id string, body UpdateCcpmV1CustomConnectPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateCcpmV1CustomConnectPluginResponse, error)

	// ListCcpmV1CustomConnectPluginVersionsWithResponse List of Custom Connect Plugin Versions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connect plugin versions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ccpm/v1/plugins/{plugin_id}/versions (the `ListCcpmV1CustomConnectPluginVersions` operationId).
	ListCcpmV1CustomConnectPluginVersionsWithResponse(ctx context.Context, pluginId string, params *ListCcpmV1CustomConnectPluginVersionsParams, reqEditors ...RequestEditorFn) (*ListCcpmV1CustomConnectPluginVersionsResponse, error)

	// CreateCcpmV1CustomConnectPluginVersionWithBodyWithResponse Create a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin version.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/plugins/{plugin_id}/versions (the `CreateCcpmV1CustomConnectPluginVersion` operationId).
	CreateCcpmV1CustomConnectPluginVersionWithBodyWithResponse(ctx context.Context, pluginId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCcpmV1CustomConnectPluginVersionResponse, error)

	// CreateCcpmV1CustomConnectPluginVersionWithResponse Create a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connect plugin version.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/plugins/{plugin_id}/versions (the `CreateCcpmV1CustomConnectPluginVersion` operationId).
	CreateCcpmV1CustomConnectPluginVersionWithResponse(ctx context.Context, pluginId string, body CreateCcpmV1CustomConnectPluginVersionJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCcpmV1CustomConnectPluginVersionResponse, error)

	// DeleteCcpmV1CustomConnectPluginVersionWithResponse Delete a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connect plugin version.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /ccpm/v1/plugins/{plugin_id}/versions/{id} (the `DeleteCcpmV1CustomConnectPluginVersion` operationId).
	DeleteCcpmV1CustomConnectPluginVersionWithResponse(ctx context.Context, pluginId string, id string, params *DeleteCcpmV1CustomConnectPluginVersionParams, reqEditors ...RequestEditorFn) (*DeleteCcpmV1CustomConnectPluginVersionResponse, error)

	// GetCcpmV1CustomConnectPluginVersionWithResponse Read a Custom Connect Plugin Version
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connect plugin version.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /ccpm/v1/plugins/{plugin_id}/versions/{id} (the `GetCcpmV1CustomConnectPluginVersion` operationId).
	GetCcpmV1CustomConnectPluginVersionWithResponse(ctx context.Context, pluginId string, id string, params *GetCcpmV1CustomConnectPluginVersionParams, reqEditors ...RequestEditorFn) (*GetCcpmV1CustomConnectPluginVersionResponse, error)

	// CreateCcpmV1PresignedUrlWithBodyWithResponse Request a presigned upload URL for a new Custom Connect Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connect Plugin archive.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/presigned-upload-url (the `CreateCcpmV1PresignedUrl` operationId).
	CreateCcpmV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCcpmV1PresignedUrlResponse, error)

	// CreateCcpmV1PresignedUrlWithResponse Request a presigned upload URL for a new Custom Connect Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connect Plugin archive.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /ccpm/v1/presigned-upload-url (the `CreateCcpmV1PresignedUrl` operationId).
	CreateCcpmV1PresignedUrlWithResponse(ctx context.Context, body CreateCcpmV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCcpmV1PresignedUrlResponse, error)
}

func (r ListCcpmV1CustomConnectPluginsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCcpmV1CustomConnectPlugins200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCcpmV1CustomConnectPlugins200JSONResponseBodyKind `json:"kind"`
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
func (r ListCcpmV1CustomConnectPluginsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCcpmV1CustomConnectPluginsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCcpmV1CustomConnectPluginsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCcpmV1CustomConnectPluginsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCcpmV1CustomConnectPluginsResponse) GetBody() []byte {
	return r.Body
}
func (r ListCcpmV1CustomConnectPluginsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCcpmV1CustomConnectPluginsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCcpmV1CustomConnectPluginsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCcpmV1CustomConnectPlugin202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateCcpmV1CustomConnectPlugin202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON202
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCcpmV1CustomConnectPluginResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCcpmV1CustomConnectPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCcpmV1CustomConnectPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCcpmV1CustomConnectPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCcpmV1CustomConnectPluginResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCcpmV1CustomConnectPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCcpmV1CustomConnectPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCcpmV1CustomConnectPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCcpmV1CustomConnectPlugin200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCcpmV1CustomConnectPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCcpmV1CustomConnectPluginResponse) GetBody() []byte {
	return r.Body
}
func (r GetCcpmV1CustomConnectPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCcpmV1CustomConnectPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCcpmV1CustomConnectPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateCcpmV1CustomConnectPlugin200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateCcpmV1CustomConnectPluginResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateCcpmV1CustomConnectPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateCcpmV1CustomConnectPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateCcpmV1CustomConnectPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCcpmV1CustomConnectPluginVersions200JSONResponseBodyKind `json:"kind"`
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
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCcpmV1CustomConnectPluginVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateCcpmV1CustomConnectPluginVersion202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Custom Connect Plugin Version
	Status CcpmV1CustomConnectPluginVersionStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCcpmV1CustomConnectPluginVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCcpmV1CustomConnectPluginVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCcpmV1CustomConnectPluginVersion200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Custom Connect Plugin Version
	Status CcpmV1CustomConnectPluginVersionStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) GetBody() []byte {
	return r.Body
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCcpmV1CustomConnectPluginVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCcpmV1PresignedUrl201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connect Plugin archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Content format of the Custom Connect Plugin archive.
	ContentFormat string      `json:"content_format"`
	Environment   interface{} `json:"environment"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreateCcpmV1PresignedUrl201JSONResponseBodyKind `json:"kind,omitempty"`

	// UploadFormData Upload form data of the Custom Connect Plugin. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Custom Connect Plugin archive.
	UploadUrl *string `json:"upload_url,omitempty"`
} {
	return r.JSON201
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCcpmV1PresignedUrlResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCcpmV1PresignedUrlResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCcpmV1PresignedUrlResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCcpmV1PresignedUrlResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCcpmV1PresignedUrlResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
