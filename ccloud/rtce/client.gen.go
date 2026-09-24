package rtce

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
	RtceV1RegionApiVersionRtcev1 RtceV1RegionApiVersion = "rtce/v1"
)

func (e RtceV1RegionApiVersion) Valid() bool {
	switch e {
	case RtceV1RegionApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RegionKindRegion RtceV1RegionKind = "Region"
)

func (e RtceV1RegionKind) Valid() bool {
	switch e {
	case RtceV1RegionKindRegion:
		return true
	default:
		return false
	}
}

const (
	RtceV1RegionListApiVersionRtcev1 RtceV1RegionListApiVersion = "rtce/v1"
)

func (e RtceV1RegionListApiVersion) Valid() bool {
	switch e {
	case RtceV1RegionListApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RegionListDataApiVersionRtcev1 RtceV1RegionListDataApiVersion = "rtce/v1"
)

func (e RtceV1RegionListDataApiVersion) Valid() bool {
	switch e {
	case RtceV1RegionListDataApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RegionListDataKindRegion RtceV1RegionListDataKind = "Region"
)

func (e RtceV1RegionListDataKind) Valid() bool {
	switch e {
	case RtceV1RegionListDataKindRegion:
		return true
	default:
		return false
	}
}

const (
	RegionList RtceV1RegionListKind = "RegionList"
)

func (e RtceV1RegionListKind) Valid() bool {
	switch e {
	case RegionList:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicApiVersionRtcev1 RtceV1RtceTopicApiVersion = "rtce/v1"
)

func (e RtceV1RtceTopicApiVersion) Valid() bool {
	switch e {
	case RtceV1RtceTopicApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicKindRtceTopic RtceV1RtceTopicKind = "RtceTopic"
)

func (e RtceV1RtceTopicKind) Valid() bool {
	switch e {
	case RtceV1RtceTopicKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicListApiVersionRtcev1 RtceV1RtceTopicListApiVersion = "rtce/v1"
)

func (e RtceV1RtceTopicListApiVersion) Valid() bool {
	switch e {
	case RtceV1RtceTopicListApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicListDataApiVersionRtcev1 RtceV1RtceTopicListDataApiVersion = "rtce/v1"
)

func (e RtceV1RtceTopicListDataApiVersion) Valid() bool {
	switch e {
	case RtceV1RtceTopicListDataApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicListDataKindRtceTopic RtceV1RtceTopicListDataKind = "RtceTopic"
)

func (e RtceV1RtceTopicListDataKind) Valid() bool {
	switch e {
	case RtceV1RtceTopicListDataKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	RtceV1RtceTopicListKindRtceTopicList RtceV1RtceTopicListKind = "RtceTopicList"
)

func (e RtceV1RtceTopicListKind) Valid() bool {
	switch e {
	case RtceV1RtceTopicListKindRtceTopicList:
		return true
	default:
		return false
	}
}

const (
	ListRtceV1RtceTopics200JSONResponseBodyApiVersionRtcev1 ListRtceV1RtceTopics200JSONResponseBodyApiVersion = "rtce/v1"
)

func (e ListRtceV1RtceTopics200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListRtceV1RtceTopics200JSONResponseBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	ListRtceV1RtceTopics200JSONResponseBodyKindRtceTopicList ListRtceV1RtceTopics200JSONResponseBodyKind = "RtceTopicList"
)

func (e ListRtceV1RtceTopics200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListRtceV1RtceTopics200JSONResponseBodyKindRtceTopicList:
		return true
	default:
		return false
	}
}

const (
	CreateRtceV1RtceTopicJSONBodyApiVersionRtcev1 CreateRtceV1RtceTopicJSONBodyApiVersion = "rtce/v1"
)

func (e CreateRtceV1RtceTopicJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateRtceV1RtceTopicJSONBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	CreateRtceV1RtceTopicJSONBodyKindRtceTopic CreateRtceV1RtceTopicJSONBodyKind = "RtceTopic"
)

func (e CreateRtceV1RtceTopicJSONBodyKind) Valid() bool {
	switch e {
	case CreateRtceV1RtceTopicJSONBodyKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	CreateRtceV1RtceTopic202JSONResponseBodyApiVersionRtcev1 CreateRtceV1RtceTopic202JSONResponseBodyApiVersion = "rtce/v1"
)

func (e CreateRtceV1RtceTopic202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateRtceV1RtceTopic202JSONResponseBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	CreateRtceV1RtceTopic202JSONResponseBodyKindRtceTopic CreateRtceV1RtceTopic202JSONResponseBodyKind = "RtceTopic"
)

func (e CreateRtceV1RtceTopic202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateRtceV1RtceTopic202JSONResponseBodyKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	GetRtceV1RtceTopic200JSONResponseBodyApiVersionRtcev1 GetRtceV1RtceTopic200JSONResponseBodyApiVersion = "rtce/v1"
)

func (e GetRtceV1RtceTopic200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetRtceV1RtceTopic200JSONResponseBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	GetRtceV1RtceTopic200JSONResponseBodyKindRtceTopic GetRtceV1RtceTopic200JSONResponseBodyKind = "RtceTopic"
)

func (e GetRtceV1RtceTopic200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetRtceV1RtceTopic200JSONResponseBodyKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	UpdateRtceV1RtceTopicJSONBodyApiVersionRtcev1 UpdateRtceV1RtceTopicJSONBodyApiVersion = "rtce/v1"
)

func (e UpdateRtceV1RtceTopicJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateRtceV1RtceTopicJSONBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	UpdateRtceV1RtceTopicJSONBodyKindRtceTopic UpdateRtceV1RtceTopicJSONBodyKind = "RtceTopic"
)

func (e UpdateRtceV1RtceTopicJSONBodyKind) Valid() bool {
	switch e {
	case UpdateRtceV1RtceTopicJSONBodyKindRtceTopic:
		return true
	default:
		return false
	}
}

const (
	UpdateRtceV1RtceTopic200JSONResponseBodyApiVersionRtcev1 UpdateRtceV1RtceTopic200JSONResponseBodyApiVersion = "rtce/v1"
)

func (e UpdateRtceV1RtceTopic200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateRtceV1RtceTopic200JSONResponseBodyApiVersionRtcev1:
		return true
	default:
		return false
	}
}

const (
	UpdateRtceV1RtceTopic200JSONResponseBodyKindRtceTopic UpdateRtceV1RtceTopic200JSONResponseBodyKind = "RtceTopic"
)

func (e UpdateRtceV1RtceTopic200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateRtceV1RtceTopic200JSONResponseBodyKindRtceTopic:
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
type RtceV1Region struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *RtceV1RegionApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider that hosts the region.
	Cloud *string `json:"cloud,omitempty"`

	// DisplayName The human-readable display name for the region.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *RtceV1RegionKind `json:"kind,omitempty"`
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
type RtceV1RegionApiVersion string
type RtceV1RegionKind string
type RtceV1RegionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion RtceV1RegionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *RtceV1RegionListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider that hosts the region.
		Cloud string `json:"cloud"`

		// DisplayName The human-readable display name for the region.
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *RtceV1RegionListDataKind `json:"kind,omitempty"`
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
	Kind     RtceV1RegionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type RtceV1RegionListApiVersion string
type RtceV1RegionListDataApiVersion string
type RtceV1RegionListDataKind string
type RtceV1RegionListKind string
type RtceV1RtceTopic struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *RtceV1RtceTopicApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *RtceV1RtceTopicKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Rtce Topic
	Spec *RtceV1RtceTopicSpec `json:"spec,omitempty"`

	// Status The status of the Rtce Topic
	Status *RtceV1RtceTopicStatus `json:"status,omitempty"`
}
type RtceV1RtceTopicApiVersion string
type RtceV1RtceTopicKind string
type RtceV1RtceTopicList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion RtceV1RtceTopicListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *RtceV1RtceTopicListDataApiVersion `json:"api_version,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *RtceV1RtceTopicListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Rtce Topic
		Status RtceV1RtceTopicStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     RtceV1RtceTopicListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type RtceV1RtceTopicListApiVersion string
type RtceV1RtceTopicListDataApiVersion string
type RtceV1RtceTopicListDataKind string
type RtceV1RtceTopicListKind string
type RtceV1RtceTopicSpec struct {
	// Cloud The cloud provider where the RTCE topic is deployed.
	Cloud *string `json:"cloud,omitempty"`

	// Description A model-readable description of the RTCE topic.
	Description *string `json:"description,omitempty"`

	// Environment The environment to which the target Kafka cluster belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// KafkaCluster The Kafka cluster containing the topic to be materialized.
	KafkaCluster *EnvScopedObjectReference `json:"kafka_cluster,omitempty"`

	// Region The cloud region where the RTCE topic is deployed.
	Region *string `json:"region,omitempty"`

	// TopicName The Kafka topic name containing the data for the RTCE topic.
	TopicName *string `json:"topic_name,omitempty"`
}
type RtceV1RtceTopicStatus struct {
	// ErrorMessage Displayable error message if RtceTopic is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the RtceTopic:
	//
	//   PENDING: RtceTopic is pending initial validation;
	//
	//   PROVISIONING: RtceTopic infrastructure is being provisioned;
	//
	//   ACTIVE: RtceTopic is active and ready for use;
	//
	//   DELETING: RtceTopic is being deleted;
	//
	//   FAILED: RtceTopic provisioning failed;
	//
	//   UNAVAILABLE: RtceTopic is temporarily unavailable.
	Phase string `json:"phase"`
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

	// ListRtceV1Regions List of Regions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all regions.
	//
	// Corresponds with GET /rtce/v1/regions (the `ListRtceV1Regions` operationId).
	ListRtceV1Regions(ctx context.Context, params *ListRtceV1RegionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListRtceV1RtceTopics List of Rtce Topics
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all rtce topics.
	//
	// Corresponds with GET /rtce/v1/rtce-topics (the `ListRtceV1RtceTopics` operationId).
	ListRtceV1RtceTopics(ctx context.Context, params *ListRtceV1RtceTopicsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateRtceV1RtceTopicWithBody Create a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a rtce topic.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /rtce/v1/rtce-topics (the `CreateRtceV1RtceTopic` operationId).
	CreateRtceV1RtceTopicWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateRtceV1RtceTopic Create a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a rtce topic.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /rtce/v1/rtce-topics (the `CreateRtceV1RtceTopic` operationId).
	CreateRtceV1RtceTopic(ctx context.Context, body CreateRtceV1RtceTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteRtceV1RtceTopic Delete a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a rtce topic.
	//
	// Corresponds with DELETE /rtce/v1/rtce-topics/{topic_name} (the `DeleteRtceV1RtceTopic` operationId).
	DeleteRtceV1RtceTopic(ctx context.Context, topicName string, params *DeleteRtceV1RtceTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetRtceV1RtceTopic Read a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a rtce topic.
	//
	// Corresponds with GET /rtce/v1/rtce-topics/{topic_name} (the `GetRtceV1RtceTopic` operationId).
	GetRtceV1RtceTopic(ctx context.Context, topicName string, params *GetRtceV1RtceTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateRtceV1RtceTopicWithBody Update a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a rtce topic.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /rtce/v1/rtce-topics/{topic_name} (the `UpdateRtceV1RtceTopic` operationId).
	UpdateRtceV1RtceTopicWithBody(ctx context.Context, topicName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateRtceV1RtceTopic Update a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a rtce topic.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /rtce/v1/rtce-topics/{topic_name} (the `UpdateRtceV1RtceTopic` operationId).
	UpdateRtceV1RtceTopic(ctx context.Context, topicName string, body UpdateRtceV1RtceTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListRtceV1RegionsWithResponse List of Regions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all regions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /rtce/v1/regions (the `ListRtceV1Regions` operationId).
	ListRtceV1RegionsWithResponse(ctx context.Context, params *ListRtceV1RegionsParams, reqEditors ...RequestEditorFn) (*ListRtceV1RegionsResponse, error)

	// ListRtceV1RtceTopicsWithResponse List of Rtce Topics
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all rtce topics.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /rtce/v1/rtce-topics (the `ListRtceV1RtceTopics` operationId).
	ListRtceV1RtceTopicsWithResponse(ctx context.Context, params *ListRtceV1RtceTopicsParams, reqEditors ...RequestEditorFn) (*ListRtceV1RtceTopicsResponse, error)

	// CreateRtceV1RtceTopicWithBodyWithResponse Create a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a rtce topic.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /rtce/v1/rtce-topics (the `CreateRtceV1RtceTopic` operationId).
	CreateRtceV1RtceTopicWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateRtceV1RtceTopicResponse, error)

	// CreateRtceV1RtceTopicWithResponse Create a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a rtce topic.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /rtce/v1/rtce-topics (the `CreateRtceV1RtceTopic` operationId).
	CreateRtceV1RtceTopicWithResponse(ctx context.Context, body CreateRtceV1RtceTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateRtceV1RtceTopicResponse, error)

	// DeleteRtceV1RtceTopicWithResponse Delete a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a rtce topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /rtce/v1/rtce-topics/{topic_name} (the `DeleteRtceV1RtceTopic` operationId).
	DeleteRtceV1RtceTopicWithResponse(ctx context.Context, topicName string, params *DeleteRtceV1RtceTopicParams, reqEditors ...RequestEditorFn) (*DeleteRtceV1RtceTopicResponse, error)

	// GetRtceV1RtceTopicWithResponse Read a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a rtce topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /rtce/v1/rtce-topics/{topic_name} (the `GetRtceV1RtceTopic` operationId).
	GetRtceV1RtceTopicWithResponse(ctx context.Context, topicName string, params *GetRtceV1RtceTopicParams, reqEditors ...RequestEditorFn) (*GetRtceV1RtceTopicResponse, error)

	// UpdateRtceV1RtceTopicWithBodyWithResponse Update a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a rtce topic.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /rtce/v1/rtce-topics/{topic_name} (the `UpdateRtceV1RtceTopic` operationId).
	UpdateRtceV1RtceTopicWithBodyWithResponse(ctx context.Context, topicName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateRtceV1RtceTopicResponse, error)

	// UpdateRtceV1RtceTopicWithResponse Update a Rtce Topic
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a rtce topic.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /rtce/v1/rtce-topics/{topic_name} (the `UpdateRtceV1RtceTopic` operationId).
	UpdateRtceV1RtceTopicWithResponse(ctx context.Context, topicName string, body UpdateRtceV1RtceTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateRtceV1RtceTopicResponse, error)
}

func (r ListRtceV1RegionsResponse) GetJSON200() *RtceV1RegionList {
	return r.JSON200
}
func (r ListRtceV1RegionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListRtceV1RegionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListRtceV1RegionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListRtceV1RegionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListRtceV1RegionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListRtceV1RegionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListRtceV1RegionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListRtceV1RegionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListRtceV1RtceTopicsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListRtceV1RtceTopics200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment  interface{} `json:"environment,omitempty"`
			KafkaCluster interface{} `json:"kafka_cluster,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListRtceV1RtceTopics200JSONResponseBodyKind `json:"kind"`
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
func (r ListRtceV1RtceTopicsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListRtceV1RtceTopicsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListRtceV1RtceTopicsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListRtceV1RtceTopicsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListRtceV1RtceTopicsResponse) GetBody() []byte {
	return r.Body
}
func (r ListRtceV1RtceTopicsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListRtceV1RtceTopicsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListRtceV1RtceTopicsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateRtceV1RtceTopicResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateRtceV1RtceTopic202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateRtceV1RtceTopic202JSONResponseBodyKind `json:"kind,omitempty"`
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

	// Status The status of the Rtce Topic
	Status RtceV1RtceTopicStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateRtceV1RtceTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateRtceV1RtceTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateRtceV1RtceTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateRtceV1RtceTopicResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateRtceV1RtceTopicResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateRtceV1RtceTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateRtceV1RtceTopicResponse) GetBody() []byte {
	return r.Body
}
func (r CreateRtceV1RtceTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateRtceV1RtceTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateRtceV1RtceTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteRtceV1RtceTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteRtceV1RtceTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteRtceV1RtceTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteRtceV1RtceTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteRtceV1RtceTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteRtceV1RtceTopicResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteRtceV1RtceTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteRtceV1RtceTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteRtceV1RtceTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetRtceV1RtceTopicResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetRtceV1RtceTopic200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetRtceV1RtceTopic200JSONResponseBodyKind `json:"kind"`
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

	// Status The status of the Rtce Topic
	Status RtceV1RtceTopicStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetRtceV1RtceTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetRtceV1RtceTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetRtceV1RtceTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetRtceV1RtceTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetRtceV1RtceTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetRtceV1RtceTopicResponse) GetBody() []byte {
	return r.Body
}
func (r GetRtceV1RtceTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetRtceV1RtceTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetRtceV1RtceTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateRtceV1RtceTopic200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateRtceV1RtceTopic200JSONResponseBodyKind `json:"kind"`
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

	// Status The status of the Rtce Topic
	Status RtceV1RtceTopicStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateRtceV1RtceTopicResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateRtceV1RtceTopicResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateRtceV1RtceTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateRtceV1RtceTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateRtceV1RtceTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
