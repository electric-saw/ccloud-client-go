package srcm

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
	SrcmV2ClusterApiVersionSrcmv2 SrcmV2ClusterApiVersion = "srcm/v2"
)

func (e SrcmV2ClusterApiVersion) Valid() bool {
	switch e {
	case SrcmV2ClusterApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2ClusterKindCluster SrcmV2ClusterKind = "Cluster"
)

func (e SrcmV2ClusterKind) Valid() bool {
	switch e {
	case SrcmV2ClusterKindCluster:
		return true
	default:
		return false
	}
}

const (
	SrcmV2ClusterListApiVersionSrcmv2 SrcmV2ClusterListApiVersion = "srcm/v2"
)

func (e SrcmV2ClusterListApiVersion) Valid() bool {
	switch e {
	case SrcmV2ClusterListApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2ClusterListDataApiVersionSrcmv2 SrcmV2ClusterListDataApiVersion = "srcm/v2"
)

func (e SrcmV2ClusterListDataApiVersion) Valid() bool {
	switch e {
	case SrcmV2ClusterListDataApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2ClusterListDataKindCluster SrcmV2ClusterListDataKind = "Cluster"
)

func (e SrcmV2ClusterListDataKind) Valid() bool {
	switch e {
	case SrcmV2ClusterListDataKindCluster:
		return true
	default:
		return false
	}
}

const (
	SrcmV2ClusterListKindClusterList SrcmV2ClusterListKind = "ClusterList"
)

func (e SrcmV2ClusterListKind) Valid() bool {
	switch e {
	case SrcmV2ClusterListKindClusterList:
		return true
	default:
		return false
	}
}

const (
	SrcmV2RegionApiVersionSrcmv2 SrcmV2RegionApiVersion = "srcm/v2"
)

func (e SrcmV2RegionApiVersion) Valid() bool {
	switch e {
	case SrcmV2RegionApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2RegionKindRegion SrcmV2RegionKind = "Region"
)

func (e SrcmV2RegionKind) Valid() bool {
	switch e {
	case SrcmV2RegionKindRegion:
		return true
	default:
		return false
	}
}

const (
	SrcmV2RegionListApiVersionSrcmv2 SrcmV2RegionListApiVersion = "srcm/v2"
)

func (e SrcmV2RegionListApiVersion) Valid() bool {
	switch e {
	case SrcmV2RegionListApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2RegionListDataApiVersionSrcmv2 SrcmV2RegionListDataApiVersion = "srcm/v2"
)

func (e SrcmV2RegionListDataApiVersion) Valid() bool {
	switch e {
	case SrcmV2RegionListDataApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	SrcmV2RegionListDataKindRegion SrcmV2RegionListDataKind = "Region"
)

func (e SrcmV2RegionListDataKind) Valid() bool {
	switch e {
	case SrcmV2RegionListDataKindRegion:
		return true
	default:
		return false
	}
}

const (
	RegionList SrcmV2RegionListKind = "RegionList"
)

func (e SrcmV2RegionListKind) Valid() bool {
	switch e {
	case RegionList:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterApiVersionSrcmv3 SrcmV3ClusterApiVersion = "srcm/v3"
)

func (e SrcmV3ClusterApiVersion) Valid() bool {
	switch e {
	case SrcmV3ClusterApiVersionSrcmv3:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterKindCluster SrcmV3ClusterKind = "Cluster"
)

func (e SrcmV3ClusterKind) Valid() bool {
	switch e {
	case SrcmV3ClusterKindCluster:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterListApiVersionSrcmv3 SrcmV3ClusterListApiVersion = "srcm/v3"
)

func (e SrcmV3ClusterListApiVersion) Valid() bool {
	switch e {
	case SrcmV3ClusterListApiVersionSrcmv3:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterListDataApiVersionSrcmv3 SrcmV3ClusterListDataApiVersion = "srcm/v3"
)

func (e SrcmV3ClusterListDataApiVersion) Valid() bool {
	switch e {
	case SrcmV3ClusterListDataApiVersionSrcmv3:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterListDataKindCluster SrcmV3ClusterListDataKind = "Cluster"
)

func (e SrcmV3ClusterListDataKind) Valid() bool {
	switch e {
	case SrcmV3ClusterListDataKindCluster:
		return true
	default:
		return false
	}
}

const (
	SrcmV3ClusterListKindClusterList SrcmV3ClusterListKind = "ClusterList"
)

func (e SrcmV3ClusterListKind) Valid() bool {
	switch e {
	case SrcmV3ClusterListKindClusterList:
		return true
	default:
		return false
	}
}

const (
	ListSrcmV2Clusters200JSONResponseBodyApiVersionSrcmv2 ListSrcmV2Clusters200JSONResponseBodyApiVersion = "srcm/v2"
)

func (e ListSrcmV2Clusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListSrcmV2Clusters200JSONResponseBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	ListSrcmV2Clusters200JSONResponseBodyKindClusterList ListSrcmV2Clusters200JSONResponseBodyKind = "ClusterList"
)

func (e ListSrcmV2Clusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListSrcmV2Clusters200JSONResponseBodyKindClusterList:
		return true
	default:
		return false
	}
}

const (
	CreateSrcmV2ClusterJSONBodyApiVersionSrcmv2 CreateSrcmV2ClusterJSONBodyApiVersion = "srcm/v2"
)

func (e CreateSrcmV2ClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSrcmV2ClusterJSONBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	CreateSrcmV2ClusterJSONBodyKindCluster CreateSrcmV2ClusterJSONBodyKind = "Cluster"
)

func (e CreateSrcmV2ClusterJSONBodyKind) Valid() bool {
	switch e {
	case CreateSrcmV2ClusterJSONBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	CreateSrcmV2Cluster202JSONResponseBodyApiVersionSrcmv2 CreateSrcmV2Cluster202JSONResponseBodyApiVersion = "srcm/v2"
)

func (e CreateSrcmV2Cluster202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSrcmV2Cluster202JSONResponseBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	CreateSrcmV2Cluster202JSONResponseBodyKindCluster CreateSrcmV2Cluster202JSONResponseBodyKind = "Cluster"
)

func (e CreateSrcmV2Cluster202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSrcmV2Cluster202JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV2Cluster200JSONResponseBodyApiVersionSrcmv2 GetSrcmV2Cluster200JSONResponseBodyApiVersion = "srcm/v2"
)

func (e GetSrcmV2Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSrcmV2Cluster200JSONResponseBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV2Cluster200JSONResponseBodyKindCluster GetSrcmV2Cluster200JSONResponseBodyKind = "Cluster"
)

func (e GetSrcmV2Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSrcmV2Cluster200JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	UpdateSrcmV2ClusterJSONBodyApiVersionSrcmv2 UpdateSrcmV2ClusterJSONBodyApiVersion = "srcm/v2"
)

func (e UpdateSrcmV2ClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSrcmV2ClusterJSONBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	UpdateSrcmV2ClusterJSONBodyKindCluster UpdateSrcmV2ClusterJSONBodyKind = "Cluster"
)

func (e UpdateSrcmV2ClusterJSONBodyKind) Valid() bool {
	switch e {
	case UpdateSrcmV2ClusterJSONBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	UpdateSrcmV2Cluster200JSONResponseBodyApiVersionSrcmv2 UpdateSrcmV2Cluster200JSONResponseBodyApiVersion = "srcm/v2"
)

func (e UpdateSrcmV2Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSrcmV2Cluster200JSONResponseBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	UpdateSrcmV2Cluster200JSONResponseBodyKindCluster UpdateSrcmV2Cluster200JSONResponseBodyKind = "Cluster"
)

func (e UpdateSrcmV2Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateSrcmV2Cluster200JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV2Region200JSONResponseBodyApiVersionSrcmv2 GetSrcmV2Region200JSONResponseBodyApiVersion = "srcm/v2"
)

func (e GetSrcmV2Region200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSrcmV2Region200JSONResponseBodyApiVersionSrcmv2:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV2Region200JSONResponseBodyKindRegion GetSrcmV2Region200JSONResponseBodyKind = "Region"
)

func (e GetSrcmV2Region200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSrcmV2Region200JSONResponseBodyKindRegion:
		return true
	default:
		return false
	}
}

const (
	ListSrcmV3Clusters200JSONResponseBodyApiVersionSrcmv3 ListSrcmV3Clusters200JSONResponseBodyApiVersion = "srcm/v3"
)

func (e ListSrcmV3Clusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListSrcmV3Clusters200JSONResponseBodyApiVersionSrcmv3:
		return true
	default:
		return false
	}
}

const (
	ListSrcmV3Clusters200JSONResponseBodyKindClusterList ListSrcmV3Clusters200JSONResponseBodyKind = "ClusterList"
)

func (e ListSrcmV3Clusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListSrcmV3Clusters200JSONResponseBodyKindClusterList:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV3Cluster200JSONResponseBodyApiVersionSrcmv3 GetSrcmV3Cluster200JSONResponseBodyApiVersion = "srcm/v3"
)

func (e GetSrcmV3Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSrcmV3Cluster200JSONResponseBodyApiVersionSrcmv3:
		return true
	default:
		return false
	}
}

const (
	GetSrcmV3Cluster200JSONResponseBodyKindCluster GetSrcmV3Cluster200JSONResponseBodyKind = "Cluster"
)

func (e GetSrcmV3Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSrcmV3Cluster200JSONResponseBodyKindCluster:
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
type SrcmV2Cluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SrcmV2ClusterApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SrcmV2ClusterKind `json:"kind,omitempty"`
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
	Spec *SrcmV2ClusterSpec `json:"spec,omitempty"`

	// Status The status of the Cluster
	Status *SrcmV2ClusterStatus `json:"status,omitempty"`
}
type SrcmV2ClusterApiVersion string
type SrcmV2ClusterKind string
type SrcmV2ClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SrcmV2ClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *SrcmV2ClusterListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *SrcmV2ClusterListDataKind `json:"kind,omitempty"`
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
		Status SrcmV2ClusterStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SrcmV2ClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SrcmV2ClusterListApiVersion string
type SrcmV2ClusterListDataApiVersion string
type SrcmV2ClusterListDataKind string
type SrcmV2ClusterListKind string
type SrcmV2ClusterSpec struct {
	// DisplayName The cluster name.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// HttpEndpoint The cluster HTTP request URL.
	HttpEndpoint *string `json:"http_endpoint,omitempty"`

	// Package The billing package.
	//
	// Note: Clusters can be upgraded from ESSENTIALS to ADVANCED, but cannot be
	// downgraded from ADVANCED to ESSENTIALS.
	Package *string `json:"package,omitempty"`

	// Region The region to which this belongs.
	Region *GlobalObjectReference `json:"region,omitempty"`
}
type SrcmV2ClusterStatus struct {
	// Phase The lifecyle phase of the cluster:
	//
	//   PROVISIONED:  cluster is provisioned;
	//
	//   PROVISIONING:  cluster provisioning is in progress;
	//
	//   FAILED:  provisioning failed
	//
	// Note: Schema Registry Cluster Management is handled through the org/v2 Environments API as of srcm/v3.
	Phase string `json:"phase"`
}
type SrcmV2Region struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SrcmV2RegionApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SrcmV2RegionKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Region
	Spec *SrcmV2RegionSpec `json:"spec,omitempty"`
}
type SrcmV2RegionApiVersion string
type SrcmV2RegionKind string
type SrcmV2RegionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SrcmV2RegionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *SrcmV2RegionListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *SrcmV2RegionListDataKind `json:"kind,omitempty"`
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
	Kind     SrcmV2RegionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SrcmV2RegionListApiVersion string
type SrcmV2RegionListDataApiVersion string
type SrcmV2RegionListDataKind string
type SrcmV2RegionListKind string
type SrcmV2RegionSpec struct {
	// Cloud The cloud service provider that hosts the region.
	Cloud *string `json:"cloud,omitempty"`

	// DisplayName The display name.
	DisplayName *string `json:"display_name,omitempty"`

	// Packages List of Stream Governance packages allowing placement in this region.
	Packages *[]string `json:"packages,omitempty"`

	// RegionName The region name.
	RegionName *string `json:"region_name,omitempty"`
}
type SrcmV3Cluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SrcmV3ClusterApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SrcmV3ClusterKind `json:"kind,omitempty"`
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
	Spec *SrcmV3ClusterSpec `json:"spec,omitempty"`

	// Status The status of the Cluster
	Status *SrcmV3ClusterStatus `json:"status,omitempty"`
}
type SrcmV3ClusterApiVersion string
type SrcmV3ClusterKind string
type SrcmV3ClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SrcmV3ClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *SrcmV3ClusterListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *SrcmV3ClusterListDataKind `json:"kind,omitempty"`
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
		Status SrcmV3ClusterStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SrcmV3ClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SrcmV3ClusterListApiVersion string
type SrcmV3ClusterListDataApiVersion string
type SrcmV3ClusterListDataKind string
type SrcmV3ClusterListKind string
type SrcmV3ClusterSpec struct {
	// CatalogHttpEndpoint The cluster's catalog HTTP request URL.
	CatalogHttpEndpoint *string `json:"catalog_http_endpoint,omitempty"`

	// Cloud The cloud service provider in which the cluster is running.
	Cloud *string `json:"cloud,omitempty"`

	// DisplayName The cluster name.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// HttpEndpoint The cluster HTTP request URL.
	HttpEndpoint *string `json:"http_endpoint,omitempty"`

	// Package The billing package.
	//
	// Note: Clusters can be upgraded from ESSENTIALS to ADVANCED, but cannot be
	// downgraded from ADVANCED to ESSENTIALS.
	Package *string `json:"package,omitempty"`

	// PrivateHttpEndpoint The cluster's private HTTP request URL.
	//
	// DEPRECATED - Please use the `private_networking_config.regional_endpoints` attribute instead,
	// which supersedes the `private_http_endpoint` attribute.
	PrivateHttpEndpoint *string `json:"private_http_endpoint,omitempty"`

	// PrivateNetworkingConfig Available HTTP request URLs for private connectivity.
	PrivateNetworkingConfig *struct {
		// RegionalEndpoints A map of region identifiers to their corresponding private HTTP request URL.
		RegionalEndpoints *map[string]string `json:"regional_endpoints,omitempty"`
	} `json:"private_networking_config,omitempty"`

	// Region The cloud service provider region where the cluster is running.
	Region *string `json:"region,omitempty"`
}
type SrcmV3ClusterStatus struct {
	// Phase The lifecyle phase of the cluster:
	//
	//   PROVISIONED:  cluster is provisioned;
	//
	//   PROVISIONING:  cluster provisioning is in progress;
	//
	//   FAILED:  provisioning failed
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

	// ListSrcmV2Clusters List of Clusters
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Corresponds with GET /srcm/v2/clusters (the `ListSrcmV2Clusters` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	listSrcmV2Clusters(ctx context.Context, params *ListSrcmV2ClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSrcmV2ClusterWithBody Create a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /srcm/v2/clusters (the `CreateSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	createSrcmV2ClusterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSrcmV2Cluster Create a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /srcm/v2/clusters (the `CreateSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	createSrcmV2Cluster(ctx context.Context, body CreateSrcmV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSrcmV2Cluster Delete a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a cluster.
	//
	// Corresponds with DELETE /srcm/v2/clusters/{id} (the `DeleteSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	deleteSrcmV2Cluster(ctx context.Context, id string, params *DeleteSrcmV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSrcmV2Cluster Read a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Corresponds with GET /srcm/v2/clusters/{id} (the `GetSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	getSrcmV2Cluster(ctx context.Context, id string, params *GetSrcmV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSrcmV2ClusterWithBody Update a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /srcm/v2/clusters/{id} (the `UpdateSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	updateSrcmV2ClusterWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSrcmV2Cluster Update a Cluster
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /srcm/v2/clusters/{id} (the `UpdateSrcmV2Cluster` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	updateSrcmV2Cluster(ctx context.Context, id string, body UpdateSrcmV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSrcmV2Regions List of Regions
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all regions.
	//
	// Corresponds with GET /srcm/v2/regions (the `ListSrcmV2Regions` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	listSrcmV2Regions(ctx context.Context, params *ListSrcmV2RegionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSrcmV2Region Read a Region
	//
	// [![Deprecated](https://img.shields.io/badge/Lifecycle%20Stage-Deprecated-%23ff005c)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a region.
	//
	// Corresponds with GET /srcm/v2/regions/{id} (the `GetSrcmV2Region` operationId).
	//
	// Deprecated: this operation has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	getSrcmV2Region(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSrcmV3Clusters List of Clusters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Corresponds with GET /srcm/v3/clusters (the `ListSrcmV3Clusters` operationId).
	listSrcmV3Clusters(ctx context.Context, params *ListSrcmV3ClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSrcmV3Cluster Read a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Corresponds with GET /srcm/v3/clusters/{id} (the `GetSrcmV3Cluster` operationId).
	getSrcmV3Cluster(ctx context.Context, id string, params *GetSrcmV3ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)
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
func (r ListSrcmV2ClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListSrcmV2Clusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Region      interface{} `json:"region,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListSrcmV2Clusters200JSONResponseBodyKind `json:"kind"`
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
func (r ListSrcmV2ClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSrcmV2ClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSrcmV2ClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSrcmV2ClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSrcmV2ClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListSrcmV2ClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSrcmV2ClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSrcmV2ClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSrcmV2ClusterResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateSrcmV2Cluster202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateSrcmV2Cluster202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Region      interface{} `json:"region,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status SrcmV2ClusterStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateSrcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSrcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSrcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSrcmV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSrcmV2ClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSrcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSrcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSrcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSrcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSrcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSrcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSrcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSrcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSrcmV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSrcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSrcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSrcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSrcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSrcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSrcmV2ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSrcmV2Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSrcmV2Cluster200JSONResponseBodyKind `json:"kind"`
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
		Region      interface{} `json:"region,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status SrcmV2ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetSrcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSrcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSrcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSrcmV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSrcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSrcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetSrcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSrcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSrcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateSrcmV2ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateSrcmV2Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateSrcmV2Cluster200JSONResponseBodyKind `json:"kind"`
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
		Region      interface{} `json:"region,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status SrcmV2ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateSrcmV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateSrcmV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateSrcmV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateSrcmV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateSrcmV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateSrcmV2ClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateSrcmV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateSrcmV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateSrcmV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateSrcmV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateSrcmV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSrcmV2RegionsResponse) GetJSON200() *SrcmV2RegionList {
	return r.JSON200
}
func (r ListSrcmV2RegionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSrcmV2RegionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSrcmV2RegionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSrcmV2RegionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSrcmV2RegionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSrcmV2RegionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSrcmV2RegionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSrcmV2RegionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSrcmV2RegionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSrcmV2Region200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSrcmV2Region200JSONResponseBodyKind `json:"kind"`
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
	Spec map[string]interface{} `json:"spec"`
} {
	return r.JSON200
}
func (r GetSrcmV2RegionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSrcmV2RegionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSrcmV2RegionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSrcmV2RegionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSrcmV2RegionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSrcmV2RegionResponse) GetBody() []byte {
	return r.Body
}
func (r GetSrcmV2RegionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSrcmV2RegionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSrcmV2RegionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSrcmV3ClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListSrcmV3Clusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListSrcmV3Clusters200JSONResponseBodyKind `json:"kind"`
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
func (r ListSrcmV3ClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSrcmV3ClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSrcmV3ClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSrcmV3ClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSrcmV3ClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListSrcmV3ClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSrcmV3ClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSrcmV3ClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSrcmV3ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSrcmV3Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSrcmV3Cluster200JSONResponseBodyKind `json:"kind"`
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

	// Status The status of the Cluster
	Status SrcmV3ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetSrcmV3ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSrcmV3ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSrcmV3ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSrcmV3ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSrcmV3ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSrcmV3ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetSrcmV3ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSrcmV3ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSrcmV3ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
