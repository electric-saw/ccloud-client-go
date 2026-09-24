package cam

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	CamV1ConnectArtifactApiVersionCamv1 CamV1ConnectArtifactApiVersion = "cam/v1"
)

func (e CamV1ConnectArtifactApiVersion) Valid() bool {
	switch e {
	case CamV1ConnectArtifactApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CamV1ConnectArtifactKindConnectArtifact CamV1ConnectArtifactKind = "ConnectArtifact"
)

func (e CamV1ConnectArtifactKind) Valid() bool {
	switch e {
	case CamV1ConnectArtifactKindConnectArtifact:
		return true
	default:
		return false
	}
}

const (
	CamV1ConnectArtifactListApiVersionCamv1 CamV1ConnectArtifactListApiVersion = "cam/v1"
)

func (e CamV1ConnectArtifactListApiVersion) Valid() bool {
	switch e {
	case CamV1ConnectArtifactListApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CamV1ConnectArtifactListDataApiVersionCamv1 CamV1ConnectArtifactListDataApiVersion = "cam/v1"
)

func (e CamV1ConnectArtifactListDataApiVersion) Valid() bool {
	switch e {
	case CamV1ConnectArtifactListDataApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CamV1ConnectArtifactListDataKindConnectArtifact CamV1ConnectArtifactListDataKind = "ConnectArtifact"
)

func (e CamV1ConnectArtifactListDataKind) Valid() bool {
	switch e {
	case CamV1ConnectArtifactListDataKindConnectArtifact:
		return true
	default:
		return false
	}
}

const (
	ConnectArtifactList CamV1ConnectArtifactListKind = "ConnectArtifactList"
)

func (e CamV1ConnectArtifactListKind) Valid() bool {
	switch e {
	case ConnectArtifactList:
		return true
	default:
		return false
	}
}

const (
	CamV1PresignedUrlApiVersionCamv1 CamV1PresignedUrlApiVersion = "cam/v1"
)

func (e CamV1PresignedUrlApiVersion) Valid() bool {
	switch e {
	case CamV1PresignedUrlApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CamV1PresignedUrlKindPresignedUrl CamV1PresignedUrlKind = "PresignedUrl"
)

func (e CamV1PresignedUrlKind) Valid() bool {
	switch e {
	case CamV1PresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	CamV1PresignedUrlRequestApiVersionCamv1 CamV1PresignedUrlRequestApiVersion = "cam/v1"
)

func (e CamV1PresignedUrlRequestApiVersion) Valid() bool {
	switch e {
	case CamV1PresignedUrlRequestApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CamV1PresignedUrlRequestKindPresignedUrlRequest CamV1PresignedUrlRequestKind = "PresignedUrlRequest"
)

func (e CamV1PresignedUrlRequestKind) Valid() bool {
	switch e {
	case CamV1PresignedUrlRequestKindPresignedUrlRequest:
		return true
	default:
		return false
	}
}

const (
	CreateCamV1ConnectArtifactJSONBodyApiVersionCamv1 CreateCamV1ConnectArtifactJSONBodyApiVersion = "cam/v1"
)

func (e CreateCamV1ConnectArtifactJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCamV1ConnectArtifactJSONBodyApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CreateCamV1ConnectArtifactJSONBodyKindConnectArtifact CreateCamV1ConnectArtifactJSONBodyKind = "ConnectArtifact"
)

func (e CreateCamV1ConnectArtifactJSONBodyKind) Valid() bool {
	switch e {
	case CreateCamV1ConnectArtifactJSONBodyKindConnectArtifact:
		return true
	default:
		return false
	}
}

const (
	CreateCamV1ConnectArtifact202JSONResponseBodyApiVersionCamv1 CreateCamV1ConnectArtifact202JSONResponseBodyApiVersion = "cam/v1"
)

func (e CreateCamV1ConnectArtifact202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCamV1ConnectArtifact202JSONResponseBodyApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	CreateCamV1ConnectArtifact202JSONResponseBodyKindConnectArtifact CreateCamV1ConnectArtifact202JSONResponseBodyKind = "ConnectArtifact"
)

func (e CreateCamV1ConnectArtifact202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCamV1ConnectArtifact202JSONResponseBodyKindConnectArtifact:
		return true
	default:
		return false
	}
}

const (
	GetCamV1ConnectArtifact200JSONResponseBodyApiVersionCamv1 GetCamV1ConnectArtifact200JSONResponseBodyApiVersion = "cam/v1"
)

func (e GetCamV1ConnectArtifact200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCamV1ConnectArtifact200JSONResponseBodyApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	GetCamV1ConnectArtifact200JSONResponseBodyKindConnectArtifact GetCamV1ConnectArtifact200JSONResponseBodyKind = "ConnectArtifact"
)

func (e GetCamV1ConnectArtifact200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCamV1ConnectArtifact200JSONResponseBodyKindConnectArtifact:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersionCamv1 PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersion = "cam/v1"
)

func (e PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlCamV1PresignedUrlJSONBodyKindPresignedUrlRequest PresignedUploadUrlCamV1PresignedUrlJSONBodyKind = "PresignedUrlRequest"
)

func (e PresignedUploadUrlCamV1PresignedUrlJSONBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlCamV1PresignedUrlJSONBodyKindPresignedUrlRequest:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersionCamv1 PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion = "cam/v1"
)

func (e PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersionCamv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKindPresignedUrl PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind = "PresignedUrl"
)

func (e PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKindPresignedUrl:
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
type CamV1ConnectArtifact struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CamV1ConnectArtifactApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CamV1ConnectArtifactKind `json:"kind,omitempty"`
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
	Spec *CamV1ConnectArtifactSpec `json:"spec,omitempty"`

	// Status The status of the Connect Artifact
	Status *CamV1ConnectArtifactStatus `json:"status,omitempty"`
}
type CamV1ConnectArtifactApiVersion string
type CamV1ConnectArtifactKind string
type CamV1ConnectArtifactList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CamV1ConnectArtifactListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CamV1ConnectArtifactListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CamV1ConnectArtifactListDataKind `json:"kind,omitempty"`
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
		Spec CamV1ConnectArtifactSpec `json:"spec"`

		// Status The status of the Connect Artifact
		Status CamV1ConnectArtifactStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CamV1ConnectArtifactListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CamV1ConnectArtifactListApiVersion string
type CamV1ConnectArtifactListDataApiVersion string
type CamV1ConnectArtifactListDataKind string
type CamV1ConnectArtifactListKind string
type CamV1ConnectArtifactSpec struct {
	// Cloud Cloud provider where the Connect Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Connect Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Connect Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Connect Artifact archive per cloud, environment scope.
	DisplayName string `json:"display_name"`

	// Environment Environment the Connect Artifact belongs to.
	Environment string `json:"environment"`

	// Plugins List of classes present in the Connect Artifact uploaded
	Plugins *[]CamV1Plugins `json:"plugins,omitempty"`

	// UploadSource Upload source of the Connect Artifact.
	UploadSource *CamV1ConnectArtifactSpec_UploadSource `json:"upload_source,omitempty"`

	// Usages List of resources using this Connect artifact, identified by CRN and connector type.
	Usages *[]CamV1Usages `json:"usages,omitempty"`
}
type CamV1ConnectArtifactSpec_UploadSource struct {
	union json.RawMessage
}
type CamV1ConnectArtifactStatus struct {
	// Phase Specifies the current processing state of a CloudConnectArtifact.
	Phase string `json:"phase"`
}
type CamV1Plugins struct {
	// Class Java class or alias for the artifact as provided by developer.
	Class *string `json:"class,omitempty"`
}
type CamV1PresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CamV1PresignedUrlApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Connect Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Connect Artifact archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Connect Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CamV1PresignedUrlKind `json:"kind,omitempty"`

	// UploadFormData Upload form data of the Connect Artifact. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Connect Artifact archive.
	UploadUrl *string `json:"upload_url,omitempty"`
}
type CamV1PresignedUrlApiVersion string
type CamV1PresignedUrlKind string
type CamV1PresignedUrlRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CamV1PresignedUrlRequestApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Connect Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Connect Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Connect Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CamV1PresignedUrlRequestKind `json:"kind,omitempty"`
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
type CamV1PresignedUrlRequestApiVersion string
type CamV1PresignedUrlRequestKind string
type CamV1UploadSourcePresignedUrl struct {
	// Location Location of the Connect Artifact source.
	Location string `json:"location"`

	// UploadId Upload ID returned by the `/presigned-upload-url` API. This field returns an empty string in all responses.
	UploadId string `json:"upload_id"`
}
type CamV1Usages struct {
	// ConnectorCrn The Confluent Resource Name (CRN) of the resource that is using the artifact.
	// This is typically the CRN of a specific connector.
	ConnectorCrn string `json:"connector_crn"`

	// ConnectorType The type of the connector using the artifact, indicating if it's a source or a sink.
	ConnectorType string `json:"connector_type"`
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
type PresignedUploadUrlCamV1PresignedUrlJSONBody struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Connect Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Connect Artifact.
	ContentFormat string `json:"content_format"`

	// Environment The Environment the uploaded Connect Artifact belongs to.
	Environment string `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *PresignedUploadUrlCamV1PresignedUrlJSONBodyKind `json:"kind,omitempty"`
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
type PresignedUploadUrlCamV1PresignedUrlJSONBodyApiVersion string
type PresignedUploadUrlCamV1PresignedUrlJSONBodyKind string
type PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion string
type PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind string
type PresignedUploadUrlCamV1PresignedUrlJSONRequestBody PresignedUploadUrlCamV1PresignedUrlJSONBody

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
func (t CamV1ConnectArtifactSpec_UploadSource) AsCamV1UploadSourcePresignedUrl() (CamV1UploadSourcePresignedUrl, error) {
	var body CamV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CamV1ConnectArtifactSpec_UploadSource) FromCamV1UploadSourcePresignedUrl(v CamV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *CamV1ConnectArtifactSpec_UploadSource) MergeCamV1UploadSourcePresignedUrl(v CamV1UploadSourcePresignedUrl) error {
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
func (t CamV1ConnectArtifactSpec_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CamV1ConnectArtifactSpec_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsCamV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CamV1ConnectArtifactSpec_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CamV1ConnectArtifactSpec_UploadSource) UnmarshalJSON(b []byte) error {
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

	// ListCamV1ConnectArtifacts List of Connect Artifacts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all connect artifacts.
	//
	// Corresponds with GET /cam/v1/connect-artifacts (the `ListCamV1ConnectArtifacts` operationId).
	ListCamV1ConnectArtifacts(ctx context.Context, params *ListCamV1ConnectArtifactsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCamV1ConnectArtifactWithBody Create a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect artifact.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cam/v1/connect-artifacts (the `CreateCamV1ConnectArtifact` operationId).
	CreateCamV1ConnectArtifactWithBody(ctx context.Context, params *CreateCamV1ConnectArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCamV1ConnectArtifact Create a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect artifact.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cam/v1/connect-artifacts (the `CreateCamV1ConnectArtifact` operationId).
	CreateCamV1ConnectArtifact(ctx context.Context, params *CreateCamV1ConnectArtifactParams, body CreateCamV1ConnectArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCamV1ConnectArtifact Delete a Connect Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a connect artifact.
	//
	// This request fails if existing workloads are using this artifact.
	//
	// Corresponds with DELETE /cam/v1/connect-artifacts/{id} (the `DeleteCamV1ConnectArtifact` operationId).
	DeleteCamV1ConnectArtifact(ctx context.Context, id string, params *DeleteCamV1ConnectArtifactParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCamV1ConnectArtifact Read a Connect Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a connect artifact.
	//
	// Corresponds with GET /cam/v1/connect-artifacts/{id} (the `GetCamV1ConnectArtifact` operationId).
	GetCamV1ConnectArtifact(ctx context.Context, id string, params *GetCamV1ConnectArtifactParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlCamV1PresignedUrlWithBody Request a presigned upload URL for a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Connect Artifact archive.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cam/v1/presigned-upload-url (the `PresignedUploadUrlCamV1PresignedUrl` operationId).
	PresignedUploadUrlCamV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlCamV1PresignedUrl Request a presigned upload URL for a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Connect Artifact archive.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cam/v1/presigned-upload-url (the `PresignedUploadUrlCamV1PresignedUrl` operationId).
	PresignedUploadUrlCamV1PresignedUrl(ctx context.Context, body PresignedUploadUrlCamV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *Client) PresignedUploadUrlCamV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlCamV1PresignedUrlRequestWithBody(c.Server, contentType, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *Client) PresignedUploadUrlCamV1PresignedUrl(ctx context.Context, body PresignedUploadUrlCamV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlCamV1PresignedUrlRequest(c.Server, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewPresignedUploadUrlCamV1PresignedUrlRequest(server string, body PresignedUploadUrlCamV1PresignedUrlJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPresignedUploadUrlCamV1PresignedUrlRequestWithBody(server, "application/json", bodyReader)
}
func NewPresignedUploadUrlCamV1PresignedUrlRequestWithBody(server string, contentType string, body io.Reader) (*http.Request, error) {
	var err error

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/cam/v1/presigned-upload-url")
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, queryURL.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", contentType)

	return req, nil
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

	// ListCamV1ConnectArtifactsWithResponse List of Connect Artifacts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all connect artifacts.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cam/v1/connect-artifacts (the `ListCamV1ConnectArtifacts` operationId).
	ListCamV1ConnectArtifactsWithResponse(ctx context.Context, params *ListCamV1ConnectArtifactsParams, reqEditors ...RequestEditorFn) (*ListCamV1ConnectArtifactsResponse, error)

	// CreateCamV1ConnectArtifactWithBodyWithResponse Create a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect artifact.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cam/v1/connect-artifacts (the `CreateCamV1ConnectArtifact` operationId).
	CreateCamV1ConnectArtifactWithBodyWithResponse(ctx context.Context, params *CreateCamV1ConnectArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCamV1ConnectArtifactResponse, error)

	// CreateCamV1ConnectArtifactWithResponse Create a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a connect artifact.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cam/v1/connect-artifacts (the `CreateCamV1ConnectArtifact` operationId).
	CreateCamV1ConnectArtifactWithResponse(ctx context.Context, params *CreateCamV1ConnectArtifactParams, body CreateCamV1ConnectArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCamV1ConnectArtifactResponse, error)

	// DeleteCamV1ConnectArtifactWithResponse Delete a Connect Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a connect artifact.
	//
	// This request fails if existing workloads are using this artifact.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /cam/v1/connect-artifacts/{id} (the `DeleteCamV1ConnectArtifact` operationId).
	DeleteCamV1ConnectArtifactWithResponse(ctx context.Context, id string, params *DeleteCamV1ConnectArtifactParams, reqEditors ...RequestEditorFn) (*DeleteCamV1ConnectArtifactResponse, error)

	// GetCamV1ConnectArtifactWithResponse Read a Connect Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a connect artifact.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cam/v1/connect-artifacts/{id} (the `GetCamV1ConnectArtifact` operationId).
	GetCamV1ConnectArtifactWithResponse(ctx context.Context, id string, params *GetCamV1ConnectArtifactParams, reqEditors ...RequestEditorFn) (*GetCamV1ConnectArtifactResponse, error)

	// PresignedUploadUrlCamV1PresignedUrlWithBodyWithResponse Request a presigned upload URL for a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Connect Artifact archive.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cam/v1/presigned-upload-url (the `PresignedUploadUrlCamV1PresignedUrl` operationId).
	PresignedUploadUrlCamV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlCamV1PresignedUrlResponse, error)

	// PresignedUploadUrlCamV1PresignedUrlWithResponse Request a presigned upload URL for a new Connect Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Connect Artifact archive.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cam/v1/presigned-upload-url (the `PresignedUploadUrlCamV1PresignedUrl` operationId).
	PresignedUploadUrlCamV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlCamV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlCamV1PresignedUrlResponse, error)
}

func (r ListCamV1ConnectArtifactsResponse) GetJSON200() *CamV1ConnectArtifactList {
	return r.JSON200
}
func (r ListCamV1ConnectArtifactsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCamV1ConnectArtifactsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCamV1ConnectArtifactsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCamV1ConnectArtifactsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCamV1ConnectArtifactsResponse) GetBody() []byte {
	return r.Body
}
func (r ListCamV1ConnectArtifactsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCamV1ConnectArtifactsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCamV1ConnectArtifactsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCamV1ConnectArtifact202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateCamV1ConnectArtifact202JSONResponseBodyKind `json:"kind,omitempty"`
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
	Spec CamV1ConnectArtifactSpec `json:"spec"`

	// Status The status of the Connect Artifact
	Status CamV1ConnectArtifactStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCamV1ConnectArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCamV1ConnectArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCamV1ConnectArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCamV1ConnectArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCamV1ConnectArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCamV1ConnectArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCamV1ConnectArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCamV1ConnectArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCamV1ConnectArtifactResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCamV1ConnectArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCamV1ConnectArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCamV1ConnectArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCamV1ConnectArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCamV1ConnectArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCamV1ConnectArtifactResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCamV1ConnectArtifact200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCamV1ConnectArtifact200JSONResponseBodyKind `json:"kind"`
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
	Spec CamV1ConnectArtifactSpec `json:"spec"`

	// Status The status of the Connect Artifact
	Status CamV1ConnectArtifactStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetCamV1ConnectArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCamV1ConnectArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCamV1ConnectArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCamV1ConnectArtifactResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCamV1ConnectArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCamV1ConnectArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r GetCamV1ConnectArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCamV1ConnectArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCamV1ConnectArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type PresignedUploadUrlCamV1PresignedUrlResponse200Headers struct {
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse400Headers struct {
	XRequestId *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse403Headers struct {
	XRequestId *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse404Headers struct {
	XRequestId *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse500Headers struct {
	XRequestId *string
}
type PresignedUploadUrlCamV1PresignedUrlResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON200 the response for an HTTP 200 `application/json` response
	JSON200 *struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

		// Cloud Cloud provider where the Connect Artifact archive is uploaded.
		Cloud *string `json:"cloud,omitempty"`

		// ContentFormat Content format of the Connect Artifact archive.
		ContentFormat *string `json:"content_format,omitempty"`

		// Environment The Environment the uploaded Connect Artifact belongs to.
		Environment *string `json:"environment,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

		// UploadFormData Upload form data of the Connect Artifact. All values should be strings.
		UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

		// UploadId Unique identifier of this upload.
		UploadId *string `json:"upload_id,omitempty"`

		// UploadUrl Upload URL for the Connect Artifact archive.
		UploadUrl *string `json:"upload_url,omitempty"`
	}
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *UnauthenticatedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *UnauthorizedError
	// JSON404 the response for an HTTP 404 `application/json` response
	JSON404 *NotFoundError
	// JSON500 the response for an HTTP 500 `application/json` response
	JSON500 *DefaultSystemError
	// Headers200 the parsed response headers for an HTTP 200 response
	Headers200 *PresignedUploadUrlCamV1PresignedUrlResponse200Headers
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *PresignedUploadUrlCamV1PresignedUrlResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *PresignedUploadUrlCamV1PresignedUrlResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *PresignedUploadUrlCamV1PresignedUrlResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *PresignedUploadUrlCamV1PresignedUrlResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *PresignedUploadUrlCamV1PresignedUrlResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *PresignedUploadUrlCamV1PresignedUrlResponse500Headers
}

func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Connect Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Connect Artifact archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Connect Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

	// UploadFormData Upload form data of the Connect Artifact. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Connect Artifact archive.
	UploadUrl *string `json:"upload_url,omitempty"`
} {
	return r.JSON200
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) GetBody() []byte {
	return r.Body
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PresignedUploadUrlCamV1PresignedUrlResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) PresignedUploadUrlCamV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlCamV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlCamV1PresignedUrlWithBody(ctx, contentType, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlCamV1PresignedUrlResponse(rsp)
}
func (c *ClientWithResponses) PresignedUploadUrlCamV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlCamV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlCamV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlCamV1PresignedUrl(ctx, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlCamV1PresignedUrlResponse(rsp)
}
func ParsePresignedUploadUrlCamV1PresignedUrlResponse(rsp *http.Response) (*PresignedUploadUrlCamV1PresignedUrlResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &PresignedUploadUrlCamV1PresignedUrlResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 200:
		var dest struct {
			// ApiVersion APIVersion defines the schema version of this representation of a resource.
			ApiVersion PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

			// Cloud Cloud provider where the Connect Artifact archive is uploaded.
			Cloud *string `json:"cloud,omitempty"`

			// ContentFormat Content format of the Connect Artifact archive.
			ContentFormat *string `json:"content_format,omitempty"`

			// Environment The Environment the uploaded Connect Artifact belongs to.
			Environment *string `json:"environment,omitempty"`

			// Kind Kind defines the object this REST resource represents.
			Kind PresignedUploadUrlCamV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

			// UploadFormData Upload form data of the Connect Artifact. All values should be strings.
			UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

			// UploadId Unique identifier of this upload.
			UploadId *string `json:"upload_id,omitempty"`

			// UploadUrl Upload URL for the Connect Artifact archive.
			UploadUrl *string `json:"upload_url,omitempty"`
		}
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON200 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest UnauthenticatedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 404:
		var dest NotFoundError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON404 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 500:
		var dest DefaultSystemError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 200:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse200Headers
		if values := rsp.Header.Values("X-RateLimit-Limit"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Limit", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitLimit = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Remaining"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Remaining", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitRemaining = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Reset"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Reset", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitReset = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers200 = &headers
	case rsp.StatusCode == 400:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse401Headers
		if values := rsp.Header.Values("WWW-Authenticate"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "WWW-Authenticate", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.WWWAuthenticate = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers401 = &headers
	case rsp.StatusCode == 403:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse429Headers
		if values := rsp.Header.Values("Retry-After"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "Retry-After", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.RetryAfter = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Limit"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Limit", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitLimit = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Remaining"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Remaining", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitRemaining = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Reset"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Reset", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitReset = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers429 = &headers
	case rsp.StatusCode == 500:
		var headers PresignedUploadUrlCamV1PresignedUrlResponse500Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers500 = &headers
	}

	return response, nil
}
