package pim

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
	openapi_types "github.com/oapi-codegen/runtime/types"
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
	PimV1IntegrationApiVersionPimv1 PimV1IntegrationApiVersion = "pim/v1"
)

func (e PimV1IntegrationApiVersion) Valid() bool {
	switch e {
	case PimV1IntegrationApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	PimV1IntegrationKindIntegration PimV1IntegrationKind = "Integration"
)

func (e PimV1IntegrationKind) Valid() bool {
	switch e {
	case PimV1IntegrationKindIntegration:
		return true
	default:
		return false
	}
}

const (
	PimV1IntegrationListApiVersionPimv1 PimV1IntegrationListApiVersion = "pim/v1"
)

func (e PimV1IntegrationListApiVersion) Valid() bool {
	switch e {
	case PimV1IntegrationListApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	PimV1IntegrationListDataApiVersionPimv1 PimV1IntegrationListDataApiVersion = "pim/v1"
)

func (e PimV1IntegrationListDataApiVersion) Valid() bool {
	switch e {
	case PimV1IntegrationListDataApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	PimV1IntegrationListDataKindIntegration PimV1IntegrationListDataKind = "Integration"
)

func (e PimV1IntegrationListDataKind) Valid() bool {
	switch e {
	case PimV1IntegrationListDataKindIntegration:
		return true
	default:
		return false
	}
}

const (
	PimV1IntegrationListKindIntegrationList PimV1IntegrationListKind = "IntegrationList"
)

func (e PimV1IntegrationListKind) Valid() bool {
	switch e {
	case PimV1IntegrationListKindIntegrationList:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationApiVersionPimv2 PimV2IntegrationApiVersion = "pim/v2"
)

func (e PimV2IntegrationApiVersion) Valid() bool {
	switch e {
	case PimV2IntegrationApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationKindIntegration PimV2IntegrationKind = "Integration"
)

func (e PimV2IntegrationKind) Valid() bool {
	switch e {
	case PimV2IntegrationKindIntegration:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationListApiVersionPimv2 PimV2IntegrationListApiVersion = "pim/v2"
)

func (e PimV2IntegrationListApiVersion) Valid() bool {
	switch e {
	case PimV2IntegrationListApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationListDataApiVersionPimv2 PimV2IntegrationListDataApiVersion = "pim/v2"
)

func (e PimV2IntegrationListDataApiVersion) Valid() bool {
	switch e {
	case PimV2IntegrationListDataApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationListDataKindIntegration PimV2IntegrationListDataKind = "Integration"
)

func (e PimV2IntegrationListDataKind) Valid() bool {
	switch e {
	case PimV2IntegrationListDataKindIntegration:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationListKindIntegrationList PimV2IntegrationListKind = "IntegrationList"
)

func (e PimV2IntegrationListKind) Valid() bool {
	switch e {
	case PimV2IntegrationListKindIntegrationList:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationValidateRequestApiVersionPimv2 PimV2IntegrationValidateRequestApiVersion = "pim/v2"
)

func (e PimV2IntegrationValidateRequestApiVersion) Valid() bool {
	switch e {
	case PimV2IntegrationValidateRequestApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	PimV2IntegrationValidateRequestKindIntegrationValidateRequest PimV2IntegrationValidateRequestKind = "IntegrationValidateRequest"
)

func (e PimV2IntegrationValidateRequestKind) Valid() bool {
	switch e {
	case PimV2IntegrationValidateRequestKindIntegrationValidateRequest:
		return true
	default:
		return false
	}
}

const (
	ListPimV1Integrations200JSONResponseBodyApiVersionPimv1 ListPimV1Integrations200JSONResponseBodyApiVersion = "pim/v1"
)

func (e ListPimV1Integrations200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListPimV1Integrations200JSONResponseBodyApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	ListPimV1Integrations200JSONResponseBodyKindIntegrationList ListPimV1Integrations200JSONResponseBodyKind = "IntegrationList"
)

func (e ListPimV1Integrations200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListPimV1Integrations200JSONResponseBodyKindIntegrationList:
		return true
	default:
		return false
	}
}

const (
	CreatePimV1IntegrationJSONBodyApiVersionPimv1 CreatePimV1IntegrationJSONBodyApiVersion = "pim/v1"
)

func (e CreatePimV1IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreatePimV1IntegrationJSONBodyApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	CreatePimV1IntegrationJSONBodyKindIntegration CreatePimV1IntegrationJSONBodyKind = "Integration"
)

func (e CreatePimV1IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case CreatePimV1IntegrationJSONBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	CreatePimV1Integration201JSONResponseBodyApiVersionPimv1 CreatePimV1Integration201JSONResponseBodyApiVersion = "pim/v1"
)

func (e CreatePimV1Integration201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreatePimV1Integration201JSONResponseBodyApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	CreatePimV1Integration201JSONResponseBodyKindIntegration CreatePimV1Integration201JSONResponseBodyKind = "Integration"
)

func (e CreatePimV1Integration201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreatePimV1Integration201JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	GetPimV1Integration200JSONResponseBodyApiVersionPimv1 GetPimV1Integration200JSONResponseBodyApiVersion = "pim/v1"
)

func (e GetPimV1Integration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetPimV1Integration200JSONResponseBodyApiVersionPimv1:
		return true
	default:
		return false
	}
}

const (
	GetPimV1Integration200JSONResponseBodyKindIntegration GetPimV1Integration200JSONResponseBodyKind = "Integration"
)

func (e GetPimV1Integration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetPimV1Integration200JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	ListPimV2Integrations200JSONResponseBodyApiVersionPimv2 ListPimV2Integrations200JSONResponseBodyApiVersion = "pim/v2"
)

func (e ListPimV2Integrations200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListPimV2Integrations200JSONResponseBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	ListPimV2Integrations200JSONResponseBodyKindIntegrationList ListPimV2Integrations200JSONResponseBodyKind = "IntegrationList"
)

func (e ListPimV2Integrations200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListPimV2Integrations200JSONResponseBodyKindIntegrationList:
		return true
	default:
		return false
	}
}

const (
	CreatePimV2IntegrationJSONBodyApiVersionPimv2 CreatePimV2IntegrationJSONBodyApiVersion = "pim/v2"
)

func (e CreatePimV2IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreatePimV2IntegrationJSONBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	CreatePimV2IntegrationJSONBodyKindIntegration CreatePimV2IntegrationJSONBodyKind = "Integration"
)

func (e CreatePimV2IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case CreatePimV2IntegrationJSONBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	CreatePimV2Integration201JSONResponseBodyApiVersionPimv2 CreatePimV2Integration201JSONResponseBodyApiVersion = "pim/v2"
)

func (e CreatePimV2Integration201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreatePimV2Integration201JSONResponseBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	CreatePimV2Integration201JSONResponseBodyKindIntegration CreatePimV2Integration201JSONResponseBodyKind = "Integration"
)

func (e CreatePimV2Integration201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreatePimV2Integration201JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	GetPimV2Integration200JSONResponseBodyApiVersionPimv2 GetPimV2Integration200JSONResponseBodyApiVersion = "pim/v2"
)

func (e GetPimV2Integration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetPimV2Integration200JSONResponseBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	GetPimV2Integration200JSONResponseBodyKindIntegration GetPimV2Integration200JSONResponseBodyKind = "Integration"
)

func (e GetPimV2Integration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetPimV2Integration200JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	UpdatePimV2IntegrationJSONBodyApiVersionPimv2 UpdatePimV2IntegrationJSONBodyApiVersion = "pim/v2"
)

func (e UpdatePimV2IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdatePimV2IntegrationJSONBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	UpdatePimV2IntegrationJSONBodyKindIntegration UpdatePimV2IntegrationJSONBodyKind = "Integration"
)

func (e UpdatePimV2IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case UpdatePimV2IntegrationJSONBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	UpdatePimV2Integration200JSONResponseBodyApiVersionPimv2 UpdatePimV2Integration200JSONResponseBodyApiVersion = "pim/v2"
)

func (e UpdatePimV2Integration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdatePimV2Integration200JSONResponseBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	UpdatePimV2Integration200JSONResponseBodyKindIntegration UpdatePimV2Integration200JSONResponseBodyKind = "Integration"
)

func (e UpdatePimV2Integration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdatePimV2Integration200JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	ValidatePimV2IntegrationJSONBodyApiVersionPimv2 ValidatePimV2IntegrationJSONBodyApiVersion = "pim/v2"
)

func (e ValidatePimV2IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case ValidatePimV2IntegrationJSONBodyApiVersionPimv2:
		return true
	default:
		return false
	}
}

const (
	ValidatePimV2IntegrationJSONBodyKindIntegrationValidateRequest ValidatePimV2IntegrationJSONBodyKind = "IntegrationValidateRequest"
)

func (e ValidatePimV2IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case ValidatePimV2IntegrationJSONBodyKindIntegrationValidateRequest:
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
type ObjectReference struct {
	// ApiVersion API group and version of the referred resource
	ApiVersion *string `json:"api_version,omitempty"`

	// Environment Environment of the referred resource, if env-scoped
	Environment *string `json:"environment,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Kind Kind of the referred resource
	Kind *string `json:"kind,omitempty"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type PimV1AwsIntegrationConfig struct {
	// CustomerIamRoleArn Amazon Resource Name (ARN) that identifies the Amazon Web Services (AWS)
	// Identity and Access Management (IAM) role that Confluent Cloud assumes when
	// it accesses resources in your AWS account.
	CustomerIamRoleArn *string `json:"customer_iam_role_arn,omitempty"`

	// ExternalId Unique external ID that Confluent Cloud uses when it assumes the IAM role
	// in your Amazon Web Services (AWS) account.
	ExternalId *openapi_types.UUID `json:"external_id,omitempty"`

	// IamRoleArn Amazon Resource Name (ARN) that identifies the Amazon Web Services (AWS)
	// Identity and Access Management (IAM) role that Confluent Cloud uses to assume
	// customer IAM role when it accesses resources in your AWS account.
	IamRoleArn *string `json:"iam_role_arn,omitempty"`

	// Kind Cloud provider specific config to which access is provided through provider integration.
	Kind string `json:"kind"`
}
type PimV1Integration struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PimV1IntegrationApiVersion `json:"api_version,omitempty"`

	// Config Cloud provider specific configs for provider integration
	Config *PimV1Integration_Config `json:"config,omitempty"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PimV1IntegrationKind `json:"kind,omitempty"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
}
type PimV1IntegrationApiVersion string
type PimV1Integration_Config struct {
	union json.RawMessage
}
type PimV1IntegrationKind string
type PimV1IntegrationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PimV1IntegrationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PimV1IntegrationListDataApiVersion `json:"api_version,omitempty"`

		// Config Cloud provider specific configs for provider integration
		Config PimV1IntegrationList_Data_Config `json:"config"`

		// DisplayName Display name of Provider Integration.
		DisplayName *string `json:"display_name,omitempty"`

		// Environment The environment to which this belongs.
		Environment GlobalObjectReference `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PimV1IntegrationListDataKind `json:"kind,omitempty"`

		// Provider Cloud provider to which access is provided through provider integration.
		Provider *string `json:"provider,omitempty"`

		// Usages List of resource crns where this integration is being used.
		Usages *[]string `json:"usages,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     PimV1IntegrationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type PimV1IntegrationListApiVersion string
type PimV1IntegrationListDataApiVersion string
type PimV1IntegrationList_Data_Config struct {
	union json.RawMessage
}
type PimV1IntegrationListDataKind string
type PimV1IntegrationListKind string
type PimV2AwsIntegrationConfig struct {
	// CustomerIamRoleArn Amazon Resource Name (ARN) that identifies the Amazon Web Services (AWS)
	// Identity and Access Management (IAM) role that Confluent Cloud assumes when
	// it accesses resources in your AWS account.
	CustomerIamRoleArn *string `json:"customer_iam_role_arn,omitempty"`

	// ExternalId Unique external ID that Confluent Cloud uses when it assumes the IAM role
	// in your Amazon Web Services (AWS) account.
	ExternalId *openapi_types.UUID `json:"external_id,omitempty"`

	// IamRoleArn Amazon Resource Name (ARN) that identifies the Amazon Web Services (AWS)
	// Identity and Access Management (IAM) role that Confluent Cloud uses to assume
	// customer IAM role when it accesses resources in your AWS account.
	IamRoleArn *string `json:"iam_role_arn,omitempty"`

	// Kind Cloud provider specific config to which access is provided through provider integration.
	Kind string `json:"kind"`
}
type PimV2AzureIntegrationConfig struct {
	// ConfluentMultiTenantAppId The ID of the Confluent Multi-Tenant App that Confluent Cloud uses to impersonate
	// customer Azure App when it accesses resources in your Azure subscription.
	ConfluentMultiTenantAppId *string `json:"confluent_multi_tenant_app_id,omitempty"`

	// CustomerAzureTenantId The ID of the customer's Azure Active Directory (Azure AD) tenant
	CustomerAzureTenantId *string `json:"customer_azure_tenant_id,omitempty"`

	// Kind Cloud provider specific config to which access is provided through provider integration.
	Kind string `json:"kind"`
}
type PimV2GcpIntegrationConfig struct {
	// CustomerGoogleServiceAccount The ID of the Google Service Account that Confluent Cloud impersonates
	// to access resources in your GCP Project.
	CustomerGoogleServiceAccount *string `json:"customer_google_service_account,omitempty"`

	// GoogleServiceAccount The ID of the Google Service Account that Confluent Cloud uses to impersonate
	// customer Google Service Account when it accesses resources in your GCP project.
	GoogleServiceAccount *string `json:"google_service_account,omitempty"`

	// Kind Cloud provider specific config to which access is provided through provider integration.
	Kind string `json:"kind"`
}
type PimV2Integration struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PimV2IntegrationApiVersion `json:"api_version,omitempty"`

	// Config Cloud provider specific configuration for the provider integration.
	// Required only when updating integrations with `DRAFT` status. Not required during creation.
	Config *PimV2Integration_Config `json:"config,omitempty"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PimV2IntegrationKind `json:"kind,omitempty"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Status Status of the provider integration.
	// - `DRAFT`: Integration exists but is not associated with customer configuration
	// - `CREATED`: Integration has been associated with customer configuration
	// - `ACTIVE`: Integration is in use by Confluent resources
	Status *string `json:"status,omitempty"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
}
type PimV2IntegrationApiVersion string
type PimV2Integration_Config struct {
	union json.RawMessage
}
type PimV2IntegrationKind string
type PimV2IntegrationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PimV2IntegrationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PimV2IntegrationListDataApiVersion `json:"api_version,omitempty"`

		// Config Cloud provider specific configuration for the provider integration.
		// Required only when updating integrations with `DRAFT` status. Not required during creation.
		Config *PimV2IntegrationList_Data_Config `json:"config,omitempty"`

		// DisplayName Display name of Provider Integration.
		DisplayName *string `json:"display_name,omitempty"`

		// Environment The environment to which this belongs.
		Environment ObjectReference `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PimV2IntegrationListDataKind `json:"kind,omitempty"`

		// Provider Cloud provider to which access is provided through provider integration.
		Provider *string `json:"provider,omitempty"`

		// Status Status of the provider integration.
		// - `DRAFT`: Integration exists but is not associated with customer configuration
		// - `CREATED`: Integration has been associated with customer configuration
		// - `ACTIVE`: Integration is in use by Confluent resources
		Status string `json:"status"`

		// Usages List of resource crns where this integration is being used.
		Usages *[]string `json:"usages,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     PimV2IntegrationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type PimV2IntegrationListApiVersion string
type PimV2IntegrationListDataApiVersion string
type PimV2IntegrationList_Data_Config struct {
	union json.RawMessage
}
type PimV2IntegrationListDataKind string
type PimV2IntegrationListKind string
type PimV2IntegrationValidateRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PimV2IntegrationValidateRequestApiVersion `json:"api_version,omitempty"`

	// Config Cloud provider specific configuration for the provider integration.
	// Required only for integrations in `DRAFT` status.
	Config *PimV2IntegrationValidateRequest_Config `json:"config,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Id The ID of the provider integration to validate.
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *PimV2IntegrationValidateRequestKind `json:"kind,omitempty"`
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
type PimV2IntegrationValidateRequestApiVersion string
type PimV2IntegrationValidateRequest_Config struct {
	union json.RawMessage
}
type PimV2IntegrationValidateRequestKind string
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
func (t PimV1Integration_Config) AsPimV1AwsIntegrationConfig() (PimV1AwsIntegrationConfig, error) {
	var body PimV1AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV1Integration_Config) FromPimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV1Integration_Config) MergePimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV1Integration_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PimV1Integration_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV1AwsIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PimV1Integration_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PimV1Integration_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PimV1IntegrationList_Data_Config) AsPimV1AwsIntegrationConfig() (PimV1AwsIntegrationConfig, error) {
	var body PimV1AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV1IntegrationList_Data_Config) FromPimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV1IntegrationList_Data_Config) MergePimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV1IntegrationList_Data_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PimV1IntegrationList_Data_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV1AwsIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PimV1IntegrationList_Data_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PimV1IntegrationList_Data_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PimV2Integration_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2Integration_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2Integration_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2Integration_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2Integration_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2Integration_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2Integration_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2Integration_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2Integration_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2Integration_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PimV2Integration_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PimV2Integration_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PimV2Integration_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PimV2IntegrationList_Data_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationList_Data_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationList_Data_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationList_Data_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationList_Data_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationList_Data_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationList_Data_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationList_Data_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationList_Data_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationList_Data_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PimV2IntegrationList_Data_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PimV2IntegrationList_Data_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PimV2IntegrationList_Data_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PimV2IntegrationValidateRequest_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationValidateRequest_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationValidateRequest_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationValidateRequest_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationValidateRequest_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationValidateRequest_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationValidateRequest_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PimV2IntegrationValidateRequest_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *PimV2IntegrationValidateRequest_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PimV2IntegrationValidateRequest_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PimV2IntegrationValidateRequest_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PimV2IntegrationValidateRequest_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PimV2IntegrationValidateRequest_Config) UnmarshalJSON(b []byte) error {
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
func (t CreatePimV1IntegrationJSONBody_Config) AsPimV1AwsIntegrationConfig() (PimV1AwsIntegrationConfig, error) {
	var body PimV1AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV1IntegrationJSONBody_Config) FromPimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV1IntegrationJSONBody_Config) MergePimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV1IntegrationJSONBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreatePimV1IntegrationJSONBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV1AwsIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreatePimV1IntegrationJSONBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreatePimV1IntegrationJSONBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreatePimV1Integration201JSONResponseBody_Config) AsPimV1AwsIntegrationConfig() (PimV1AwsIntegrationConfig, error) {
	var body PimV1AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV1Integration201JSONResponseBody_Config) FromPimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV1Integration201JSONResponseBody_Config) MergePimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV1Integration201JSONResponseBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreatePimV1Integration201JSONResponseBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV1AwsIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreatePimV1Integration201JSONResponseBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreatePimV1Integration201JSONResponseBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetPimV1Integration200JSONResponseBody_Config) AsPimV1AwsIntegrationConfig() (PimV1AwsIntegrationConfig, error) {
	var body PimV1AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetPimV1Integration200JSONResponseBody_Config) FromPimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *GetPimV1Integration200JSONResponseBody_Config) MergePimV1AwsIntegrationConfig(v PimV1AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetPimV1Integration200JSONResponseBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetPimV1Integration200JSONResponseBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV1AwsIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetPimV1Integration200JSONResponseBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetPimV1Integration200JSONResponseBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreatePimV2IntegrationJSONBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2IntegrationJSONBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2IntegrationJSONBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2IntegrationJSONBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2IntegrationJSONBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2IntegrationJSONBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2IntegrationJSONBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2IntegrationJSONBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2IntegrationJSONBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2IntegrationJSONBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreatePimV2IntegrationJSONBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreatePimV2IntegrationJSONBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreatePimV2IntegrationJSONBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreatePimV2Integration201JSONResponseBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2Integration201JSONResponseBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2Integration201JSONResponseBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreatePimV2Integration201JSONResponseBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreatePimV2Integration201JSONResponseBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreatePimV2Integration201JSONResponseBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreatePimV2Integration201JSONResponseBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetPimV2Integration200JSONResponseBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetPimV2Integration200JSONResponseBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetPimV2Integration200JSONResponseBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetPimV2Integration200JSONResponseBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetPimV2Integration200JSONResponseBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetPimV2Integration200JSONResponseBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetPimV2Integration200JSONResponseBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdatePimV2IntegrationJSONBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2IntegrationJSONBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2IntegrationJSONBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2IntegrationJSONBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdatePimV2IntegrationJSONBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdatePimV2IntegrationJSONBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdatePimV2IntegrationJSONBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdatePimV2Integration200JSONResponseBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdatePimV2Integration200JSONResponseBody_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t ValidatePimV2IntegrationJSONBody_Config) AsPimV2GcpIntegrationConfig() (PimV2GcpIntegrationConfig, error) {
	var body PimV2GcpIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) FromPimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) MergePimV2GcpIntegrationConfig(v PimV2GcpIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ValidatePimV2IntegrationJSONBody_Config) AsPimV2AzureIntegrationConfig() (PimV2AzureIntegrationConfig, error) {
	var body PimV2AzureIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) FromPimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) MergePimV2AzureIntegrationConfig(v PimV2AzureIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ValidatePimV2IntegrationJSONBody_Config) AsPimV2AwsIntegrationConfig() (PimV2AwsIntegrationConfig, error) {
	var body PimV2AwsIntegrationConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) FromPimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	t.union = b
	return err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) MergePimV2AwsIntegrationConfig(v PimV2AwsIntegrationConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIntegrationConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ValidatePimV2IntegrationJSONBody_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ValidatePimV2IntegrationJSONBody_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsIntegrationConfig":
		return t.AsPimV2AwsIntegrationConfig()
	case "AzureIntegrationConfig":
		return t.AsPimV2AzureIntegrationConfig()
	case "GcpIntegrationConfig":
		return t.AsPimV2GcpIntegrationConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ValidatePimV2IntegrationJSONBody_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ValidatePimV2IntegrationJSONBody_Config) UnmarshalJSON(b []byte) error {
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

	// ListPimV1Integrations List of Integrations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all integrations.
	//
	// If no `provider` filter is specified, returns provider integrations from all clouds.
	//
	// Corresponds with GET /pim/v1/integrations (the `ListPimV1Integrations` operationId).
	ListPimV1Integrations(ctx context.Context, params *ListPimV1IntegrationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePimV1IntegrationWithBody Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /pim/v1/integrations (the `CreatePimV1Integration` operationId).
	CreatePimV1IntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePimV1Integration Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /pim/v1/integrations (the `CreatePimV1Integration` operationId).
	CreatePimV1Integration(ctx context.Context, body CreatePimV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeletePimV1Integration Delete an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an integration.
	//
	// This request fails if existing workloads are using this CSP integration.
	//
	// Corresponds with DELETE /pim/v1/integrations/{id} (the `DeletePimV1Integration` operationId).
	DeletePimV1Integration(ctx context.Context, id string, params *DeletePimV1IntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetPimV1Integration Read an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an integration.
	//
	// Corresponds with GET /pim/v1/integrations/{id} (the `GetPimV1Integration` operationId).
	GetPimV1Integration(ctx context.Context, id string, params *GetPimV1IntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListPimV2Integrations List of Integrations
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all integrations.
	//
	// If no `provider` filter is specified, returns provider integrations from all clouds.
	//
	// Corresponds with GET /pim/v2/integrations (the `ListPimV2Integrations` operationId).
	ListPimV2Integrations(ctx context.Context, params *ListPimV2IntegrationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePimV2IntegrationWithBody Create an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /pim/v2/integrations (the `CreatePimV2Integration` operationId).
	CreatePimV2IntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePimV2Integration Create an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /pim/v2/integrations (the `CreatePimV2Integration` operationId).
	CreatePimV2Integration(ctx context.Context, body CreatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeletePimV2Integration Delete an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to delete an integration.
	//
	// This request fails if existing workloads are using this CSP integration.
	//
	// Corresponds with DELETE /pim/v2/integrations/{id} (the `DeletePimV2Integration` operationId).
	DeletePimV2Integration(ctx context.Context, id string, params *DeletePimV2IntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetPimV2Integration Read an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an integration.
	//
	// Corresponds with GET /pim/v2/integrations/{id} (the `GetPimV2Integration` operationId).
	GetPimV2Integration(ctx context.Context, id string, params *GetPimV2IntegrationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdatePimV2IntegrationWithBody Update an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update an integration.
	//
	// This request only works for integrations with `DRAFT` status.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /pim/v2/integrations/{id} (the `UpdatePimV2Integration` operationId).
	UpdatePimV2IntegrationWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdatePimV2Integration Update an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update an integration.
	//
	// This request only works for integrations with `DRAFT` status.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /pim/v2/integrations/{id} (the `UpdatePimV2Integration` operationId).
	UpdatePimV2Integration(ctx context.Context, id string, body UpdatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ValidatePimV2IntegrationWithBody Validate an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Validate the provider integration configuration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /pim/v2/integrations:validate (the `ValidatePimV2Integration` operationId).
	ValidatePimV2IntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ValidatePimV2Integration Validate an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Validate the provider integration configuration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /pim/v2/integrations:validate (the `ValidatePimV2Integration` operationId).
	ValidatePimV2Integration(ctx context.Context, body ValidatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListPimV1IntegrationsWithResponse List of Integrations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all integrations.
	//
	// If no `provider` filter is specified, returns provider integrations from all clouds.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /pim/v1/integrations (the `ListPimV1Integrations` operationId).
	ListPimV1IntegrationsWithResponse(ctx context.Context, params *ListPimV1IntegrationsParams, reqEditors ...RequestEditorFn) (*ListPimV1IntegrationsResponse, error)

	// CreatePimV1IntegrationWithBodyWithResponse Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v1/integrations (the `CreatePimV1Integration` operationId).
	CreatePimV1IntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreatePimV1IntegrationResponse, error)

	// CreatePimV1IntegrationWithResponse Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v1/integrations (the `CreatePimV1Integration` operationId).
	CreatePimV1IntegrationWithResponse(ctx context.Context, body CreatePimV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*CreatePimV1IntegrationResponse, error)

	// DeletePimV1IntegrationWithResponse Delete an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an integration.
	//
	// This request fails if existing workloads are using this CSP integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /pim/v1/integrations/{id} (the `DeletePimV1Integration` operationId).
	DeletePimV1IntegrationWithResponse(ctx context.Context, id string, params *DeletePimV1IntegrationParams, reqEditors ...RequestEditorFn) (*DeletePimV1IntegrationResponse, error)

	// GetPimV1IntegrationWithResponse Read an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /pim/v1/integrations/{id} (the `GetPimV1Integration` operationId).
	GetPimV1IntegrationWithResponse(ctx context.Context, id string, params *GetPimV1IntegrationParams, reqEditors ...RequestEditorFn) (*GetPimV1IntegrationResponse, error)

	// ListPimV2IntegrationsWithResponse List of Integrations
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all integrations.
	//
	// If no `provider` filter is specified, returns provider integrations from all clouds.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /pim/v2/integrations (the `ListPimV2Integrations` operationId).
	ListPimV2IntegrationsWithResponse(ctx context.Context, params *ListPimV2IntegrationsParams, reqEditors ...RequestEditorFn) (*ListPimV2IntegrationsResponse, error)

	// CreatePimV2IntegrationWithBodyWithResponse Create an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v2/integrations (the `CreatePimV2Integration` operationId).
	CreatePimV2IntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreatePimV2IntegrationResponse, error)

	// CreatePimV2IntegrationWithResponse Create an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v2/integrations (the `CreatePimV2Integration` operationId).
	CreatePimV2IntegrationWithResponse(ctx context.Context, body CreatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*CreatePimV2IntegrationResponse, error)

	// DeletePimV2IntegrationWithResponse Delete an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to delete an integration.
	//
	// This request fails if existing workloads are using this CSP integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /pim/v2/integrations/{id} (the `DeletePimV2Integration` operationId).
	DeletePimV2IntegrationWithResponse(ctx context.Context, id string, params *DeletePimV2IntegrationParams, reqEditors ...RequestEditorFn) (*DeletePimV2IntegrationResponse, error)

	// GetPimV2IntegrationWithResponse Read an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /pim/v2/integrations/{id} (the `GetPimV2Integration` operationId).
	GetPimV2IntegrationWithResponse(ctx context.Context, id string, params *GetPimV2IntegrationParams, reqEditors ...RequestEditorFn) (*GetPimV2IntegrationResponse, error)

	// UpdatePimV2IntegrationWithBodyWithResponse Update an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update an integration.
	//
	// This request only works for integrations with `DRAFT` status.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /pim/v2/integrations/{id} (the `UpdatePimV2Integration` operationId).
	UpdatePimV2IntegrationWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdatePimV2IntegrationResponse, error)

	// UpdatePimV2IntegrationWithResponse Update an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update an integration.
	//
	// This request only works for integrations with `DRAFT` status.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /pim/v2/integrations/{id} (the `UpdatePimV2Integration` operationId).
	UpdatePimV2IntegrationWithResponse(ctx context.Context, id string, body UpdatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdatePimV2IntegrationResponse, error)

	// ValidatePimV2IntegrationWithBodyWithResponse Validate an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Validate the provider integration configuration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v2/integrations:validate (the `ValidatePimV2Integration` operationId).
	ValidatePimV2IntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*ValidatePimV2IntegrationResponse, error)

	// ValidatePimV2IntegrationWithResponse Validate an Integration
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Provider Integration](https://img.shields.io/badge/-Request%20Access%20To%20Provider%20Integration-%23bc8540)](mailto:ccloud-api-access+pim-v2-early-access@confluent.io?subject=Request%20to%20join%20pim/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20pim/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Validate the provider integration configuration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /pim/v2/integrations:validate (the `ValidatePimV2Integration` operationId).
	ValidatePimV2IntegrationWithResponse(ctx context.Context, body ValidatePimV2IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*ValidatePimV2IntegrationResponse, error)
}

func (r ListPimV1IntegrationsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListPimV1Integrations200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment interface{} `json:"environment,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListPimV1Integrations200JSONResponseBodyKind `json:"kind"`
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
func (r ListPimV1IntegrationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListPimV1IntegrationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListPimV1IntegrationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListPimV1IntegrationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListPimV1IntegrationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListPimV1IntegrationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListPimV1IntegrationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListPimV1IntegrationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreatePimV1IntegrationResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreatePimV1Integration201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Config Cloud provider specific configs for provider integration
	Config CreatePimV1Integration201JSONResponseBody_Config `json:"config"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string     `json:"display_name,omitempty"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreatePimV1Integration201JSONResponseBodyKind `json:"kind,omitempty"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
} {
	return r.JSON201
}
func (r CreatePimV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreatePimV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreatePimV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreatePimV1IntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreatePimV1IntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreatePimV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreatePimV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r CreatePimV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreatePimV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreatePimV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeletePimV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeletePimV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeletePimV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeletePimV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeletePimV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeletePimV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r DeletePimV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeletePimV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeletePimV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetPimV1IntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetPimV1Integration200JSONResponseBodyApiVersion `json:"api_version"`

	// Config Cloud provider specific configs for provider integration
	Config GetPimV1Integration200JSONResponseBody_Config `json:"config"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string     `json:"display_name,omitempty"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetPimV1Integration200JSONResponseBodyKind `json:"kind"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
} {
	return r.JSON200
}
func (r GetPimV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetPimV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetPimV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetPimV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetPimV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetPimV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r GetPimV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetPimV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetPimV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListPimV2IntegrationsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListPimV2Integrations200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Environment interface{} `json:"environment,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListPimV2Integrations200JSONResponseBodyKind `json:"kind"`
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
func (r ListPimV2IntegrationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListPimV2IntegrationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListPimV2IntegrationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListPimV2IntegrationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListPimV2IntegrationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListPimV2IntegrationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListPimV2IntegrationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListPimV2IntegrationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreatePimV2IntegrationResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreatePimV2Integration201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Config Cloud provider specific configuration for the provider integration.
	// Required only when updating integrations with `DRAFT` status. Not required during creation.
	Config *CreatePimV2Integration201JSONResponseBody_Config `json:"config,omitempty"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string     `json:"display_name,omitempty"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreatePimV2Integration201JSONResponseBodyKind `json:"kind,omitempty"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Status Status of the provider integration.
	// - `DRAFT`: Integration exists but is not associated with customer configuration
	// - `CREATED`: Integration has been associated with customer configuration
	// - `ACTIVE`: Integration is in use by Confluent resources
	Status string `json:"status"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
} {
	return r.JSON201
}
func (r CreatePimV2IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreatePimV2IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreatePimV2IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreatePimV2IntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreatePimV2IntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreatePimV2IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreatePimV2IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r CreatePimV2IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreatePimV2IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreatePimV2IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeletePimV2IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeletePimV2IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeletePimV2IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeletePimV2IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeletePimV2IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeletePimV2IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r DeletePimV2IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeletePimV2IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeletePimV2IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetPimV2IntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetPimV2Integration200JSONResponseBodyApiVersion `json:"api_version"`

	// Config Cloud provider specific configuration for the provider integration.
	// Required only when updating integrations with `DRAFT` status. Not required during creation.
	Config *GetPimV2Integration200JSONResponseBody_Config `json:"config,omitempty"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string     `json:"display_name,omitempty"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetPimV2Integration200JSONResponseBodyKind `json:"kind"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Status Status of the provider integration.
	// - `DRAFT`: Integration exists but is not associated with customer configuration
	// - `CREATED`: Integration has been associated with customer configuration
	// - `ACTIVE`: Integration is in use by Confluent resources
	Status string `json:"status"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
} {
	return r.JSON200
}
func (r GetPimV2IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetPimV2IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetPimV2IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetPimV2IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetPimV2IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetPimV2IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r GetPimV2IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetPimV2IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetPimV2IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdatePimV2IntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdatePimV2Integration200JSONResponseBodyApiVersion `json:"api_version"`

	// Config Cloud provider specific configuration for the provider integration.
	// Required only when updating integrations with `DRAFT` status. Not required during creation.
	Config *UpdatePimV2Integration200JSONResponseBody_Config `json:"config,omitempty"`

	// DisplayName Display name of Provider Integration.
	DisplayName *string     `json:"display_name,omitempty"`
	Environment interface{} `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind UpdatePimV2Integration200JSONResponseBodyKind `json:"kind"`

	// Provider Cloud provider to which access is provided through provider integration.
	Provider *string `json:"provider,omitempty"`

	// Status Status of the provider integration.
	// - `DRAFT`: Integration exists but is not associated with customer configuration
	// - `CREATED`: Integration has been associated with customer configuration
	// - `ACTIVE`: Integration is in use by Confluent resources
	Status string `json:"status"`

	// Usages List of resource crns where this integration is being used.
	Usages *[]string `json:"usages,omitempty"`
} {
	return r.JSON200
}
func (r UpdatePimV2IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdatePimV2IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdatePimV2IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdatePimV2IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdatePimV2IntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdatePimV2IntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdatePimV2IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdatePimV2IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r UpdatePimV2IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdatePimV2IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdatePimV2IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ValidatePimV2IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ValidatePimV2IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ValidatePimV2IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ValidatePimV2IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ValidatePimV2IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ValidatePimV2IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r ValidatePimV2IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ValidatePimV2IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ValidatePimV2IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
