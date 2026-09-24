package org

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
	OrgV2EnvironmentApiVersionOrgv2 OrgV2EnvironmentApiVersion = "org/v2"
)

func (e OrgV2EnvironmentApiVersion) Valid() bool {
	switch e {
	case OrgV2EnvironmentApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2EnvironmentKindEnvironment OrgV2EnvironmentKind = "Environment"
)

func (e OrgV2EnvironmentKind) Valid() bool {
	switch e {
	case OrgV2EnvironmentKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	OrgV2EnvironmentListApiVersionOrgv2 OrgV2EnvironmentListApiVersion = "org/v2"
)

func (e OrgV2EnvironmentListApiVersion) Valid() bool {
	switch e {
	case OrgV2EnvironmentListApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2EnvironmentListDataApiVersionOrgv2 OrgV2EnvironmentListDataApiVersion = "org/v2"
)

func (e OrgV2EnvironmentListDataApiVersion) Valid() bool {
	switch e {
	case OrgV2EnvironmentListDataApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2EnvironmentListDataKindEnvironment OrgV2EnvironmentListDataKind = "Environment"
)

func (e OrgV2EnvironmentListDataKind) Valid() bool {
	switch e {
	case OrgV2EnvironmentListDataKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	EnvironmentList OrgV2EnvironmentListKind = "EnvironmentList"
)

func (e OrgV2EnvironmentListKind) Valid() bool {
	switch e {
	case EnvironmentList:
		return true
	default:
		return false
	}
}

const (
	OrgV2OrganizationApiVersionOrgv2 OrgV2OrganizationApiVersion = "org/v2"
)

func (e OrgV2OrganizationApiVersion) Valid() bool {
	switch e {
	case OrgV2OrganizationApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2OrganizationKindOrganization OrgV2OrganizationKind = "Organization"
)

func (e OrgV2OrganizationKind) Valid() bool {
	switch e {
	case OrgV2OrganizationKindOrganization:
		return true
	default:
		return false
	}
}

const (
	OrgV2OrganizationListApiVersionOrgv2 OrgV2OrganizationListApiVersion = "org/v2"
)

func (e OrgV2OrganizationListApiVersion) Valid() bool {
	switch e {
	case OrgV2OrganizationListApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2OrganizationListDataApiVersionOrgv2 OrgV2OrganizationListDataApiVersion = "org/v2"
)

func (e OrgV2OrganizationListDataApiVersion) Valid() bool {
	switch e {
	case OrgV2OrganizationListDataApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2OrganizationListDataKindOrganization OrgV2OrganizationListDataKind = "Organization"
)

func (e OrgV2OrganizationListDataKind) Valid() bool {
	switch e {
	case OrgV2OrganizationListDataKindOrganization:
		return true
	default:
		return false
	}
}

const (
	OrganizationList OrgV2OrganizationListKind = "OrganizationList"
)

func (e OrgV2OrganizationListKind) Valid() bool {
	switch e {
	case OrganizationList:
		return true
	default:
		return false
	}
}

const (
	OrgV2ScimTokenApiVersionOrgv2 OrgV2ScimTokenApiVersion = "org/v2"
)

func (e OrgV2ScimTokenApiVersion) Valid() bool {
	switch e {
	case OrgV2ScimTokenApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2ScimTokenKindScimToken OrgV2ScimTokenKind = "ScimToken"
)

func (e OrgV2ScimTokenKind) Valid() bool {
	switch e {
	case OrgV2ScimTokenKindScimToken:
		return true
	default:
		return false
	}
}

const (
	OrgV2ScimTokenListApiVersionOrgv2 OrgV2ScimTokenListApiVersion = "org/v2"
)

func (e OrgV2ScimTokenListApiVersion) Valid() bool {
	switch e {
	case OrgV2ScimTokenListApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2ScimTokenListDataApiVersionOrgv2 OrgV2ScimTokenListDataApiVersion = "org/v2"
)

func (e OrgV2ScimTokenListDataApiVersion) Valid() bool {
	switch e {
	case OrgV2ScimTokenListDataApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	OrgV2ScimTokenListDataKindScimToken OrgV2ScimTokenListDataKind = "ScimToken"
)

func (e OrgV2ScimTokenListDataKind) Valid() bool {
	switch e {
	case OrgV2ScimTokenListDataKindScimToken:
		return true
	default:
		return false
	}
}

const (
	ScimTokenList OrgV2ScimTokenListKind = "ScimTokenList"
)

func (e OrgV2ScimTokenListKind) Valid() bool {
	switch e {
	case ScimTokenList:
		return true
	default:
		return false
	}
}

const (
	CreateOrgV2EnvironmentJSONBodyApiVersionOrgv2 CreateOrgV2EnvironmentJSONBodyApiVersion = "org/v2"
)

func (e CreateOrgV2EnvironmentJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateOrgV2EnvironmentJSONBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	CreateOrgV2EnvironmentJSONBodyKindEnvironment CreateOrgV2EnvironmentJSONBodyKind = "Environment"
)

func (e CreateOrgV2EnvironmentJSONBodyKind) Valid() bool {
	switch e {
	case CreateOrgV2EnvironmentJSONBodyKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	CreateOrgV2Environment201JSONResponseBodyApiVersionOrgv2 CreateOrgV2Environment201JSONResponseBodyApiVersion = "org/v2"
)

func (e CreateOrgV2Environment201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateOrgV2Environment201JSONResponseBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	CreateOrgV2Environment201JSONResponseBodyKindEnvironment CreateOrgV2Environment201JSONResponseBodyKind = "Environment"
)

func (e CreateOrgV2Environment201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateOrgV2Environment201JSONResponseBodyKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	GetOrgV2Environment200JSONResponseBodyApiVersionOrgv2 GetOrgV2Environment200JSONResponseBodyApiVersion = "org/v2"
)

func (e GetOrgV2Environment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetOrgV2Environment200JSONResponseBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	GetOrgV2Environment200JSONResponseBodyKindEnvironment GetOrgV2Environment200JSONResponseBodyKind = "Environment"
)

func (e GetOrgV2Environment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetOrgV2Environment200JSONResponseBodyKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	UpdateOrgV2Environment200JSONResponseBodyApiVersionOrgv2 UpdateOrgV2Environment200JSONResponseBodyApiVersion = "org/v2"
)

func (e UpdateOrgV2Environment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateOrgV2Environment200JSONResponseBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	UpdateOrgV2Environment200JSONResponseBodyKindEnvironment UpdateOrgV2Environment200JSONResponseBodyKind = "Environment"
)

func (e UpdateOrgV2Environment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateOrgV2Environment200JSONResponseBodyKindEnvironment:
		return true
	default:
		return false
	}
}

const (
	GetOrgV2Organization200JSONResponseBodyApiVersionOrgv2 GetOrgV2Organization200JSONResponseBodyApiVersion = "org/v2"
)

func (e GetOrgV2Organization200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetOrgV2Organization200JSONResponseBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	GetOrgV2Organization200JSONResponseBodyKindOrganization GetOrgV2Organization200JSONResponseBodyKind = "Organization"
)

func (e GetOrgV2Organization200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetOrgV2Organization200JSONResponseBodyKindOrganization:
		return true
	default:
		return false
	}
}

const (
	UpdateOrgV2Organization200JSONResponseBodyApiVersionOrgv2 UpdateOrgV2Organization200JSONResponseBodyApiVersion = "org/v2"
)

func (e UpdateOrgV2Organization200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateOrgV2Organization200JSONResponseBodyApiVersionOrgv2:
		return true
	default:
		return false
	}
}

const (
	UpdateOrgV2Organization200JSONResponseBodyKindOrganization UpdateOrgV2Organization200JSONResponseBodyKind = "Organization"
)

func (e UpdateOrgV2Organization200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateOrgV2Organization200JSONResponseBodyKindOrganization:
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
type OrgV2Environment struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *OrgV2EnvironmentApiVersion `json:"api_version,omitempty"`

	// DisplayName A human-readable name for the Environment
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *OrgV2EnvironmentKind `json:"kind,omitempty"`
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

	// StreamGovernanceConfig Stream Governance configurations for the environment
	StreamGovernanceConfig *OrgV2StreamGovernanceConfig `json:"stream_governance_config,omitempty"`
}
type OrgV2EnvironmentApiVersion string
type OrgV2EnvironmentKind string
type OrgV2EnvironmentList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion OrgV2EnvironmentListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *OrgV2EnvironmentListDataApiVersion `json:"api_version,omitempty"`

		// DisplayName A human-readable name for the Environment
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *OrgV2EnvironmentListDataKind `json:"kind,omitempty"`
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

		// StreamGovernanceConfig Stream Governance configurations for the environment
		StreamGovernanceConfig *OrgV2StreamGovernanceConfig `json:"stream_governance_config,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     OrgV2EnvironmentListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type OrgV2EnvironmentListApiVersion string
type OrgV2EnvironmentListDataApiVersion string
type OrgV2EnvironmentListDataKind string
type OrgV2EnvironmentListKind string
type OrgV2Organization struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *OrgV2OrganizationApiVersion `json:"api_version,omitempty"`

	// DisplayName A human-readable name for the Organization
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// JitEnabled The flag to toggle Just-In-Time user provisioning for SSO-enabled organization. Available for early access only.
	JitEnabled *bool `json:"jit_enabled,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *OrgV2OrganizationKind `json:"kind,omitempty"`
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

	// ScimEnabled The flag to toggle SCIM user provisioning for an SSO-enabled organization.
	ScimEnabled *bool `json:"scim_enabled,omitempty"`
}
type OrgV2OrganizationApiVersion string
type OrgV2OrganizationKind string
type OrgV2OrganizationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion OrgV2OrganizationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *OrgV2OrganizationListDataApiVersion `json:"api_version,omitempty"`

		// DisplayName A human-readable name for the Organization
		DisplayName *string `json:"display_name,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// JitEnabled The flag to toggle Just-In-Time user provisioning for SSO-enabled organization. Available for early access only.
		JitEnabled *bool `json:"jit_enabled,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *OrgV2OrganizationListDataKind `json:"kind,omitempty"`
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

		// ScimEnabled The flag to toggle SCIM user provisioning for an SSO-enabled organization.
		ScimEnabled *bool `json:"scim_enabled,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     OrgV2OrganizationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type OrgV2OrganizationListApiVersion string
type OrgV2OrganizationListDataApiVersion string
type OrgV2OrganizationListDataKind string
type OrgV2OrganizationListKind string
type OrgV2ScimToken struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *OrgV2ScimTokenApiVersion `json:"api_version,omitempty"`

	// ConnectionName The SSO connection name associated with this token.
	ConnectionName *string `json:"connection_name,omitempty"`

	// CreatedAt The date and time when the token was created.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// ExpiresAt The date and time when the token expires.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *OrgV2ScimTokenKind `json:"kind,omitempty"`
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

	// Token The SCIM bearer token. Only provided in create responses, not in `list`.
	Token *string `json:"token,omitempty"`
}
type OrgV2ScimTokenApiVersion string
type OrgV2ScimTokenKind string
type OrgV2ScimTokenList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion OrgV2ScimTokenListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *OrgV2ScimTokenListDataApiVersion `json:"api_version,omitempty"`

		// ConnectionName The SSO connection name associated with this token.
		ConnectionName string `json:"connection_name"`

		// CreatedAt The date and time when the token was created.
		CreatedAt time.Time `json:"created_at"`

		// ExpiresAt The date and time when the token expires.
		ExpiresAt time.Time `json:"expires_at"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *OrgV2ScimTokenListDataKind `json:"kind,omitempty"`
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

		// Token The SCIM bearer token. Only provided in create responses, not in `list`.
		Token *string `json:"token,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     OrgV2ScimTokenListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type OrgV2ScimTokenListApiVersion string
type OrgV2ScimTokenListDataApiVersion string
type OrgV2ScimTokenListDataKind string
type OrgV2ScimTokenListKind string
type OrgV2StreamGovernanceConfig struct {
	// Package Stream Governance Package. Supported values are ESSENTIALS and ADVANCED.
	// Package comparison can be found
	// [here](https://docs.confluent.io/cloud/current/stream-governance/packages.html#features-by-package-type).
	Package string `json:"package"`
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

	// ListOrgV2Environments List of Environments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all environments.
	//
	// Corresponds with GET /org/v2/environments (the `ListOrgV2Environments` operationId).
	ListOrgV2Environments(ctx context.Context, params *ListOrgV2EnvironmentsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrgV2EnvironmentWithBody Create an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an environment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /org/v2/environments (the `CreateOrgV2Environment` operationId).
	CreateOrgV2EnvironmentWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrgV2Environment Create an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an environment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /org/v2/environments (the `CreateOrgV2Environment` operationId).
	CreateOrgV2Environment(ctx context.Context, body CreateOrgV2EnvironmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteOrgV2Environment Delete an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an environment.
	//
	// If successful, this request will also recursively delete all of the environment's associated resources,
	// including all Kafka clusters, connectors, etc.
	//
	// Corresponds with DELETE /org/v2/environments/{id} (the `DeleteOrgV2Environment` operationId).
	DeleteOrgV2Environment(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetOrgV2Environment Read an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an environment.
	//
	// Corresponds with GET /org/v2/environments/{id} (the `GetOrgV2Environment` operationId).
	GetOrgV2Environment(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateOrgV2EnvironmentWithBody Update an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an environment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /org/v2/environments/{id} (the `UpdateOrgV2Environment` operationId).
	UpdateOrgV2EnvironmentWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateOrgV2Environment Update an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an environment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /org/v2/environments/{id} (the `UpdateOrgV2Environment` operationId).
	UpdateOrgV2Environment(ctx context.Context, id string, body UpdateOrgV2EnvironmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListOrgV2Organizations List of Organizations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all organizations.
	//
	// Corresponds with GET /org/v2/organizations (the `ListOrgV2Organizations` operationId).
	ListOrgV2Organizations(ctx context.Context, params *ListOrgV2OrganizationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetOrgV2Organization Read an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an organization.
	//
	// Corresponds with GET /org/v2/organizations/{id} (the `GetOrgV2Organization` operationId).
	GetOrgV2Organization(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateOrgV2OrganizationWithBody Update an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an organization.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /org/v2/organizations/{id} (the `UpdateOrgV2Organization` operationId).
	UpdateOrgV2OrganizationWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateOrgV2Organization Update an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an organization.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /org/v2/organizations/{id} (the `UpdateOrgV2Organization` operationId).
	UpdateOrgV2Organization(ctx context.Context, id string, body UpdateOrgV2OrganizationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListOrgV2ScimTokens List of Scim Tokens
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all scim tokens.
	//
	// Corresponds with GET /org/v2/scim-tokens (the `ListOrgV2ScimTokens` operationId).
	ListOrgV2ScimTokens(ctx context.Context, params *ListOrgV2ScimTokensParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrgV2ScimTokenWithBody Create a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a scim token.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /org/v2/scim-tokens (the `CreateOrgV2ScimToken` operationId).
	CreateOrgV2ScimTokenWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrgV2ScimToken Create a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a scim token.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /org/v2/scim-tokens (the `CreateOrgV2ScimToken` operationId).
	CreateOrgV2ScimToken(ctx context.Context, body CreateOrgV2ScimTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteOrgV2ScimToken Delete a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a scim token.
	//
	// Corresponds with DELETE /org/v2/scim-tokens/{id} (the `DeleteOrgV2ScimToken` operationId).
	DeleteOrgV2ScimToken(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListOrgV2EnvironmentsWithResponse List of Environments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all environments.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /org/v2/environments (the `ListOrgV2Environments` operationId).
	ListOrgV2EnvironmentsWithResponse(ctx context.Context, params *ListOrgV2EnvironmentsParams, reqEditors ...RequestEditorFn) (*ListOrgV2EnvironmentsResponse, error)

	// CreateOrgV2EnvironmentWithBodyWithResponse Create an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an environment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /org/v2/environments (the `CreateOrgV2Environment` operationId).
	CreateOrgV2EnvironmentWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateOrgV2EnvironmentResponse, error)

	// CreateOrgV2EnvironmentWithResponse Create an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an environment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /org/v2/environments (the `CreateOrgV2Environment` operationId).
	CreateOrgV2EnvironmentWithResponse(ctx context.Context, body CreateOrgV2EnvironmentJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateOrgV2EnvironmentResponse, error)

	// DeleteOrgV2EnvironmentWithResponse Delete an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an environment.
	//
	// If successful, this request will also recursively delete all of the environment's associated resources,
	// including all Kafka clusters, connectors, etc.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /org/v2/environments/{id} (the `DeleteOrgV2Environment` operationId).
	DeleteOrgV2EnvironmentWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteOrgV2EnvironmentResponse, error)

	// GetOrgV2EnvironmentWithResponse Read an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an environment.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /org/v2/environments/{id} (the `GetOrgV2Environment` operationId).
	GetOrgV2EnvironmentWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetOrgV2EnvironmentResponse, error)

	// UpdateOrgV2EnvironmentWithBodyWithResponse Update an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an environment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /org/v2/environments/{id} (the `UpdateOrgV2Environment` operationId).
	UpdateOrgV2EnvironmentWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateOrgV2EnvironmentResponse, error)

	// UpdateOrgV2EnvironmentWithResponse Update an Environment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an environment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /org/v2/environments/{id} (the `UpdateOrgV2Environment` operationId).
	UpdateOrgV2EnvironmentWithResponse(ctx context.Context, id string, body UpdateOrgV2EnvironmentJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateOrgV2EnvironmentResponse, error)

	// ListOrgV2OrganizationsWithResponse List of Organizations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all organizations.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /org/v2/organizations (the `ListOrgV2Organizations` operationId).
	ListOrgV2OrganizationsWithResponse(ctx context.Context, params *ListOrgV2OrganizationsParams, reqEditors ...RequestEditorFn) (*ListOrgV2OrganizationsResponse, error)

	// GetOrgV2OrganizationWithResponse Read an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an organization.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /org/v2/organizations/{id} (the `GetOrgV2Organization` operationId).
	GetOrgV2OrganizationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetOrgV2OrganizationResponse, error)

	// UpdateOrgV2OrganizationWithBodyWithResponse Update an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an organization.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /org/v2/organizations/{id} (the `UpdateOrgV2Organization` operationId).
	UpdateOrgV2OrganizationWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateOrgV2OrganizationResponse, error)

	// UpdateOrgV2OrganizationWithResponse Update an Organization
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an organization.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /org/v2/organizations/{id} (the `UpdateOrgV2Organization` operationId).
	UpdateOrgV2OrganizationWithResponse(ctx context.Context, id string, body UpdateOrgV2OrganizationJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateOrgV2OrganizationResponse, error)

	// ListOrgV2ScimTokensWithResponse List of Scim Tokens
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all scim tokens.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /org/v2/scim-tokens (the `ListOrgV2ScimTokens` operationId).
	ListOrgV2ScimTokensWithResponse(ctx context.Context, params *ListOrgV2ScimTokensParams, reqEditors ...RequestEditorFn) (*ListOrgV2ScimTokensResponse, error)

	// CreateOrgV2ScimTokenWithBodyWithResponse Create a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a scim token.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /org/v2/scim-tokens (the `CreateOrgV2ScimToken` operationId).
	CreateOrgV2ScimTokenWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateOrgV2ScimTokenResponse, error)

	// CreateOrgV2ScimTokenWithResponse Create a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a scim token.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /org/v2/scim-tokens (the `CreateOrgV2ScimToken` operationId).
	CreateOrgV2ScimTokenWithResponse(ctx context.Context, body CreateOrgV2ScimTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateOrgV2ScimTokenResponse, error)

	// DeleteOrgV2ScimTokenWithResponse Delete a Scim Token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a scim token.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /org/v2/scim-tokens/{id} (the `DeleteOrgV2ScimToken` operationId).
	DeleteOrgV2ScimTokenWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteOrgV2ScimTokenResponse, error)
}

func (r ListOrgV2EnvironmentsResponse) GetJSON200() *OrgV2EnvironmentList {
	return r.JSON200
}
func (r ListOrgV2EnvironmentsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListOrgV2EnvironmentsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListOrgV2EnvironmentsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListOrgV2EnvironmentsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListOrgV2EnvironmentsResponse) GetBody() []byte {
	return r.Body
}
func (r ListOrgV2EnvironmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListOrgV2EnvironmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListOrgV2EnvironmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateOrgV2EnvironmentResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateOrgV2Environment201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// DisplayName A human-readable name for the Environment
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateOrgV2Environment201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// StreamGovernanceConfig Stream Governance configurations for the environment
	StreamGovernanceConfig *OrgV2StreamGovernanceConfig `json:"stream_governance_config,omitempty"`
} {
	return r.JSON201
}
func (r CreateOrgV2EnvironmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateOrgV2EnvironmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateOrgV2EnvironmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateOrgV2EnvironmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateOrgV2EnvironmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateOrgV2EnvironmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateOrgV2EnvironmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateOrgV2EnvironmentResponse) GetBody() []byte {
	return r.Body
}
func (r CreateOrgV2EnvironmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateOrgV2EnvironmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateOrgV2EnvironmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r DeleteOrgV2EnvironmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteOrgV2EnvironmentResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteOrgV2EnvironmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteOrgV2EnvironmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteOrgV2EnvironmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetOrgV2EnvironmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetOrgV2Environment200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName A human-readable name for the Environment
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetOrgV2Environment200JSONResponseBodyKind `json:"kind"`
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

	// StreamGovernanceConfig Stream Governance configurations for the environment
	StreamGovernanceConfig *OrgV2StreamGovernanceConfig `json:"stream_governance_config,omitempty"`
} {
	return r.JSON200
}
func (r GetOrgV2EnvironmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetOrgV2EnvironmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetOrgV2EnvironmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetOrgV2EnvironmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetOrgV2EnvironmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetOrgV2EnvironmentResponse) GetBody() []byte {
	return r.Body
}
func (r GetOrgV2EnvironmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetOrgV2EnvironmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetOrgV2EnvironmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateOrgV2Environment200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName A human-readable name for the Environment
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateOrgV2Environment200JSONResponseBodyKind `json:"kind"`
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

	// StreamGovernanceConfig Stream Governance configurations for the environment
	StreamGovernanceConfig *OrgV2StreamGovernanceConfig `json:"stream_governance_config,omitempty"`
} {
	return r.JSON200
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateOrgV2EnvironmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateOrgV2EnvironmentResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateOrgV2EnvironmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateOrgV2EnvironmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateOrgV2EnvironmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListOrgV2OrganizationsResponse) GetJSON200() *OrgV2OrganizationList {
	return r.JSON200
}
func (r ListOrgV2OrganizationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListOrgV2OrganizationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListOrgV2OrganizationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListOrgV2OrganizationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListOrgV2OrganizationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListOrgV2OrganizationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListOrgV2OrganizationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListOrgV2OrganizationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetOrgV2OrganizationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetOrgV2Organization200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName A human-readable name for the Organization
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// JitEnabled The flag to toggle Just-In-Time user provisioning for SSO-enabled organization. Available for early access only.
	JitEnabled *bool `json:"jit_enabled,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetOrgV2Organization200JSONResponseBodyKind `json:"kind"`
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

	// ScimEnabled The flag to toggle SCIM user provisioning for an SSO-enabled organization.
	ScimEnabled *bool `json:"scim_enabled,omitempty"`
} {
	return r.JSON200
}
func (r GetOrgV2OrganizationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetOrgV2OrganizationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetOrgV2OrganizationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetOrgV2OrganizationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetOrgV2OrganizationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetOrgV2OrganizationResponse) GetBody() []byte {
	return r.Body
}
func (r GetOrgV2OrganizationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetOrgV2OrganizationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetOrgV2OrganizationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateOrgV2OrganizationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateOrgV2Organization200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName A human-readable name for the Organization
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// JitEnabled The flag to toggle Just-In-Time user provisioning for SSO-enabled organization. Available for early access only.
	JitEnabled *bool `json:"jit_enabled,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateOrgV2Organization200JSONResponseBodyKind `json:"kind"`
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

	// ScimEnabled The flag to toggle SCIM user provisioning for an SSO-enabled organization.
	ScimEnabled *bool `json:"scim_enabled,omitempty"`
} {
	return r.JSON200
}
func (r UpdateOrgV2OrganizationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateOrgV2OrganizationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateOrgV2OrganizationResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateOrgV2OrganizationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateOrgV2OrganizationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateOrgV2OrganizationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateOrgV2OrganizationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateOrgV2OrganizationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateOrgV2OrganizationResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateOrgV2OrganizationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateOrgV2OrganizationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateOrgV2OrganizationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListOrgV2ScimTokensResponse) GetJSON200() *OrgV2ScimTokenList {
	return r.JSON200
}
func (r ListOrgV2ScimTokensResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListOrgV2ScimTokensResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListOrgV2ScimTokensResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListOrgV2ScimTokensResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListOrgV2ScimTokensResponse) GetBody() []byte {
	return r.Body
}
func (r ListOrgV2ScimTokensResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListOrgV2ScimTokensResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListOrgV2ScimTokensResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateOrgV2ScimTokenResponse) GetJSON201() *OrgV2ScimToken {
	return r.JSON201
}
func (r CreateOrgV2ScimTokenResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateOrgV2ScimTokenResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateOrgV2ScimTokenResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateOrgV2ScimTokenResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateOrgV2ScimTokenResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateOrgV2ScimTokenResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateOrgV2ScimTokenResponse) GetBody() []byte {
	return r.Body
}
func (r CreateOrgV2ScimTokenResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateOrgV2ScimTokenResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateOrgV2ScimTokenResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteOrgV2ScimTokenResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteOrgV2ScimTokenResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteOrgV2ScimTokenResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteOrgV2ScimTokenResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteOrgV2ScimTokenResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteOrgV2ScimTokenResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteOrgV2ScimTokenResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteOrgV2ScimTokenResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteOrgV2ScimTokenResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
