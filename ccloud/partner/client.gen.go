package partner

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
	PartnerLinkRequestEntitlement0ApiVersionPartnerv2 PartnerLinkRequestEntitlement0ApiVersion = "partner/v2"
)

func (e PartnerLinkRequestEntitlement0ApiVersion) Valid() bool {
	switch e {
	case PartnerLinkRequestEntitlement0ApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerLinkRequestEntitlement0KindEntitlement PartnerLinkRequestEntitlement0Kind = "Entitlement"
)

func (e PartnerLinkRequestEntitlement0Kind) Valid() bool {
	switch e {
	case PartnerLinkRequestEntitlement0KindEntitlement:
		return true
	default:
		return false
	}
}

const (
	PartnerLinkRequestOrganizationApiVersionPartnerv2 PartnerLinkRequestOrganizationApiVersion = "partner/v2"
)

func (e PartnerLinkRequestOrganizationApiVersion) Valid() bool {
	switch e {
	case PartnerLinkRequestOrganizationApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerLinkRequestOrganizationKindOrganization PartnerLinkRequestOrganizationKind = "Organization"
)

func (e PartnerLinkRequestOrganizationKind) Valid() bool {
	switch e {
	case PartnerLinkRequestOrganizationKindOrganization:
		return true
	default:
		return false
	}
}

const (
	PartnerSignupRequestEntitlement0ApiVersionPartnerv2 PartnerSignupRequestEntitlement0ApiVersion = "partner/v2"
)

func (e PartnerSignupRequestEntitlement0ApiVersion) Valid() bool {
	switch e {
	case PartnerSignupRequestEntitlement0ApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerSignupRequestEntitlement0KindEntitlement PartnerSignupRequestEntitlement0Kind = "Entitlement"
)

func (e PartnerSignupRequestEntitlement0Kind) Valid() bool {
	switch e {
	case PartnerSignupRequestEntitlement0KindEntitlement:
		return true
	default:
		return false
	}
}

const (
	PartnerSignupRequestOrganizationApiVersionPartnerv2 PartnerSignupRequestOrganizationApiVersion = "partner/v2"
)

func (e PartnerSignupRequestOrganizationApiVersion) Valid() bool {
	switch e {
	case PartnerSignupRequestOrganizationApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerSignupRequestOrganizationKindOrganization PartnerSignupRequestOrganizationKind = "Organization"
)

func (e PartnerSignupRequestOrganizationKind) Valid() bool {
	switch e {
	case PartnerSignupRequestOrganizationKindOrganization:
		return true
	default:
		return false
	}
}

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
	PartnerV2EntitlementApiVersionPartnerv2 PartnerV2EntitlementApiVersion = "partner/v2"
)

func (e PartnerV2EntitlementApiVersion) Valid() bool {
	switch e {
	case PartnerV2EntitlementApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2EntitlementKindEntitlement PartnerV2EntitlementKind = "Entitlement"
)

func (e PartnerV2EntitlementKind) Valid() bool {
	switch e {
	case PartnerV2EntitlementKindEntitlement:
		return true
	default:
		return false
	}
}

const (
	PartnerV2EntitlementListApiVersionPartnerv2 PartnerV2EntitlementListApiVersion = "partner/v2"
)

func (e PartnerV2EntitlementListApiVersion) Valid() bool {
	switch e {
	case PartnerV2EntitlementListApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2EntitlementListDataApiVersionPartnerv2 PartnerV2EntitlementListDataApiVersion = "partner/v2"
)

func (e PartnerV2EntitlementListDataApiVersion) Valid() bool {
	switch e {
	case PartnerV2EntitlementListDataApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2EntitlementListDataKindEntitlement PartnerV2EntitlementListDataKind = "Entitlement"
)

func (e PartnerV2EntitlementListDataKind) Valid() bool {
	switch e {
	case PartnerV2EntitlementListDataKindEntitlement:
		return true
	default:
		return false
	}
}

const (
	EntitlementList PartnerV2EntitlementListKind = "EntitlementList"
)

func (e PartnerV2EntitlementListKind) Valid() bool {
	switch e {
	case EntitlementList:
		return true
	default:
		return false
	}
}

const (
	PartnerV2OrganizationApiVersionPartnerv2 PartnerV2OrganizationApiVersion = "partner/v2"
)

func (e PartnerV2OrganizationApiVersion) Valid() bool {
	switch e {
	case PartnerV2OrganizationApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2OrganizationKindOrganization PartnerV2OrganizationKind = "Organization"
)

func (e PartnerV2OrganizationKind) Valid() bool {
	switch e {
	case PartnerV2OrganizationKindOrganization:
		return true
	default:
		return false
	}
}

const (
	PartnerV2OrganizationListApiVersionPartnerv2 PartnerV2OrganizationListApiVersion = "partner/v2"
)

func (e PartnerV2OrganizationListApiVersion) Valid() bool {
	switch e {
	case PartnerV2OrganizationListApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2OrganizationListDataApiVersionPartnerv2 PartnerV2OrganizationListDataApiVersion = "partner/v2"
)

func (e PartnerV2OrganizationListDataApiVersion) Valid() bool {
	switch e {
	case PartnerV2OrganizationListDataApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	PartnerV2OrganizationListDataKindOrganization PartnerV2OrganizationListDataKind = "Organization"
)

func (e PartnerV2OrganizationListDataKind) Valid() bool {
	switch e {
	case PartnerV2OrganizationListDataKindOrganization:
		return true
	default:
		return false
	}
}

const (
	OrganizationList PartnerV2OrganizationListKind = "OrganizationList"
)

func (e PartnerV2OrganizationListKind) Valid() bool {
	switch e {
	case OrganizationList:
		return true
	default:
		return false
	}
}

const (
	CreatePartnerV2EntitlementJSONBodyApiVersionPartnerv2 CreatePartnerV2EntitlementJSONBodyApiVersion = "partner/v2"
)

func (e CreatePartnerV2EntitlementJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreatePartnerV2EntitlementJSONBodyApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	CreatePartnerV2EntitlementJSONBodyKindEntitlement CreatePartnerV2EntitlementJSONBodyKind = "Entitlement"
)

func (e CreatePartnerV2EntitlementJSONBodyKind) Valid() bool {
	switch e {
	case CreatePartnerV2EntitlementJSONBodyKindEntitlement:
		return true
	default:
		return false
	}
}

const (
	CreatePartnerV2Entitlement201JSONResponseBodyApiVersionPartnerv2 CreatePartnerV2Entitlement201JSONResponseBodyApiVersion = "partner/v2"
)

func (e CreatePartnerV2Entitlement201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreatePartnerV2Entitlement201JSONResponseBodyApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	CreatePartnerV2Entitlement201JSONResponseBodyKindEntitlement CreatePartnerV2Entitlement201JSONResponseBodyKind = "Entitlement"
)

func (e CreatePartnerV2Entitlement201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreatePartnerV2Entitlement201JSONResponseBodyKindEntitlement:
		return true
	default:
		return false
	}
}

const (
	GetPartnerV2Entitlement200JSONResponseBodyApiVersionPartnerv2 GetPartnerV2Entitlement200JSONResponseBodyApiVersion = "partner/v2"
)

func (e GetPartnerV2Entitlement200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetPartnerV2Entitlement200JSONResponseBodyApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	GetPartnerV2Entitlement200JSONResponseBodyKindEntitlement GetPartnerV2Entitlement200JSONResponseBodyKind = "Entitlement"
)

func (e GetPartnerV2Entitlement200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetPartnerV2Entitlement200JSONResponseBodyKindEntitlement:
		return true
	default:
		return false
	}
}

const (
	GetPartnerV2Organization200JSONResponseBodyApiVersionPartnerv2 GetPartnerV2Organization200JSONResponseBodyApiVersion = "partner/v2"
)

func (e GetPartnerV2Organization200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetPartnerV2Organization200JSONResponseBodyApiVersionPartnerv2:
		return true
	default:
		return false
	}
}

const (
	GetPartnerV2Organization200JSONResponseBodyKindOrganization GetPartnerV2Organization200JSONResponseBodyKind = "Organization"
)

func (e GetPartnerV2Organization200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetPartnerV2Organization200JSONResponseBodyKindOrganization:
		return true
	default:
		return false
	}
}

type ActivatePartnerSignupRequest struct {
	// OrganizationId The ID of the organization
	OrganizationId string                 `json:"organization_id"`
	User           map[string]interface{} `json:"user"`
}
type AssignmentsType = string
type AzureSSOConfig struct {
	Kind string `json:"kind"`

	// TenantId The Azure AD tenant ID
	TenantId string `json:"tenant_id"`
}
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
type PartnerLinkRequest struct {
	Entitlement  PartnerLinkRequest_Entitlement `json:"entitlement"`
	Organization struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PartnerLinkRequestOrganizationApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id *string `json:"id,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PartnerLinkRequestOrganizationKind `json:"kind,omitempty"`

		// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
		Metadata *ObjectMeta `json:"metadata,omitempty"`

		// Name The name of the organization
		Name      *string                                   `json:"name,omitempty"`
		SsoConfig PartnerLinkRequest_Organization_SsoConfig `json:"sso_config"`

		// SsoUrl The login URL for the customer to access Confluent Cloud
		SsoUrl *string `json:"sso_url,omitempty"`
	} `json:"organization"`

	// Token The linking token that was generated.
	Token string `json:"token"`
}
type PartnerLinkRequestEntitlement0 struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PartnerLinkRequestEntitlement0ApiVersion `json:"api_version,omitempty"`

	// ExternalId The unique external ID of the entitlement (this should be unique to customer)
	ExternalId string `json:"external_id"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PartnerLinkRequestEntitlement0Kind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the entitlement
	Name string `json:"name"`

	// Organization The organization associated with this object.
	Organization *struct {
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
	} `json:"organization,omitempty"`

	// PlanId The plan ID the entitlement
	PlanId string `json:"plan_id"`

	// ProductId The product ID of the entitlement
	ProductId string `json:"product_id"`

	// ResourceId The resource ID of the entitlement
	ResourceId *string `json:"resource_id,omitempty"`

	// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
	// a different ID, otherwise, same as external_id)
	UsageReportingId *string `json:"usage_reporting_id,omitempty"`
}
type PartnerLinkRequestEntitlement0ApiVersion string
type PartnerLinkRequestEntitlement0Kind string
type PartnerLinkRequestEntitlement1 struct {
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
type PartnerLinkRequest_Entitlement struct {
	union json.RawMessage
}
type PartnerLinkRequestOrganizationApiVersion string
type PartnerLinkRequestOrganizationKind string
type PartnerLinkRequest_Organization_SsoConfig struct {
	union json.RawMessage
}
type PartnerSignupRequest struct {
	Entitlement  PartnerSignupRequest_Entitlement `json:"entitlement"`
	Organization struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PartnerSignupRequestOrganizationApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id *string `json:"id,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PartnerSignupRequestOrganizationKind `json:"kind,omitempty"`

		// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
		Metadata *ObjectMeta `json:"metadata,omitempty"`

		// Name The name of the organization
		Name      string                                      `json:"name"`
		SsoConfig PartnerSignupRequest_Organization_SsoConfig `json:"sso_config"`

		// SsoUrl The login URL for the customer to access Confluent Cloud
		SsoUrl *string `json:"sso_url,omitempty"`
	} `json:"organization"`
	User *map[string]interface{} `json:"user,omitempty"`
}
type PartnerSignupRequestEntitlement0 struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PartnerSignupRequestEntitlement0ApiVersion `json:"api_version,omitempty"`

	// ExternalId The unique external ID of the entitlement (this should be unique to customer)
	ExternalId string `json:"external_id"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PartnerSignupRequestEntitlement0Kind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the entitlement
	Name string `json:"name"`

	// Organization The organization associated with this object.
	Organization *struct {
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
	} `json:"organization,omitempty"`

	// PlanId The plan ID the entitlement
	PlanId string `json:"plan_id"`

	// ProductId The product ID of the entitlement
	ProductId string `json:"product_id"`

	// ResourceId The resource ID of the entitlement
	ResourceId *string `json:"resource_id,omitempty"`

	// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
	// a different ID, otherwise, same as external_id)
	UsageReportingId *string `json:"usage_reporting_id,omitempty"`
}
type PartnerSignupRequestEntitlement0ApiVersion string
type PartnerSignupRequestEntitlement0Kind string
type PartnerSignupRequestEntitlement1 struct {
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
type PartnerSignupRequest_Entitlement struct {
	union json.RawMessage
}
type PartnerSignupRequestOrganizationApiVersion string
type PartnerSignupRequestOrganizationKind string
type PartnerSignupRequest_Organization_SsoConfig struct {
	union json.RawMessage
}
type PartnerSignupResponse struct {
	// DisplayMessage The display message contains useful information which is shown on the Marketplace UI to the customers.
	DisplayMessage *string `json:"display_message,omitempty"`

	// OrganizationId The ID of the organization
	OrganizationId string `json:"organization_id"`

	// SsoUrl The login URL for the customer to access Confluent Cloud
	SsoUrl string `json:"sso_url"`
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
type PartnerV2Entitlement struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PartnerV2EntitlementApiVersion `json:"api_version,omitempty"`

	// ExternalId The unique external ID of the entitlement (this should be unique to customer)
	ExternalId *string `json:"external_id,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PartnerV2EntitlementKind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the entitlement
	Name *string `json:"name,omitempty"`

	// Organization The organization associated with this object.
	Organization *struct {
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
	} `json:"organization,omitempty"`

	// PlanId The plan ID the entitlement
	PlanId *string `json:"plan_id,omitempty"`

	// ProductId The product ID of the entitlement
	ProductId *string `json:"product_id,omitempty"`

	// ResourceId The resource ID of the entitlement
	ResourceId *string `json:"resource_id,omitempty"`

	// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
	// a different ID, otherwise, same as external_id)
	UsageReportingId *string `json:"usage_reporting_id,omitempty"`
}
type PartnerV2EntitlementApiVersion string
type PartnerV2EntitlementKind string
type PartnerV2EntitlementList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PartnerV2EntitlementListApiVersion `json:"api_version"`
	Data       []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PartnerV2EntitlementListDataApiVersion `json:"api_version,omitempty"`

		// ExternalId The unique external ID of the entitlement (this should be unique to customer)
		ExternalId string `json:"external_id"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PartnerV2EntitlementListDataKind `json:"kind,omitempty"`

		// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
		Metadata ObjectMeta `json:"metadata"`

		// Name The name of the entitlement
		Name string `json:"name"`

		// Organization The organization associated with this object.
		Organization *struct {
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
		} `json:"organization,omitempty"`

		// PlanId The plan ID the entitlement
		PlanId string `json:"plan_id"`

		// ProductId The product ID of the entitlement
		ProductId string `json:"product_id"`

		// ResourceId The resource ID of the entitlement
		ResourceId *string `json:"resource_id,omitempty"`

		// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
		// a different ID, otherwise, same as external_id)
		UsageReportingId *string `json:"usage_reporting_id,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind PartnerV2EntitlementListKind `json:"kind"`

	// Metadata ListMeta describes metadata that resource collections may have
	Metadata ListMeta `json:"metadata"`
}
type PartnerV2EntitlementListApiVersion string
type PartnerV2EntitlementListDataApiVersion string
type PartnerV2EntitlementListDataKind string
type PartnerV2EntitlementListKind string
type PartnerV2Organization struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PartnerV2OrganizationApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *PartnerV2OrganizationKind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the organization
	Name      *string                          `json:"name,omitempty"`
	SsoConfig *PartnerV2Organization_SsoConfig `json:"sso_config,omitempty"`

	// SsoUrl The login URL for the customer to access Confluent Cloud
	SsoUrl *string `json:"sso_url,omitempty"`
}
type PartnerV2OrganizationApiVersion string
type PartnerV2OrganizationKind string
type PartnerV2Organization_SsoConfig struct {
	union json.RawMessage
}
type PartnerV2OrganizationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PartnerV2OrganizationListApiVersion `json:"api_version"`
	Data       []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *PartnerV2OrganizationListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *PartnerV2OrganizationListDataKind `json:"kind,omitempty"`

		// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
		Metadata ObjectMeta `json:"metadata"`

		// Name The name of the organization
		Name      *string                                   `json:"name,omitempty"`
		SsoConfig *PartnerV2OrganizationList_Data_SsoConfig `json:"sso_config,omitempty"`

		// SsoUrl The login URL for the customer to access Confluent Cloud
		SsoUrl *string `json:"sso_url,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind PartnerV2OrganizationListKind `json:"kind"`

	// Metadata ListMeta describes metadata that resource collections may have
	Metadata ListMeta `json:"metadata"`
}
type PartnerV2OrganizationListApiVersion string
type PartnerV2OrganizationListDataApiVersion string
type PartnerV2OrganizationListDataKind string
type PartnerV2OrganizationList_Data_SsoConfig struct {
	union json.RawMessage
}
type PartnerV2OrganizationListKind string
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
type V2User = map[string]interface{}
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

func (t PartnerLinkRequest_Entitlement) AsPartnerLinkRequestEntitlement0() (PartnerLinkRequestEntitlement0, error) {
	var body PartnerLinkRequestEntitlement0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerLinkRequest_Entitlement) FromPartnerLinkRequestEntitlement0(v PartnerLinkRequestEntitlement0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PartnerLinkRequest_Entitlement) MergePartnerLinkRequestEntitlement0(v PartnerLinkRequestEntitlement0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerLinkRequest_Entitlement) AsPartnerLinkRequestEntitlement1() (PartnerLinkRequestEntitlement1, error) {
	var body PartnerLinkRequestEntitlement1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerLinkRequest_Entitlement) FromPartnerLinkRequestEntitlement1(v PartnerLinkRequestEntitlement1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PartnerLinkRequest_Entitlement) MergePartnerLinkRequestEntitlement1(v PartnerLinkRequestEntitlement1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerLinkRequest_Entitlement) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerLinkRequest_Entitlement) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PartnerLinkRequest_Organization_SsoConfig) AsAzureSSOConfig() (AzureSSOConfig, error) {
	var body AzureSSOConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerLinkRequest_Organization_SsoConfig) FromAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	t.union = b
	return err
}
func (t *PartnerLinkRequest_Organization_SsoConfig) MergeAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerLinkRequest_Organization_SsoConfig) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PartnerLinkRequest_Organization_SsoConfig) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureSSOConfig":
		return t.AsAzureSSOConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PartnerLinkRequest_Organization_SsoConfig) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerLinkRequest_Organization_SsoConfig) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PartnerSignupRequest_Entitlement) AsPartnerSignupRequestEntitlement0() (PartnerSignupRequestEntitlement0, error) {
	var body PartnerSignupRequestEntitlement0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerSignupRequest_Entitlement) FromPartnerSignupRequestEntitlement0(v PartnerSignupRequestEntitlement0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PartnerSignupRequest_Entitlement) MergePartnerSignupRequestEntitlement0(v PartnerSignupRequestEntitlement0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerSignupRequest_Entitlement) AsPartnerSignupRequestEntitlement1() (PartnerSignupRequestEntitlement1, error) {
	var body PartnerSignupRequestEntitlement1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerSignupRequest_Entitlement) FromPartnerSignupRequestEntitlement1(v PartnerSignupRequestEntitlement1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PartnerSignupRequest_Entitlement) MergePartnerSignupRequestEntitlement1(v PartnerSignupRequestEntitlement1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerSignupRequest_Entitlement) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerSignupRequest_Entitlement) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PartnerSignupRequest_Organization_SsoConfig) AsAzureSSOConfig() (AzureSSOConfig, error) {
	var body AzureSSOConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerSignupRequest_Organization_SsoConfig) FromAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	t.union = b
	return err
}
func (t *PartnerSignupRequest_Organization_SsoConfig) MergeAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerSignupRequest_Organization_SsoConfig) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PartnerSignupRequest_Organization_SsoConfig) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureSSOConfig":
		return t.AsAzureSSOConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PartnerSignupRequest_Organization_SsoConfig) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerSignupRequest_Organization_SsoConfig) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
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
func (t PartnerV2Organization_SsoConfig) AsAzureSSOConfig() (AzureSSOConfig, error) {
	var body AzureSSOConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerV2Organization_SsoConfig) FromAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	t.union = b
	return err
}
func (t *PartnerV2Organization_SsoConfig) MergeAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerV2Organization_SsoConfig) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PartnerV2Organization_SsoConfig) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureSSOConfig":
		return t.AsAzureSSOConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PartnerV2Organization_SsoConfig) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerV2Organization_SsoConfig) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PartnerV2OrganizationList_Data_SsoConfig) AsAzureSSOConfig() (AzureSSOConfig, error) {
	var body AzureSSOConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PartnerV2OrganizationList_Data_SsoConfig) FromAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	t.union = b
	return err
}
func (t *PartnerV2OrganizationList_Data_SsoConfig) MergeAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PartnerV2OrganizationList_Data_SsoConfig) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t PartnerV2OrganizationList_Data_SsoConfig) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureSSOConfig":
		return t.AsAzureSSOConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t PartnerV2OrganizationList_Data_SsoConfig) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PartnerV2OrganizationList_Data_SsoConfig) UnmarshalJSON(b []byte) error {
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
func (t GetPartnerV2Organization200JSONResponseBody_SsoConfig) AsAzureSSOConfig() (AzureSSOConfig, error) {
	var body AzureSSOConfig
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetPartnerV2Organization200JSONResponseBody_SsoConfig) FromAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	t.union = b
	return err
}
func (t *GetPartnerV2Organization200JSONResponseBody_SsoConfig) MergeAzureSSOConfig(v AzureSSOConfig) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureSSOConfig"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetPartnerV2Organization200JSONResponseBody_SsoConfig) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetPartnerV2Organization200JSONResponseBody_SsoConfig) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AzureSSOConfig":
		return t.AsAzureSSOConfig()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetPartnerV2Organization200JSONResponseBody_SsoConfig) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetPartnerV2Organization200JSONResponseBody_SsoConfig) UnmarshalJSON(b []byte) error {
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

	// ListPartnerV2Entitlements List of Entitlements
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all entitlements.
	//
	// Corresponds with GET /partner/v2/entitlements (the `ListPartnerV2Entitlements` operationId).
	ListPartnerV2Entitlements(ctx context.Context, params *ListPartnerV2EntitlementsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePartnerV2EntitlementWithBody Create an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an entitlement.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /partner/v2/entitlements (the `CreatePartnerV2Entitlement` operationId).
	CreatePartnerV2EntitlementWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreatePartnerV2Entitlement Create an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an entitlement.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /partner/v2/entitlements (the `CreatePartnerV2Entitlement` operationId).
	CreatePartnerV2Entitlement(ctx context.Context, body CreatePartnerV2EntitlementJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetPartnerV2Entitlement Read an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an entitlement.
	//
	// Corresponds with GET /partner/v2/entitlements/{id} (the `GetPartnerV2Entitlement` operationId).
	GetPartnerV2Entitlement(ctx context.Context, id string, params *GetPartnerV2EntitlementParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListPartnerV2Organizations List of Organizations
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all organizations.
	//
	// Corresponds with GET /partner/v2/organizations (the `ListPartnerV2Organizations` operationId).
	ListPartnerV2Organizations(ctx context.Context, params *ListPartnerV2OrganizationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetPartnerV2Organization Read an Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an organization.
	//
	// Corresponds with GET /partner/v2/organizations/{id} (the `GetPartnerV2Organization` operationId).
	GetPartnerV2Organization(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// SignupWithBody Signup an Organization on behalf of a Customer
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Create an organization for a customer. You must pass in either an entitlement object reference (a url to
	// a previously created entitlement) or entitlement details. If you pass in an entitlement object reference, we will link with the
	// created entitlement. If you pass in the entitlement details, we will create the entitlement with the organization
	// in a single transaction. If you pass in user details (email, given name, and family name), we will
	// create a user as well. If you do not pass in user details, you MUST call `/partner/v2/signup/activate`
	// with user details to complete signup.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /partner/v2/signup (the `Signup` operationId).
	SignupWithBody(ctx context.Context, params *SignupParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// Signup Signup an Organization on behalf of a Customer
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Create an organization for a customer. You must pass in either an entitlement object reference (a url to
	// a previously created entitlement) or entitlement details. If you pass in an entitlement object reference, we will link with the
	// created entitlement. If you pass in the entitlement details, we will create the entitlement with the organization
	// in a single transaction. If you pass in user details (email, given name, and family name), we will
	// create a user as well. If you do not pass in user details, you MUST call `/partner/v2/signup/activate`
	// with user details to complete signup.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /partner/v2/signup (the `Signup` operationId).
	Signup(ctx context.Context, params *SignupParams, body SignupJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ActivateSignupWithBody Activate an Incomplete Signup
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Creates a user in the organization previously created in `/partner/v2/signup`. This completes the signup
	// process if you did not pass in user details to `/partner/v2/signup`. Calling this endpoint if the signup
	// process has been completed will result in a `409 Conflict` error.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /partner/v2/signup/activate (the `ActivateSignup` operationId).
	ActivateSignupWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ActivateSignup Activate an Incomplete Signup
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Creates a user in the organization previously created in `/partner/v2/signup`. This completes the signup
	// process if you did not pass in user details to `/partner/v2/signup`. Calling this endpoint if the signup
	// process has been completed will result in a `409 Conflict` error.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /partner/v2/signup/activate (the `ActivateSignup` operationId).
	ActivateSignup(ctx context.Context, body ActivateSignupJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// SignupPartnerV2LinkWithBody Signup a Customer by Linking to an Existing Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Signup a customer by linking a new entitlement to an existing Confluent Cloud organization.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /partner/v2/signup/link (the `SignupPartnerV2Link` operationId).
	SignupPartnerV2LinkWithBody(ctx context.Context, params *SignupPartnerV2LinkParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// SignupPartnerV2Link Signup a Customer by Linking to an Existing Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Signup a customer by linking a new entitlement to an existing Confluent Cloud organization.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /partner/v2/signup/link (the `SignupPartnerV2Link` operationId).
	SignupPartnerV2Link(ctx context.Context, params *SignupPartnerV2LinkParams, body SignupPartnerV2LinkJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListPartnerV2EntitlementsWithResponse List of Entitlements
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all entitlements.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /partner/v2/entitlements (the `ListPartnerV2Entitlements` operationId).
	ListPartnerV2EntitlementsWithResponse(ctx context.Context, params *ListPartnerV2EntitlementsParams, reqEditors ...RequestEditorFn) (*ListPartnerV2EntitlementsResponse, error)

	// CreatePartnerV2EntitlementWithBodyWithResponse Create an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an entitlement.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/entitlements (the `CreatePartnerV2Entitlement` operationId).
	CreatePartnerV2EntitlementWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreatePartnerV2EntitlementResponse, error)

	// CreatePartnerV2EntitlementWithResponse Create an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to create an entitlement.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/entitlements (the `CreatePartnerV2Entitlement` operationId).
	CreatePartnerV2EntitlementWithResponse(ctx context.Context, body CreatePartnerV2EntitlementJSONRequestBody, reqEditors ...RequestEditorFn) (*CreatePartnerV2EntitlementResponse, error)

	// GetPartnerV2EntitlementWithResponse Read an Entitlement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an entitlement.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /partner/v2/entitlements/{id} (the `GetPartnerV2Entitlement` operationId).
	GetPartnerV2EntitlementWithResponse(ctx context.Context, id string, params *GetPartnerV2EntitlementParams, reqEditors ...RequestEditorFn) (*GetPartnerV2EntitlementResponse, error)

	// ListPartnerV2OrganizationsWithResponse List of Organizations
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all organizations.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /partner/v2/organizations (the `ListPartnerV2Organizations` operationId).
	ListPartnerV2OrganizationsWithResponse(ctx context.Context, params *ListPartnerV2OrganizationsParams, reqEditors ...RequestEditorFn) (*ListPartnerV2OrganizationsResponse, error)

	// GetPartnerV2OrganizationWithResponse Read an Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read an organization.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /partner/v2/organizations/{id} (the `GetPartnerV2Organization` operationId).
	GetPartnerV2OrganizationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetPartnerV2OrganizationResponse, error)

	// SignupWithBodyWithResponse Signup an Organization on behalf of a Customer
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Create an organization for a customer. You must pass in either an entitlement object reference (a url to
	// a previously created entitlement) or entitlement details. If you pass in an entitlement object reference, we will link with the
	// created entitlement. If you pass in the entitlement details, we will create the entitlement with the organization
	// in a single transaction. If you pass in user details (email, given name, and family name), we will
	// create a user as well. If you do not pass in user details, you MUST call `/partner/v2/signup/activate`
	// with user details to complete signup.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup (the `Signup` operationId).
	SignupWithBodyWithResponse(ctx context.Context, params *SignupParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*SignupResponse, error)

	// SignupWithResponse Signup an Organization on behalf of a Customer
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Create an organization for a customer. You must pass in either an entitlement object reference (a url to
	// a previously created entitlement) or entitlement details. If you pass in an entitlement object reference, we will link with the
	// created entitlement. If you pass in the entitlement details, we will create the entitlement with the organization
	// in a single transaction. If you pass in user details (email, given name, and family name), we will
	// create a user as well. If you do not pass in user details, you MUST call `/partner/v2/signup/activate`
	// with user details to complete signup.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup (the `Signup` operationId).
	SignupWithResponse(ctx context.Context, params *SignupParams, body SignupJSONRequestBody, reqEditors ...RequestEditorFn) (*SignupResponse, error)

	// ActivateSignupWithBodyWithResponse Activate an Incomplete Signup
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Creates a user in the organization previously created in `/partner/v2/signup`. This completes the signup
	// process if you did not pass in user details to `/partner/v2/signup`. Calling this endpoint if the signup
	// process has been completed will result in a `409 Conflict` error.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup/activate (the `ActivateSignup` operationId).
	ActivateSignupWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*ActivateSignupResponse, error)

	// ActivateSignupWithResponse Activate an Incomplete Signup
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Creates a user in the organization previously created in `/partner/v2/signup`. This completes the signup
	// process if you did not pass in user details to `/partner/v2/signup`. Calling this endpoint if the signup
	// process has been completed will result in a `409 Conflict` error.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup/activate (the `ActivateSignup` operationId).
	ActivateSignupWithResponse(ctx context.Context, body ActivateSignupJSONRequestBody, reqEditors ...RequestEditorFn) (*ActivateSignupResponse, error)

	// SignupPartnerV2LinkWithBodyWithResponse Signup a Customer by Linking to an Existing Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Signup a customer by linking a new entitlement to an existing Confluent Cloud organization.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup/link (the `SignupPartnerV2Link` operationId).
	SignupPartnerV2LinkWithBodyWithResponse(ctx context.Context, params *SignupPartnerV2LinkParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*SignupPartnerV2LinkResponse, error)

	// SignupPartnerV2LinkWithResponse Signup a Customer by Linking to an Existing Organization
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To Partner v2](https://img.shields.io/badge/-Request%20Access%20To%20Partner%20v2-%23bc8540)](mailto:ccloud-api-access+partner-v2-early-access@confluent.io?subject=Request%20to%20join%20partner/v2%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20partner/v2%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Signup a customer by linking a new entitlement to an existing Confluent Cloud organization.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /partner/v2/signup/link (the `SignupPartnerV2Link` operationId).
	SignupPartnerV2LinkWithResponse(ctx context.Context, params *SignupPartnerV2LinkParams, body SignupPartnerV2LinkJSONRequestBody, reqEditors ...RequestEditorFn) (*SignupPartnerV2LinkResponse, error)
}

func (r ListPartnerV2EntitlementsResponse) GetJSON200() *PartnerV2EntitlementList {
	return r.JSON200
}
func (r ListPartnerV2EntitlementsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListPartnerV2EntitlementsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListPartnerV2EntitlementsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListPartnerV2EntitlementsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListPartnerV2EntitlementsResponse) GetBody() []byte {
	return r.Body
}
func (r ListPartnerV2EntitlementsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListPartnerV2EntitlementsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListPartnerV2EntitlementsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreatePartnerV2EntitlementResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreatePartnerV2Entitlement201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// ExternalId The unique external ID of the entitlement (this should be unique to customer)
	ExternalId string `json:"external_id"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CreatePartnerV2Entitlement201JSONResponseBodyKind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the entitlement
	Name string `json:"name"`

	// Organization The organization associated with this object.
	Organization *struct {
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
	} `json:"organization,omitempty"`

	// PlanId The plan ID the entitlement
	PlanId string `json:"plan_id"`

	// ProductId The product ID of the entitlement
	ProductId string `json:"product_id"`

	// ResourceId The resource ID of the entitlement
	ResourceId *string `json:"resource_id,omitempty"`

	// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
	// a different ID, otherwise, same as external_id)
	UsageReportingId *string `json:"usage_reporting_id,omitempty"`
} {
	return r.JSON201
}
func (r CreatePartnerV2EntitlementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreatePartnerV2EntitlementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreatePartnerV2EntitlementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreatePartnerV2EntitlementResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreatePartnerV2EntitlementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreatePartnerV2EntitlementResponse) GetBody() []byte {
	return r.Body
}
func (r CreatePartnerV2EntitlementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreatePartnerV2EntitlementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreatePartnerV2EntitlementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetPartnerV2EntitlementResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *GetPartnerV2Entitlement200JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// ExternalId The unique external ID of the entitlement (this should be unique to customer)
	ExternalId string `json:"external_id"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *GetPartnerV2Entitlement200JSONResponseBodyKind `json:"kind,omitempty"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata *ObjectMeta `json:"metadata,omitempty"`

	// Name The name of the entitlement
	Name string `json:"name"`

	// Organization The organization associated with this object.
	Organization *struct {
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
	} `json:"organization,omitempty"`

	// PlanId The plan ID the entitlement
	PlanId string `json:"plan_id"`

	// ProductId The product ID of the entitlement
	ProductId string `json:"product_id"`

	// ResourceId The resource ID of the entitlement
	ResourceId *string `json:"resource_id,omitempty"`

	// UsageReportingId The usage reporting ID of the entitlement (if usage reporting uses
	// a different ID, otherwise, same as external_id)
	UsageReportingId *string `json:"usage_reporting_id,omitempty"`
} {
	return r.JSON200
}
func (r GetPartnerV2EntitlementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetPartnerV2EntitlementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetPartnerV2EntitlementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetPartnerV2EntitlementResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetPartnerV2EntitlementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetPartnerV2EntitlementResponse) GetBody() []byte {
	return r.Body
}
func (r GetPartnerV2EntitlementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetPartnerV2EntitlementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetPartnerV2EntitlementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListPartnerV2OrganizationsResponse) GetJSON200() *PartnerV2OrganizationList {
	return r.JSON200
}
func (r ListPartnerV2OrganizationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListPartnerV2OrganizationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListPartnerV2OrganizationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListPartnerV2OrganizationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListPartnerV2OrganizationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListPartnerV2OrganizationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListPartnerV2OrganizationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListPartnerV2OrganizationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetPartnerV2OrganizationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetPartnerV2Organization200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetPartnerV2Organization200JSONResponseBodyKind `json:"kind"`

	// Metadata ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.
	Metadata ObjectMeta `json:"metadata"`

	// Name The name of the organization
	Name      *string                                                `json:"name,omitempty"`
	SsoConfig *GetPartnerV2Organization200JSONResponseBody_SsoConfig `json:"sso_config,omitempty"`

	// SsoUrl The login URL for the customer to access Confluent Cloud
	SsoUrl *string `json:"sso_url,omitempty"`
} {
	return r.JSON200
}
func (r GetPartnerV2OrganizationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetPartnerV2OrganizationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetPartnerV2OrganizationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetPartnerV2OrganizationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetPartnerV2OrganizationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetPartnerV2OrganizationResponse) GetBody() []byte {
	return r.Body
}
func (r GetPartnerV2OrganizationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetPartnerV2OrganizationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetPartnerV2OrganizationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r SignupResponse) GetJSON201() *PartnerSignupResponse {
	return r.JSON201
}
func (r SignupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r SignupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r SignupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r SignupResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r SignupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r SignupResponse) GetBody() []byte {
	return r.Body
}
func (r SignupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r SignupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r SignupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ActivateSignupResponse) GetJSON201() *PartnerSignupResponse {
	return r.JSON201
}
func (r ActivateSignupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ActivateSignupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ActivateSignupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ActivateSignupResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r ActivateSignupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ActivateSignupResponse) GetBody() []byte {
	return r.Body
}
func (r ActivateSignupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ActivateSignupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ActivateSignupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r SignupPartnerV2LinkResponse) GetJSON201() *PartnerSignupResponse {
	return r.JSON201
}
func (r SignupPartnerV2LinkResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r SignupPartnerV2LinkResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r SignupPartnerV2LinkResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r SignupPartnerV2LinkResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r SignupPartnerV2LinkResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r SignupPartnerV2LinkResponse) GetBody() []byte {
	return r.Body
}
func (r SignupPartnerV2LinkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r SignupPartnerV2LinkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r SignupPartnerV2LinkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
