package catalog

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
	LIST   AttributeDefCardinality = "LIST"
	SET    AttributeDefCardinality = "SET"
	SINGLE AttributeDefCardinality = "SINGLE"
)

func (e AttributeDefCardinality) Valid() bool {
	switch e {
	case LIST:
		return true
	case SET:
		return true
	case SINGLE:
		return true
	default:
		return false
	}
}

const (
	DEFAULT AttributeDefIndexType = "DEFAULT"
	STRING  AttributeDefIndexType = "STRING"
)

func (e AttributeDefIndexType) Valid() bool {
	switch e {
	case DEFAULT:
		return true
	case STRING:
		return true
	default:
		return false
	}
}

const (
	BusinessMetadataDefCategoryARRAY            BusinessMetadataDefCategory = "ARRAY"
	BusinessMetadataDefCategoryBUSINESSMETADATA BusinessMetadataDefCategory = "BUSINESS_METADATA"
	BusinessMetadataDefCategoryCLASSIFICATION   BusinessMetadataDefCategory = "CLASSIFICATION"
	BusinessMetadataDefCategoryENTITY           BusinessMetadataDefCategory = "ENTITY"
	BusinessMetadataDefCategoryENUM             BusinessMetadataDefCategory = "ENUM"
	BusinessMetadataDefCategoryMAP              BusinessMetadataDefCategory = "MAP"
	BusinessMetadataDefCategoryOBJECTIDTYPE     BusinessMetadataDefCategory = "OBJECT_ID_TYPE"
	BusinessMetadataDefCategoryPRIMITIVE        BusinessMetadataDefCategory = "PRIMITIVE"
	BusinessMetadataDefCategoryRELATIONSHIP     BusinessMetadataDefCategory = "RELATIONSHIP"
	BusinessMetadataDefCategorySTRUCT           BusinessMetadataDefCategory = "STRUCT"
)

func (e BusinessMetadataDefCategory) Valid() bool {
	switch e {
	case BusinessMetadataDefCategoryARRAY:
		return true
	case BusinessMetadataDefCategoryBUSINESSMETADATA:
		return true
	case BusinessMetadataDefCategoryCLASSIFICATION:
		return true
	case BusinessMetadataDefCategoryENTITY:
		return true
	case BusinessMetadataDefCategoryENUM:
		return true
	case BusinessMetadataDefCategoryMAP:
		return true
	case BusinessMetadataDefCategoryOBJECTIDTYPE:
		return true
	case BusinessMetadataDefCategoryPRIMITIVE:
		return true
	case BusinessMetadataDefCategoryRELATIONSHIP:
		return true
	case BusinessMetadataDefCategorySTRUCT:
		return true
	default:
		return false
	}
}

const (
	BusinessMetadataDefResponseCategoryARRAY            BusinessMetadataDefResponseCategory = "ARRAY"
	BusinessMetadataDefResponseCategoryBUSINESSMETADATA BusinessMetadataDefResponseCategory = "BUSINESS_METADATA"
	BusinessMetadataDefResponseCategoryCLASSIFICATION   BusinessMetadataDefResponseCategory = "CLASSIFICATION"
	BusinessMetadataDefResponseCategoryENTITY           BusinessMetadataDefResponseCategory = "ENTITY"
	BusinessMetadataDefResponseCategoryENUM             BusinessMetadataDefResponseCategory = "ENUM"
	BusinessMetadataDefResponseCategoryMAP              BusinessMetadataDefResponseCategory = "MAP"
	BusinessMetadataDefResponseCategoryOBJECTIDTYPE     BusinessMetadataDefResponseCategory = "OBJECT_ID_TYPE"
	BusinessMetadataDefResponseCategoryPRIMITIVE        BusinessMetadataDefResponseCategory = "PRIMITIVE"
	BusinessMetadataDefResponseCategoryRELATIONSHIP     BusinessMetadataDefResponseCategory = "RELATIONSHIP"
	BusinessMetadataDefResponseCategorySTRUCT           BusinessMetadataDefResponseCategory = "STRUCT"
)

func (e BusinessMetadataDefResponseCategory) Valid() bool {
	switch e {
	case BusinessMetadataDefResponseCategoryARRAY:
		return true
	case BusinessMetadataDefResponseCategoryBUSINESSMETADATA:
		return true
	case BusinessMetadataDefResponseCategoryCLASSIFICATION:
		return true
	case BusinessMetadataDefResponseCategoryENTITY:
		return true
	case BusinessMetadataDefResponseCategoryENUM:
		return true
	case BusinessMetadataDefResponseCategoryMAP:
		return true
	case BusinessMetadataDefResponseCategoryOBJECTIDTYPE:
		return true
	case BusinessMetadataDefResponseCategoryPRIMITIVE:
		return true
	case BusinessMetadataDefResponseCategoryRELATIONSHIP:
		return true
	case BusinessMetadataDefResponseCategorySTRUCT:
		return true
	default:
		return false
	}
}

const (
	ClassificationEntityStatusACTIVE  ClassificationEntityStatus = "ACTIVE"
	ClassificationEntityStatusDELETED ClassificationEntityStatus = "DELETED"
	ClassificationEntityStatusPURGED  ClassificationEntityStatus = "PURGED"
)

func (e ClassificationEntityStatus) Valid() bool {
	switch e {
	case ClassificationEntityStatusACTIVE:
		return true
	case ClassificationEntityStatusDELETED:
		return true
	case ClassificationEntityStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	ClassificationHeaderEntityStatusACTIVE  ClassificationHeaderEntityStatus = "ACTIVE"
	ClassificationHeaderEntityStatusDELETED ClassificationHeaderEntityStatus = "DELETED"
	ClassificationHeaderEntityStatusPURGED  ClassificationHeaderEntityStatus = "PURGED"
)

func (e ClassificationHeaderEntityStatus) Valid() bool {
	switch e {
	case ClassificationHeaderEntityStatusACTIVE:
		return true
	case ClassificationHeaderEntityStatusDELETED:
		return true
	case ClassificationHeaderEntityStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	EntityStatusACTIVE  EntityStatus = "ACTIVE"
	EntityStatusDELETED EntityStatus = "DELETED"
	EntityStatusPURGED  EntityStatus = "PURGED"
)

func (e EntityStatus) Valid() bool {
	switch e {
	case EntityStatusACTIVE:
		return true
	case EntityStatusDELETED:
		return true
	case EntityStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	EntityHeaderStatusACTIVE  EntityHeaderStatus = "ACTIVE"
	EntityHeaderStatusDELETED EntityHeaderStatus = "DELETED"
	EntityHeaderStatusPURGED  EntityHeaderStatus = "PURGED"
)

func (e EntityHeaderStatus) Valid() bool {
	switch e {
	case EntityHeaderStatusACTIVE:
		return true
	case EntityHeaderStatusDELETED:
		return true
	case EntityHeaderStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	PartialUpdateParamsStatusACTIVE  PartialUpdateParamsStatus = "ACTIVE"
	PartialUpdateParamsStatusDELETED PartialUpdateParamsStatus = "DELETED"
	PartialUpdateParamsStatusPURGED  PartialUpdateParamsStatus = "PURGED"
)

func (e PartialUpdateParamsStatus) Valid() bool {
	switch e {
	case PartialUpdateParamsStatusACTIVE:
		return true
	case PartialUpdateParamsStatusDELETED:
		return true
	case PartialUpdateParamsStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	TagEntityStatusACTIVE  TagEntityStatus = "ACTIVE"
	TagEntityStatusDELETED TagEntityStatus = "DELETED"
	TagEntityStatusPURGED  TagEntityStatus = "PURGED"
)

func (e TagEntityStatus) Valid() bool {
	switch e {
	case TagEntityStatusACTIVE:
		return true
	case TagEntityStatusDELETED:
		return true
	case TagEntityStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	TagDefCategoryARRAY            TagDefCategory = "ARRAY"
	TagDefCategoryBUSINESSMETADATA TagDefCategory = "BUSINESS_METADATA"
	TagDefCategoryCLASSIFICATION   TagDefCategory = "CLASSIFICATION"
	TagDefCategoryENTITY           TagDefCategory = "ENTITY"
	TagDefCategoryENUM             TagDefCategory = "ENUM"
	TagDefCategoryMAP              TagDefCategory = "MAP"
	TagDefCategoryOBJECTIDTYPE     TagDefCategory = "OBJECT_ID_TYPE"
	TagDefCategoryPRIMITIVE        TagDefCategory = "PRIMITIVE"
	TagDefCategoryRELATIONSHIP     TagDefCategory = "RELATIONSHIP"
	TagDefCategorySTRUCT           TagDefCategory = "STRUCT"
)

func (e TagDefCategory) Valid() bool {
	switch e {
	case TagDefCategoryARRAY:
		return true
	case TagDefCategoryBUSINESSMETADATA:
		return true
	case TagDefCategoryCLASSIFICATION:
		return true
	case TagDefCategoryENTITY:
		return true
	case TagDefCategoryENUM:
		return true
	case TagDefCategoryMAP:
		return true
	case TagDefCategoryOBJECTIDTYPE:
		return true
	case TagDefCategoryPRIMITIVE:
		return true
	case TagDefCategoryRELATIONSHIP:
		return true
	case TagDefCategorySTRUCT:
		return true
	default:
		return false
	}
}

const (
	TagDefResponseCategoryARRAY            TagDefResponseCategory = "ARRAY"
	TagDefResponseCategoryBUSINESSMETADATA TagDefResponseCategory = "BUSINESS_METADATA"
	TagDefResponseCategoryCLASSIFICATION   TagDefResponseCategory = "CLASSIFICATION"
	TagDefResponseCategoryENTITY           TagDefResponseCategory = "ENTITY"
	TagDefResponseCategoryENUM             TagDefResponseCategory = "ENUM"
	TagDefResponseCategoryMAP              TagDefResponseCategory = "MAP"
	TagDefResponseCategoryOBJECTIDTYPE     TagDefResponseCategory = "OBJECT_ID_TYPE"
	TagDefResponseCategoryPRIMITIVE        TagDefResponseCategory = "PRIMITIVE"
	TagDefResponseCategoryRELATIONSHIP     TagDefResponseCategory = "RELATIONSHIP"
	TagDefResponseCategorySTRUCT           TagDefResponseCategory = "STRUCT"
)

func (e TagDefResponseCategory) Valid() bool {
	switch e {
	case TagDefResponseCategoryARRAY:
		return true
	case TagDefResponseCategoryBUSINESSMETADATA:
		return true
	case TagDefResponseCategoryCLASSIFICATION:
		return true
	case TagDefResponseCategoryENTITY:
		return true
	case TagDefResponseCategoryENUM:
		return true
	case TagDefResponseCategoryMAP:
		return true
	case TagDefResponseCategoryOBJECTIDTYPE:
		return true
	case TagDefResponseCategoryPRIMITIVE:
		return true
	case TagDefResponseCategoryRELATIONSHIP:
		return true
	case TagDefResponseCategorySTRUCT:
		return true
	default:
		return false
	}
}

const (
	TagResponseEntityStatusACTIVE  TagResponseEntityStatus = "ACTIVE"
	TagResponseEntityStatusDELETED TagResponseEntityStatus = "DELETED"
	TagResponseEntityStatusPURGED  TagResponseEntityStatus = "PURGED"
)

func (e TagResponseEntityStatus) Valid() bool {
	switch e {
	case TagResponseEntityStatusACTIVE:
		return true
	case TagResponseEntityStatusDELETED:
		return true
	case TagResponseEntityStatusPURGED:
		return true
	default:
		return false
	}
}

const (
	DEPRECATED TermAssignmentHeaderStatus = "DEPRECATED"
	DISCOVERED TermAssignmentHeaderStatus = "DISCOVERED"
	IMPORTED   TermAssignmentHeaderStatus = "IMPORTED"
	OBSOLETE   TermAssignmentHeaderStatus = "OBSOLETE"
	OTHER      TermAssignmentHeaderStatus = "OTHER"
	PROPOSED   TermAssignmentHeaderStatus = "PROPOSED"
	VALIDATED  TermAssignmentHeaderStatus = "VALIDATED"
)

func (e TermAssignmentHeaderStatus) Valid() bool {
	switch e {
	case DEPRECATED:
		return true
	case DISCOVERED:
		return true
	case IMPORTED:
		return true
	case OBSOLETE:
		return true
	case OTHER:
		return true
	case PROPOSED:
		return true
	case VALIDATED:
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
	SearchUsingAttributeParamsSortOrderASCENDING  SearchUsingAttributeParamsSortOrder = "ASCENDING"
	SearchUsingAttributeParamsSortOrderDESCENDING SearchUsingAttributeParamsSortOrder = "DESCENDING"
)

func (e SearchUsingAttributeParamsSortOrder) Valid() bool {
	switch e {
	case SearchUsingAttributeParamsSortOrderASCENDING:
		return true
	case SearchUsingAttributeParamsSortOrderDESCENDING:
		return true
	default:
		return false
	}
}

const (
	SearchUsingBasicParamsSortOrderASCENDING  SearchUsingBasicParamsSortOrder = "ASCENDING"
	SearchUsingBasicParamsSortOrderDESCENDING SearchUsingBasicParamsSortOrder = "DESCENDING"
)

func (e SearchUsingBasicParamsSortOrder) Valid() bool {
	switch e {
	case SearchUsingBasicParamsSortOrderASCENDING:
		return true
	case SearchUsingBasicParamsSortOrderDESCENDING:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type AttributeDef struct {
	// Cardinality The cardinality
	Cardinality *AttributeDefCardinality `json:"cardinality,omitempty"`

	// Constraints The constraints
	Constraints *[]ConstraintDef `json:"constraints,omitempty"`

	// DefaultValue The default value
	DefaultValue *string `json:"defaultValue,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// DisplayName The display name
	DisplayName *string `json:"displayName,omitempty"`

	// IncludeInNotification Whether to include in notifications
	IncludeInNotification *bool `json:"includeInNotification,omitempty"`

	// IndexType The index type
	IndexType *AttributeDefIndexType `json:"indexType,omitempty"`

	// IsIndexable Whether is indexable
	IsIndexable *bool `json:"isIndexable,omitempty"`

	// IsOptional Whether is optional
	IsOptional *bool `json:"isOptional,omitempty"`

	// IsUnique Whether is unique
	IsUnique *bool `json:"isUnique,omitempty"`

	// Name The name
	Name *string `json:"name,omitempty"`

	// Options The options
	Options *map[string]string `json:"options,omitempty"`

	// SearchWeight The search weight
	SearchWeight *int32 `json:"searchWeight,omitempty"`

	// TypeName The type name
	TypeName *string `json:"typeName,omitempty"`

	// ValuesMaxCount The values max count
	ValuesMaxCount *int32 `json:"valuesMaxCount,omitempty"`

	// ValuesMinCount The values min count
	ValuesMinCount *int32 `json:"valuesMinCount,omitempty"`
}
type AttributeDefCardinality string
type AttributeDefIndexType string
type BusinessMetadata struct {
	// Attributes The business metadata attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// EntityName The qualified name of the entity
	EntityName *string `json:"entityName,omitempty"`

	// EntityType The entity type
	EntityType *string `json:"entityType,omitempty"`

	// TypeName The business metadata name
	TypeName *string `json:"typeName,omitempty"`
}
type BusinessMetadataDef struct {
	// AttributeDefs The attribute definitions
	AttributeDefs *[]AttributeDef `json:"attributeDefs,omitempty"`

	// Category The category
	Category *BusinessMetadataDefCategory `json:"category,omitempty"`

	// CreateTime The create time
	CreateTime *int64 `json:"createTime,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// Name The name
	Name *string `json:"name,omitempty"`

	// Options The options
	Options *map[string]string `json:"options,omitempty"`

	// ServiceType The service type
	ServiceType *string `json:"serviceType,omitempty"`

	// TypeVersion The type version
	TypeVersion *string `json:"typeVersion,omitempty"`

	// UpdateTime The update time
	UpdateTime *int64 `json:"updateTime,omitempty"`

	// UpdatedBy The updater
	UpdatedBy *string `json:"updatedBy,omitempty"`

	// Version The version
	Version *int32 `json:"version,omitempty"`
}
type BusinessMetadataDefCategory string
type BusinessMetadataDefResponse struct {
	// AttributeDefs The attribute definitions
	AttributeDefs *[]AttributeDef `json:"attributeDefs,omitempty"`

	// Category The category
	Category *BusinessMetadataDefResponseCategory `json:"category,omitempty"`

	// CreateTime The create time
	CreateTime *int64 `json:"createTime,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// Error Error message of this operation
	Error *ErrorMessage `json:"error,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// Name The name
	Name *string `json:"name,omitempty"`

	// Options The options
	Options *map[string]string `json:"options,omitempty"`

	// ServiceType The service type
	ServiceType *string `json:"serviceType,omitempty"`

	// TypeVersion The type version
	TypeVersion *string `json:"typeVersion,omitempty"`

	// UpdateTime The update time
	UpdateTime *int64 `json:"updateTime,omitempty"`

	// UpdatedBy The updater
	UpdatedBy *string `json:"updatedBy,omitempty"`

	// Version The version
	Version *int32 `json:"version,omitempty"`
}
type BusinessMetadataDefResponseCategory string
type BusinessMetadataResponse struct {
	// Attributes The business metadata attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// EntityName The qualified name of the entity
	EntityName *string `json:"entityName,omitempty"`

	// EntityType The entity type
	EntityType *string `json:"entityType,omitempty"`

	// Error Error message of this operation
	Error *ErrorMessage `json:"error,omitempty"`

	// TypeName The business metadata name
	TypeName *string `json:"typeName,omitempty"`
}
type Classification struct {
	// Attributes The tag attributes
	Attributes *map[string]map[string]interface{} `json:"attributes,omitempty"`

	// EntityGuid The internal entity guid
	EntityGuid *string `json:"entityGuid,omitempty"`

	// EntityStatus The entity status
	EntityStatus *ClassificationEntityStatus `json:"entityStatus,omitempty"`

	// Propagate Whether to propagate the tag
	Propagate *bool `json:"propagate,omitempty"`

	// RemovePropagationsOnEntityDelete Whether to remove propagations on entity delete
	RemovePropagationsOnEntityDelete *bool `json:"removePropagationsOnEntityDelete,omitempty"`

	// TypeName The tag name
	TypeName *string `json:"typeName,omitempty"`

	// ValidityPeriods The validity periods
	ValidityPeriods *[]TimeBoundary `json:"validityPeriods,omitempty"`
}
type ClassificationEntityStatus string
type ClassificationHeader struct {
	// EntityGuid The internal entity guid
	EntityGuid *string `json:"entityGuid,omitempty"`

	// EntityStatus The entity status
	EntityStatus *ClassificationHeaderEntityStatus `json:"entityStatus,omitempty"`

	// Propagate Whether to propagate the tag
	Propagate *bool `json:"propagate,omitempty"`

	// RemovePropagationsOnEntityDelete Whether to remove propagations on entity delete
	RemovePropagationsOnEntityDelete *bool `json:"removePropagationsOnEntityDelete,omitempty"`

	// TypeName The tag name
	TypeName *string `json:"typeName,omitempty"`
}
type ClassificationHeaderEntityStatus string
type ConstraintDef struct {
	// Params The params
	Params *map[string]map[string]interface{} `json:"params,omitempty"`

	// Type The type
	Type *string `json:"type,omitempty"`
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
type Entity struct {
	// Attributes The type attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// BusinessAttributes The business attributes
	BusinessAttributes *map[string]map[string]interface{} `json:"businessAttributes,omitempty"`

	// Classifications The classifications (tags)
	Classifications *[]Classification `json:"classifications,omitempty"`

	// CreateTime The create time
	CreateTime *int64 `json:"createTime,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// CustomAttributes The custom attributes
	CustomAttributes *map[string]string `json:"customAttributes,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// HomeId The home id
	HomeId *string `json:"homeId,omitempty"`

	// IsIncomplete Whether is incomplete
	IsIncomplete *bool `json:"isIncomplete,omitempty"`

	// IsProxy Whether is a proxy
	IsProxy *bool `json:"isProxy,omitempty"`

	// Labels The labels
	Labels *[]string `json:"labels,omitempty"`

	// Meanings The meanings
	Meanings *[]TermAssignmentHeader `json:"meanings,omitempty"`

	// ProvenanceType The provenance type
	ProvenanceType *int32 `json:"provenanceType,omitempty"`

	// Proxy Whether is a proxy
	Proxy *bool `json:"proxy,omitempty"`

	// RelationshipAttributes The relationship attributes
	RelationshipAttributes *map[string]interface{} `json:"relationshipAttributes,omitempty"`

	// Status The status
	Status *EntityStatus `json:"status,omitempty"`

	// TypeName The type name
	TypeName *string `json:"typeName,omitempty"`

	// UpdateTime The update time
	UpdateTime *int64 `json:"updateTime,omitempty"`

	// UpdatedBy The updater
	UpdatedBy *string `json:"updatedBy,omitempty"`

	// Version The version
	Version *int32 `json:"version,omitempty"`
}
type EntityStatus string
type EntityHeader struct {
	// Attributes The attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// ClassificationNames The classification (tag) names
	ClassificationNames *[]string `json:"classificationNames,omitempty"`

	// Classifications The classifications (tags)
	Classifications *[]Classification `json:"classifications,omitempty"`

	// DisplayText The display text
	DisplayText *string `json:"displayText,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// IsIncomplete Whether is incomplete
	IsIncomplete *bool `json:"isIncomplete,omitempty"`

	// Labels The labels
	Labels *[]string `json:"labels,omitempty"`

	// MeaningNames The meaning names
	MeaningNames *[]string `json:"meaningNames,omitempty"`

	// Meanings The meanings
	Meanings *[]TermAssignmentHeader `json:"meanings,omitempty"`

	// Status The status
	Status *EntityHeaderStatus `json:"status,omitempty"`

	// TypeName The type name
	TypeName *string `json:"typeName,omitempty"`
}
type EntityHeaderStatus string
type EntityPartialUpdate struct {
	// UPDATE The updated entities.
	UPDATE *[]PartialUpdateParams `json:"UPDATE,omitempty"`
}
type EntityPartialUpdateResponse struct {
	// MutatedEntities The updated entities.
	MutatedEntities *EntityPartialUpdate `json:"mutatedEntities,omitempty"`
}
type EntityWithExtInfo struct {
	// Entity The entity
	Entity *Entity `json:"entity,omitempty"`

	// ReferredEntities The referred entities
	ReferredEntities *map[string]Entity `json:"referredEntities,omitempty"`
}
type ErrorMessage struct {
	// ErrorCode The error code
	ErrorCode *int32 `json:"error_code,omitempty"`

	// Message The error message
	Message *string `json:"message,omitempty"`
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
type PartialUpdateParams struct {
	// Attributes The attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// ClassificationNames The classification (tag) names
	ClassificationNames *[]string `json:"classificationNames,omitempty"`

	// Classifications The classifications (tags)
	Classifications *[]ClassificationHeader `json:"classifications,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// IsIncomplete Whether is incomplete
	IsIncomplete *bool `json:"isIncomplete,omitempty"`

	// Status The status
	Status *PartialUpdateParamsStatus `json:"status,omitempty"`

	// TypeName The type name
	TypeName *string `json:"typeName,omitempty"`
}
type PartialUpdateParamsStatus string
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type SearchParams struct {
	// IncludeDeleted Whether to include deleted
	IncludeDeleted *bool `json:"includeDeleted,omitempty"`

	// Limit The limit
	Limit *int32 `json:"limit,omitempty"`

	// Offset The offset
	Offset *int32 `json:"offset,omitempty"`
}
type SearchResult struct {
	// Entities The entities
	Entities *[]EntityHeader `json:"entities,omitempty"`

	// ReferredEntities The referred entities
	ReferredEntities *map[string]EntityHeader `json:"referredEntities,omitempty"`

	// SearchParameters Search paramas to filter results
	SearchParameters *SearchParams `json:"searchParameters,omitempty"`

	// Types The types
	Types *[]string `json:"types,omitempty"`
}
type Tag struct {
	// Attributes The tag attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// EntityGuid The internal entity guid
	EntityGuid *string `json:"entityGuid,omitempty"`

	// EntityName The qualified name of the entity
	EntityName *string `json:"entityName,omitempty"`

	// EntityStatus The entity status
	EntityStatus *TagEntityStatus `json:"entityStatus,omitempty"`

	// EntityType The entity type
	EntityType *string `json:"entityType,omitempty"`

	// Propagate Whether to propagate the tag
	Propagate *bool `json:"propagate,omitempty"`

	// RemovePropagationsOnEntityDelete Whether to remove propagations on entity delete
	RemovePropagationsOnEntityDelete *bool `json:"removePropagationsOnEntityDelete,omitempty"`

	// TypeName The tag name
	TypeName *string `json:"typeName,omitempty"`

	// ValidityPeriods The validity periods
	ValidityPeriods *[]TimeBoundary `json:"validityPeriods,omitempty"`
}
type TagEntityStatus string
type TagDef struct {
	// AttributeDefs The attribute definitions
	AttributeDefs *[]AttributeDef `json:"attributeDefs,omitempty"`

	// Category The category
	Category *TagDefCategory `json:"category,omitempty"`

	// CreateTime The create time
	CreateTime *int64 `json:"createTime,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// EntityTypes The entity types
	EntityTypes *[]string `json:"entityTypes,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// Name The name
	Name *string `json:"name,omitempty"`

	// Options The options
	Options *map[string]string `json:"options,omitempty"`

	// ServiceType The service type
	ServiceType *string `json:"serviceType,omitempty"`

	// SubTypes The subtypes
	SubTypes *[]string `json:"subTypes,omitempty"`

	// SuperTypes The supertypes
	SuperTypes *[]string `json:"superTypes,omitempty"`

	// TypeVersion The type version
	TypeVersion *string `json:"typeVersion,omitempty"`

	// UpdateTime The update time
	UpdateTime *int64 `json:"updateTime,omitempty"`

	// UpdatedBy The updater
	UpdatedBy *string `json:"updatedBy,omitempty"`

	// Version The version
	Version *int32 `json:"version,omitempty"`
}
type TagDefCategory string
type TagDefResponse struct {
	// AttributeDefs The attribute definitions
	AttributeDefs *[]AttributeDef `json:"attributeDefs,omitempty"`

	// Category The category
	Category *TagDefResponseCategory `json:"category,omitempty"`

	// CreateTime The create time
	CreateTime *int64 `json:"createTime,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// EntityTypes The entity types
	EntityTypes *[]string `json:"entityTypes,omitempty"`

	// Error Error message of this operation
	Error *ErrorMessage `json:"error,omitempty"`

	// Guid The internal guid
	Guid *string `json:"guid,omitempty"`

	// Name The name
	Name *string `json:"name,omitempty"`

	// Options The options
	Options *map[string]string `json:"options,omitempty"`

	// ServiceType The service type
	ServiceType *string `json:"serviceType,omitempty"`

	// SubTypes The subtypes
	SubTypes *[]string `json:"subTypes,omitempty"`

	// SuperTypes The supertypes
	SuperTypes *[]string `json:"superTypes,omitempty"`

	// TypeVersion The type version
	TypeVersion *string `json:"typeVersion,omitempty"`

	// UpdateTime The update time
	UpdateTime *int64 `json:"updateTime,omitempty"`

	// UpdatedBy The updater
	UpdatedBy *string `json:"updatedBy,omitempty"`

	// Version The version
	Version *int32 `json:"version,omitempty"`
}
type TagDefResponseCategory string
type TagResponse struct {
	// Attributes The tag attributes
	Attributes *map[string]interface{} `json:"attributes,omitempty"`

	// EntityGuid The internal entity guid
	EntityGuid *string `json:"entityGuid,omitempty"`

	// EntityName The qualified name of the entity
	EntityName *string `json:"entityName,omitempty"`

	// EntityStatus The entity status
	EntityStatus *TagResponseEntityStatus `json:"entityStatus,omitempty"`

	// EntityType The entity type
	EntityType *string `json:"entityType,omitempty"`

	// Error Error message of this operation
	Error *ErrorMessage `json:"error,omitempty"`

	// Propagate Whether to propagate the tag
	Propagate *bool `json:"propagate,omitempty"`

	// RemovePropagationsOnEntityDelete Whether to remove propagations on entity delete
	RemovePropagationsOnEntityDelete *bool `json:"removePropagationsOnEntityDelete,omitempty"`

	// TypeName The tag name
	TypeName *string `json:"typeName,omitempty"`

	// ValidityPeriods The validity periods
	ValidityPeriods *[]TimeBoundary `json:"validityPeriods,omitempty"`
}
type TagResponseEntityStatus string
type TermAssignmentHeader struct {
	// Confidence The confidence
	Confidence *int32 `json:"confidence,omitempty"`

	// CreatedBy The creator
	CreatedBy *string `json:"createdBy,omitempty"`

	// Description The description
	Description *string `json:"description,omitempty"`

	// DisplayText The display text
	DisplayText *string `json:"displayText,omitempty"`

	// Expression The expression
	Expression *string `json:"expression,omitempty"`

	// RelationGuid The relation guid
	RelationGuid *string `json:"relationGuid,omitempty"`

	// Source The source
	Source *string `json:"source,omitempty"`

	// Status The status
	Status *TermAssignmentHeaderStatus `json:"status,omitempty"`

	// Steward The steward
	Steward *string `json:"steward,omitempty"`

	// TermGuid The term guid
	TermGuid *string `json:"termGuid,omitempty"`
}
type TermAssignmentHeaderStatus string
type TimeBoundary struct {
	// EndTime The end time of format yyyy/MM/dd HH:mm:ss
	EndTime *string `json:"endTime,omitempty"`

	// StartTime The start time of format yyyy/MM/dd HH:mm:ss
	StartTime *string `json:"startTime,omitempty"`

	// TimeZone The time zone (see java.util.TimeZone)
	TimeZone *string `json:"timeZone,omitempty"`
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

type ClientInterface interface {

	// PartialEntityUpdateWithBody Update an Entity Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Partially update an entity attribute.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /catalog/v1/entity (the `PartialEntityUpdate` operationId).
	PartialEntityUpdateWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PartialEntityUpdate Update an Entity Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Partially update an entity attribute.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /catalog/v1/entity (the `PartialEntityUpdate` operationId).
	PartialEntityUpdate(ctx context.Context, body PartialEntityUpdateJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateBusinessMetadataWithBody Bulk Create Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple business metadata.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /catalog/v1/entity/businessmetadata (the `CreateBusinessMetadata` operationId).
	CreateBusinessMetadataWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateBusinessMetadata Bulk Create Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple business metadata.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /catalog/v1/entity/businessmetadata (the `CreateBusinessMetadata` operationId).
	CreateBusinessMetadata(ctx context.Context, body CreateBusinessMetadataJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateBusinessMetadataWithBody Bulk Update Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple business metadata.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /catalog/v1/entity/businessmetadata (the `UpdateBusinessMetadata` operationId).
	UpdateBusinessMetadataWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateBusinessMetadata Bulk Update Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple business metadata.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /catalog/v1/entity/businessmetadata (the `UpdateBusinessMetadata` operationId).
	UpdateBusinessMetadata(ctx context.Context, body UpdateBusinessMetadataJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTagsWithBody Bulk Create Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple tags.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /catalog/v1/entity/tags (the `CreateTags` operationId).
	CreateTagsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTags Bulk Create Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple tags.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /catalog/v1/entity/tags (the `CreateTags` operationId).
	CreateTags(ctx context.Context, body CreateTagsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTagsWithBody Bulk Update Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple tags.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /catalog/v1/entity/tags (the `UpdateTags` operationId).
	UpdateTagsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTags Bulk Update Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple tags.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /catalog/v1/entity/tags (the `UpdateTags` operationId).
	UpdateTags(ctx context.Context, body UpdateTagsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetByUniqueAttributes Read an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Fetch complete definition of an entity given its type and unique attribute.
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName} (the `GetByUniqueAttributes` operationId).
	GetByUniqueAttributes(ctx context.Context, typeName string, qualifiedName string, params *GetByUniqueAttributesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetBusinessMetadata Read Business Metadata for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Gets the list of business metadata for a given entity represented
	// by a qualified name.
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/businessmetadata (the `GetBusinessMetadata` operationId).
	GetBusinessMetadata(ctx context.Context, typeName string, qualifiedName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteBusinessMetadata Delete a Business Metadata for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a business metadata on an entity.
	//
	// Corresponds with DELETE /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/businessmetadata/{bmName} (the `DeleteBusinessMetadata` operationId).
	DeleteBusinessMetadata(ctx context.Context, typeName string, qualifiedName string, bmName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetTags Read Tags for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Gets the list of tags for a given entity represented by a qualified name.
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/tags (the `GetTags` operationId).
	GetTags(ctx context.Context, typeName string, qualifiedName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteTag Delete a Tag for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a tag for an entity.
	//
	// Corresponds with DELETE /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/tags/{tagName} (the `DeleteTag` operationId).
	DeleteTag(ctx context.Context, typeName string, qualifiedName string, tagName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// SearchUsingAttribute Search by Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve data for the specified attribute search query.
	//
	// Corresponds with GET /catalog/v1/search/attribute (the `SearchUsingAttribute` operationId).
	SearchUsingAttribute(ctx context.Context, params *SearchUsingAttributeParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// SearchUsingBasic Search by Fulltext Query
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve data for the specified fulltext query.
	//
	// Corresponds with GET /catalog/v1/search/basic (the `SearchUsingBasic` operationId).
	SearchUsingBasic(ctx context.Context, params *SearchUsingBasicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetAllBusinessMetadataDefs Bulk Read Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk retrieval API for retrieving business metadata definitions.
	//
	// Corresponds with GET /catalog/v1/types/businessmetadatadefs (the `GetAllBusinessMetadataDefs` operationId).
	GetAllBusinessMetadataDefs(ctx context.Context, params *GetAllBusinessMetadataDefsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateBusinessMetadataDefsWithBody Bulk Create Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for business metadata definitions.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /catalog/v1/types/businessmetadatadefs (the `CreateBusinessMetadataDefs` operationId).
	CreateBusinessMetadataDefsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateBusinessMetadataDefs Bulk Create Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for business metadata definitions.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /catalog/v1/types/businessmetadatadefs (the `CreateBusinessMetadataDefs` operationId).
	CreateBusinessMetadataDefs(ctx context.Context, body CreateBusinessMetadataDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateBusinessMetadataDefsWithBody Bulk Update Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for business metadata definitions.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /catalog/v1/types/businessmetadatadefs (the `UpdateBusinessMetadataDefs` operationId).
	UpdateBusinessMetadataDefsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateBusinessMetadataDefs Bulk Update Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for business metadata definitions.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /catalog/v1/types/businessmetadatadefs (the `UpdateBusinessMetadataDefs` operationId).
	UpdateBusinessMetadataDefs(ctx context.Context, body UpdateBusinessMetadataDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteBusinessMetadataDef Delete Business Metadata Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete API for business metadata definition identified by its name.
	//
	// Corresponds with DELETE /catalog/v1/types/businessmetadatadefs/{bmName} (the `DeleteBusinessMetadataDef` operationId).
	DeleteBusinessMetadataDef(ctx context.Context, bmName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetBusinessMetadataDefByName Read Business Metadata Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the business metadata definition with the given name.
	//
	// Corresponds with GET /catalog/v1/types/businessmetadatadefs/{bmName} (the `GetBusinessMetadataDefByName` operationId).
	GetBusinessMetadataDefByName(ctx context.Context, bmName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetAllTagDefs Bulk Read Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk retrieval API for retrieving tag definitions.
	//
	// Corresponds with GET /catalog/v1/types/tagdefs (the `GetAllTagDefs` operationId).
	GetAllTagDefs(ctx context.Context, params *GetAllTagDefsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTagDefsWithBody Bulk Create Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for tag definitions.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /catalog/v1/types/tagdefs (the `CreateTagDefs` operationId).
	CreateTagDefsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateTagDefs Bulk Create Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for tag definitions.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /catalog/v1/types/tagdefs (the `CreateTagDefs` operationId).
	CreateTagDefs(ctx context.Context, body CreateTagDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTagDefsWithBody Bulk Update Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for tag definitions.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /catalog/v1/types/tagdefs (the `UpdateTagDefs` operationId).
	UpdateTagDefsWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTagDefs Bulk Update Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for tag definitions.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /catalog/v1/types/tagdefs (the `UpdateTagDefs` operationId).
	UpdateTagDefs(ctx context.Context, body UpdateTagDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteTagDef Delete Tag Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete API for tag definition identified by its name.
	//
	// Corresponds with DELETE /catalog/v1/types/tagdefs/{tagName} (the `DeleteTagDef` operationId).
	DeleteTagDef(ctx context.Context, tagName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetTagDefByName Read Tag Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the tag definition with the given name.
	//
	// Corresponds with GET /catalog/v1/types/tagdefs/{tagName} (the `GetTagDefByName` operationId).
	GetTagDefByName(ctx context.Context, tagName string, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// PartialEntityUpdateWithBodyWithResponse Update an Entity Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Partially update an entity attribute.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity (the `PartialEntityUpdate` operationId).
	PartialEntityUpdateWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PartialEntityUpdateResponse, error)

	// PartialEntityUpdateWithResponse Update an Entity Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Partially update an entity attribute.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity (the `PartialEntityUpdate` operationId).
	PartialEntityUpdateWithResponse(ctx context.Context, body PartialEntityUpdateJSONRequestBody, reqEditors ...RequestEditorFn) (*PartialEntityUpdateResponse, error)

	// CreateBusinessMetadataWithBodyWithResponse Bulk Create Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple business metadata.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/entity/businessmetadata (the `CreateBusinessMetadata` operationId).
	CreateBusinessMetadataWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateBusinessMetadataResponse, error)

	// CreateBusinessMetadataWithResponse Bulk Create Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple business metadata.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/entity/businessmetadata (the `CreateBusinessMetadata` operationId).
	CreateBusinessMetadataWithResponse(ctx context.Context, body CreateBusinessMetadataJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateBusinessMetadataResponse, error)

	// UpdateBusinessMetadataWithBodyWithResponse Bulk Update Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple business metadata.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity/businessmetadata (the `UpdateBusinessMetadata` operationId).
	UpdateBusinessMetadataWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateBusinessMetadataResponse, error)

	// UpdateBusinessMetadataWithResponse Bulk Update Business Metadata
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple business metadata.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity/businessmetadata (the `UpdateBusinessMetadata` operationId).
	UpdateBusinessMetadataWithResponse(ctx context.Context, body UpdateBusinessMetadataJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateBusinessMetadataResponse, error)

	// CreateTagsWithBodyWithResponse Bulk Create Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple tags.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/entity/tags (the `CreateTags` operationId).
	CreateTagsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateTagsResponse, error)

	// CreateTagsWithResponse Bulk Create Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to create multiple tags.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/entity/tags (the `CreateTags` operationId).
	CreateTagsWithResponse(ctx context.Context, body CreateTagsJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateTagsResponse, error)

	// UpdateTagsWithBodyWithResponse Bulk Update Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple tags.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity/tags (the `UpdateTags` operationId).
	UpdateTagsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateTagsResponse, error)

	// UpdateTagsWithResponse Bulk Update Tags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk API to update multiple tags.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/entity/tags (the `UpdateTags` operationId).
	UpdateTagsWithResponse(ctx context.Context, body UpdateTagsJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTagsResponse, error)

	// GetByUniqueAttributesWithResponse Read an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Fetch complete definition of an entity given its type and unique attribute.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName} (the `GetByUniqueAttributes` operationId).
	GetByUniqueAttributesWithResponse(ctx context.Context, typeName string, qualifiedName string, params *GetByUniqueAttributesParams, reqEditors ...RequestEditorFn) (*GetByUniqueAttributesResponse, error)

	// GetBusinessMetadataWithResponse Read Business Metadata for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Gets the list of business metadata for a given entity represented
	// by a qualified name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/businessmetadata (the `GetBusinessMetadata` operationId).
	GetBusinessMetadataWithResponse(ctx context.Context, typeName string, qualifiedName string, reqEditors ...RequestEditorFn) (*GetBusinessMetadataResponse, error)

	// DeleteBusinessMetadataWithResponse Delete a Business Metadata for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a business metadata on an entity.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/businessmetadata/{bmName} (the `DeleteBusinessMetadata` operationId).
	DeleteBusinessMetadataWithResponse(ctx context.Context, typeName string, qualifiedName string, bmName string, reqEditors ...RequestEditorFn) (*DeleteBusinessMetadataResponse, error)

	// GetTagsWithResponse Read Tags for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Gets the list of tags for a given entity represented by a qualified name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/tags (the `GetTags` operationId).
	GetTagsWithResponse(ctx context.Context, typeName string, qualifiedName string, reqEditors ...RequestEditorFn) (*GetTagsResponse, error)

	// DeleteTagWithResponse Delete a Tag for an Entity
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a tag for an entity.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /catalog/v1/entity/type/{typeName}/name/{qualifiedName}/tags/{tagName} (the `DeleteTag` operationId).
	DeleteTagWithResponse(ctx context.Context, typeName string, qualifiedName string, tagName string, reqEditors ...RequestEditorFn) (*DeleteTagResponse, error)

	// SearchUsingAttributeWithResponse Search by Attribute
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve data for the specified attribute search query.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/search/attribute (the `SearchUsingAttribute` operationId).
	SearchUsingAttributeWithResponse(ctx context.Context, params *SearchUsingAttributeParams, reqEditors ...RequestEditorFn) (*SearchUsingAttributeResponse, error)

	// SearchUsingBasicWithResponse Search by Fulltext Query
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve data for the specified fulltext query.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/search/basic (the `SearchUsingBasic` operationId).
	SearchUsingBasicWithResponse(ctx context.Context, params *SearchUsingBasicParams, reqEditors ...RequestEditorFn) (*SearchUsingBasicResponse, error)

	// GetAllBusinessMetadataDefsWithResponse Bulk Read Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk retrieval API for retrieving business metadata definitions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/types/businessmetadatadefs (the `GetAllBusinessMetadataDefs` operationId).
	GetAllBusinessMetadataDefsWithResponse(ctx context.Context, params *GetAllBusinessMetadataDefsParams, reqEditors ...RequestEditorFn) (*GetAllBusinessMetadataDefsResponse, error)

	// CreateBusinessMetadataDefsWithBodyWithResponse Bulk Create Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for business metadata definitions.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/types/businessmetadatadefs (the `CreateBusinessMetadataDefs` operationId).
	CreateBusinessMetadataDefsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateBusinessMetadataDefsResponse, error)

	// CreateBusinessMetadataDefsWithResponse Bulk Create Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for business metadata definitions.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/types/businessmetadatadefs (the `CreateBusinessMetadataDefs` operationId).
	CreateBusinessMetadataDefsWithResponse(ctx context.Context, body CreateBusinessMetadataDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateBusinessMetadataDefsResponse, error)

	// UpdateBusinessMetadataDefsWithBodyWithResponse Bulk Update Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for business metadata definitions.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/types/businessmetadatadefs (the `UpdateBusinessMetadataDefs` operationId).
	UpdateBusinessMetadataDefsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateBusinessMetadataDefsResponse, error)

	// UpdateBusinessMetadataDefsWithResponse Bulk Update Business Metadata Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for business metadata definitions.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/types/businessmetadatadefs (the `UpdateBusinessMetadataDefs` operationId).
	UpdateBusinessMetadataDefsWithResponse(ctx context.Context, body UpdateBusinessMetadataDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateBusinessMetadataDefsResponse, error)

	// DeleteBusinessMetadataDefWithResponse Delete Business Metadata Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete API for business metadata definition identified by its name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /catalog/v1/types/businessmetadatadefs/{bmName} (the `DeleteBusinessMetadataDef` operationId).
	DeleteBusinessMetadataDefWithResponse(ctx context.Context, bmName string, reqEditors ...RequestEditorFn) (*DeleteBusinessMetadataDefResponse, error)

	// GetBusinessMetadataDefByNameWithResponse Read Business Metadata Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the business metadata definition with the given name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/types/businessmetadatadefs/{bmName} (the `GetBusinessMetadataDefByName` operationId).
	GetBusinessMetadataDefByNameWithResponse(ctx context.Context, bmName string, reqEditors ...RequestEditorFn) (*GetBusinessMetadataDefByNameResponse, error)

	// GetAllTagDefsWithResponse Bulk Read Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk retrieval API for retrieving tag definitions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/types/tagdefs (the `GetAllTagDefs` operationId).
	GetAllTagDefsWithResponse(ctx context.Context, params *GetAllTagDefsParams, reqEditors ...RequestEditorFn) (*GetAllTagDefsResponse, error)

	// CreateTagDefsWithBodyWithResponse Bulk Create Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for tag definitions.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/types/tagdefs (the `CreateTagDefs` operationId).
	CreateTagDefsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateTagDefsResponse, error)

	// CreateTagDefsWithResponse Bulk Create Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk create API for tag definitions.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /catalog/v1/types/tagdefs (the `CreateTagDefs` operationId).
	CreateTagDefsWithResponse(ctx context.Context, body CreateTagDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateTagDefsResponse, error)

	// UpdateTagDefsWithBodyWithResponse Bulk Update Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for tag definitions.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/types/tagdefs (the `UpdateTagDefs` operationId).
	UpdateTagDefsWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateTagDefsResponse, error)

	// UpdateTagDefsWithResponse Bulk Update Tag Definitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Bulk update API for tag definitions.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /catalog/v1/types/tagdefs (the `UpdateTagDefs` operationId).
	UpdateTagDefsWithResponse(ctx context.Context, body UpdateTagDefsJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTagDefsResponse, error)

	// DeleteTagDefWithResponse Delete Tag Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete API for tag definition identified by its name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /catalog/v1/types/tagdefs/{tagName} (the `DeleteTagDef` operationId).
	DeleteTagDefWithResponse(ctx context.Context, tagName string, reqEditors ...RequestEditorFn) (*DeleteTagDefResponse, error)

	// GetTagDefByNameWithResponse Read Tag Definition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the tag definition with the given name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /catalog/v1/types/tagdefs/{tagName} (the `GetTagDefByName` operationId).
	GetTagDefByNameWithResponse(ctx context.Context, tagName string, reqEditors ...RequestEditorFn) (*GetTagDefByNameResponse, error)
}

func (r PartialEntityUpdateResponse) GetJSON200() *EntityPartialUpdateResponse {
	return r.JSON200
}
func (r PartialEntityUpdateResponse) GetBody() []byte {
	return r.Body
}
func (r PartialEntityUpdateResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PartialEntityUpdateResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PartialEntityUpdateResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateBusinessMetadataResponse) GetJSON200() *[]BusinessMetadataResponse {
	return r.JSON200
}
func (r CreateBusinessMetadataResponse) GetBody() []byte {
	return r.Body
}
func (r CreateBusinessMetadataResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateBusinessMetadataResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateBusinessMetadataResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateBusinessMetadataResponse) GetJSON200() *[]BusinessMetadataResponse {
	return r.JSON200
}
func (r UpdateBusinessMetadataResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateBusinessMetadataResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateBusinessMetadataResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateBusinessMetadataResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateTagsResponse) GetJSON200() *[]TagResponse {
	return r.JSON200
}
func (r CreateTagsResponse) GetBody() []byte {
	return r.Body
}
func (r CreateTagsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateTagsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateTagsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateTagsResponse) GetJSON200() *[]TagResponse {
	return r.JSON200
}
func (r UpdateTagsResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateTagsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateTagsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateTagsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetByUniqueAttributesResponse) GetJSON200() *EntityWithExtInfo {
	return r.JSON200
}
func (r GetByUniqueAttributesResponse) GetBody() []byte {
	return r.Body
}
func (r GetByUniqueAttributesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetByUniqueAttributesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetByUniqueAttributesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetBusinessMetadataResponse) GetJSON200() *[]BusinessMetadataResponse {
	return r.JSON200
}
func (r GetBusinessMetadataResponse) GetBody() []byte {
	return r.Body
}
func (r GetBusinessMetadataResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetBusinessMetadataResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetBusinessMetadataResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteBusinessMetadataResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteBusinessMetadataResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteBusinessMetadataResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteBusinessMetadataResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetTagsResponse) GetJSON200() *[]TagResponse {
	return r.JSON200
}
func (r GetTagsResponse) GetBody() []byte {
	return r.Body
}
func (r GetTagsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetTagsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetTagsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteTagResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteTagResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteTagResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteTagResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r SearchUsingAttributeResponse) GetJSON200() *SearchResult {
	return r.JSON200
}
func (r SearchUsingAttributeResponse) GetBody() []byte {
	return r.Body
}
func (r SearchUsingAttributeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r SearchUsingAttributeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r SearchUsingAttributeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r SearchUsingBasicResponse) GetJSON200() *SearchResult {
	return r.JSON200
}
func (r SearchUsingBasicResponse) GetBody() []byte {
	return r.Body
}
func (r SearchUsingBasicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r SearchUsingBasicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r SearchUsingBasicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetAllBusinessMetadataDefsResponse) GetJSON200() *[]BusinessMetadataDefResponse {
	return r.JSON200
}
func (r GetAllBusinessMetadataDefsResponse) GetBody() []byte {
	return r.Body
}
func (r GetAllBusinessMetadataDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetAllBusinessMetadataDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetAllBusinessMetadataDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateBusinessMetadataDefsResponse) GetJSON200() *[]BusinessMetadataDefResponse {
	return r.JSON200
}
func (r CreateBusinessMetadataDefsResponse) GetBody() []byte {
	return r.Body
}
func (r CreateBusinessMetadataDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateBusinessMetadataDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateBusinessMetadataDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateBusinessMetadataDefsResponse) GetJSON200() *[]BusinessMetadataDefResponse {
	return r.JSON200
}
func (r UpdateBusinessMetadataDefsResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateBusinessMetadataDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateBusinessMetadataDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateBusinessMetadataDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteBusinessMetadataDefResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteBusinessMetadataDefResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteBusinessMetadataDefResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteBusinessMetadataDefResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetBusinessMetadataDefByNameResponse) GetJSON200() *BusinessMetadataDef {
	return r.JSON200
}
func (r GetBusinessMetadataDefByNameResponse) GetBody() []byte {
	return r.Body
}
func (r GetBusinessMetadataDefByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetBusinessMetadataDefByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetBusinessMetadataDefByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetAllTagDefsResponse) GetJSON200() *[]TagDefResponse {
	return r.JSON200
}
func (r GetAllTagDefsResponse) GetBody() []byte {
	return r.Body
}
func (r GetAllTagDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetAllTagDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetAllTagDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateTagDefsResponse) GetJSON200() *[]TagDefResponse {
	return r.JSON200
}
func (r CreateTagDefsResponse) GetBody() []byte {
	return r.Body
}
func (r CreateTagDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateTagDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateTagDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateTagDefsResponse) GetJSON200() *[]TagDefResponse {
	return r.JSON200
}
func (r UpdateTagDefsResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateTagDefsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateTagDefsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateTagDefsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteTagDefResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteTagDefResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteTagDefResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteTagDefResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetTagDefByNameResponse) GetJSON200() *TagDef {
	return r.JSON200
}
func (r GetTagDefByNameResponse) GetBody() []byte {
	return r.Body
}
func (r GetTagDefByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetTagDefByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetTagDefByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
