package byok

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
	AwsKey ByokV1AwsKeyKind = "AwsKey"
)

func (e ByokV1AwsKeyKind) Valid() bool {
	switch e {
	case AwsKey:
		return true
	default:
		return false
	}
}

const (
	AzureKey ByokV1AzureKeyKind = "AzureKey"
)

func (e ByokV1AzureKeyKind) Valid() bool {
	switch e {
	case AzureKey:
		return true
	default:
		return false
	}
}

const (
	GcpKey ByokV1GcpKeyKind = "GcpKey"
)

func (e ByokV1GcpKeyKind) Valid() bool {
	switch e {
	case GcpKey:
		return true
	default:
		return false
	}
}

const (
	ByokV1KeyApiVersionByokv1 ByokV1KeyApiVersion = "byok/v1"
)

func (e ByokV1KeyApiVersion) Valid() bool {
	switch e {
	case ByokV1KeyApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	ByokV1KeyKindKey ByokV1KeyKind = "Key"
)

func (e ByokV1KeyKind) Valid() bool {
	switch e {
	case ByokV1KeyKindKey:
		return true
	default:
		return false
	}
}

const (
	ByokV1KeyListApiVersionByokv1 ByokV1KeyListApiVersion = "byok/v1"
)

func (e ByokV1KeyListApiVersion) Valid() bool {
	switch e {
	case ByokV1KeyListApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	ByokV1KeyListDataApiVersionByokv1 ByokV1KeyListDataApiVersion = "byok/v1"
)

func (e ByokV1KeyListDataApiVersion) Valid() bool {
	switch e {
	case ByokV1KeyListDataApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	ByokV1KeyListDataKindKey ByokV1KeyListDataKind = "Key"
)

func (e ByokV1KeyListDataKind) Valid() bool {
	switch e {
	case ByokV1KeyListDataKindKey:
		return true
	default:
		return false
	}
}

const (
	KeyList ByokV1KeyListKind = "KeyList"
)

func (e ByokV1KeyListKind) Valid() bool {
	switch e {
	case KeyList:
		return true
	default:
		return false
	}
}

const (
	CreateByokV1KeyJSONBodyApiVersionByokv1 CreateByokV1KeyJSONBodyApiVersion = "byok/v1"
)

func (e CreateByokV1KeyJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateByokV1KeyJSONBodyApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	CreateByokV1KeyJSONBodyKindKey CreateByokV1KeyJSONBodyKind = "Key"
)

func (e CreateByokV1KeyJSONBodyKind) Valid() bool {
	switch e {
	case CreateByokV1KeyJSONBodyKindKey:
		return true
	default:
		return false
	}
}

const (
	CreateByokV1Key201JSONResponseBodyApiVersionByokv1 CreateByokV1Key201JSONResponseBodyApiVersion = "byok/v1"
)

func (e CreateByokV1Key201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateByokV1Key201JSONResponseBodyApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	CreateByokV1Key201JSONResponseBodyKindKey CreateByokV1Key201JSONResponseBodyKind = "Key"
)

func (e CreateByokV1Key201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateByokV1Key201JSONResponseBodyKindKey:
		return true
	default:
		return false
	}
}

const (
	GetByokV1Key200JSONResponseBodyApiVersionByokv1 GetByokV1Key200JSONResponseBodyApiVersion = "byok/v1"
)

func (e GetByokV1Key200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetByokV1Key200JSONResponseBodyApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	GetByokV1Key200JSONResponseBodyKindKey GetByokV1Key200JSONResponseBodyKind = "Key"
)

func (e GetByokV1Key200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetByokV1Key200JSONResponseBodyKindKey:
		return true
	default:
		return false
	}
}

const (
	UpdateByokV1Key200JSONResponseBodyApiVersionByokv1 UpdateByokV1Key200JSONResponseBodyApiVersion = "byok/v1"
)

func (e UpdateByokV1Key200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateByokV1Key200JSONResponseBodyApiVersionByokv1:
		return true
	default:
		return false
	}
}

const (
	UpdateByokV1Key200JSONResponseBodyKindKey UpdateByokV1Key200JSONResponseBodyKind = "Key"
)

func (e UpdateByokV1Key200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateByokV1Key200JSONResponseBodyKindKey:
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
type ByokV1AwsKey struct {
	// KeyArn The Amazon Resource Name (ARN) of an AWS KMS key.
	KeyArn string `json:"key_arn"`

	// Kind BYOK kind type.
	Kind ByokV1AwsKeyKind `json:"kind"`

	// Roles The Amazon Resource Names (ARNs) of IAM Roles created for this key-environment combination.
	Roles *[]string `json:"roles,omitempty"`
}
type ByokV1AwsKeyKind string
type ByokV1AzureKey struct {
	// ApplicationId The Application ID created for this key-environment combination.
	ApplicationId *string `json:"application_id,omitempty"`

	// KeyId The unique Key Object Identifier URL without version of an Azure Key Vault key.
	KeyId string `json:"key_id"`

	// KeyVaultId Key Vault ID containing the key
	KeyVaultId string `json:"key_vault_id"`

	// Kind BYOK kind type.
	Kind ByokV1AzureKeyKind `json:"kind"`

	// TenantId Tenant ID (uuid) hosting the Key Vault containing the key
	TenantId string `json:"tenant_id"`
}
type ByokV1AzureKeyKind string
type ByokV1GcpKey struct {
	// KeyId The Google Cloud Platform key ID.
	KeyId string `json:"key_id"`

	// Kind BYOK kind type.
	Kind ByokV1GcpKeyKind `json:"kind"`

	// SecurityGroup The Google security group created for this key.
	SecurityGroup *string `json:"security_group,omitempty"`
}
type ByokV1GcpKeyKind string
type ByokV1Key struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ByokV1KeyApiVersion `json:"api_version,omitempty"`

	// DisplayName The human-readable name of the key object.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Key The cloud-specific key details.
	//
	// For AWS, provide the corresponding `key_arn`.
	//
	// For Azure, provide the corresponding `key_id`.
	//
	// For GCP, provide the corresponding `key_id`.
	Key *ByokV1Key_Key `json:"key,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ByokV1KeyKind `json:"kind,omitempty"`
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

	// Provider The cloud provider of the Key.
	Provider *string `json:"provider,omitempty"`

	// State The state of the key:
	//
	//   AVAILABLE: key can be used for a Kafka cluster provisioning.
	//
	//   IN_USE: key is already in use by a Kafka cluster provisioning.
	State *string `json:"state,omitempty"`

	// Validation The validation details of the key.
	Validation *ByokV1KeyValidation `json:"validation,omitempty"`
}
type ByokV1KeyApiVersion string
type ByokV1Key_Key struct {
	union json.RawMessage
}
type ByokV1KeyKind string
type ByokV1KeyList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ByokV1KeyListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ByokV1KeyListDataApiVersion `json:"api_version,omitempty"`

		// DisplayName The human-readable name of the key object.
		DisplayName *string `json:"display_name,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Key The cloud-specific key details.
		//
		// For AWS, provide the corresponding `key_arn`.
		//
		// For Azure, provide the corresponding `key_id`.
		//
		// For GCP, provide the corresponding `key_id`.
		Key ByokV1KeyList_Data_Key `json:"key"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *ByokV1KeyListDataKind `json:"kind,omitempty"`
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

		// Provider The cloud provider of the Key.
		Provider string `json:"provider"`

		// State The state of the key:
		//
		//   AVAILABLE: key can be used for a Kafka cluster provisioning.
		//
		//   IN_USE: key is already in use by a Kafka cluster provisioning.
		State string `json:"state"`

		// Validation The validation details of the key.
		Validation ByokV1KeyValidation `json:"validation"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ByokV1KeyListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ByokV1KeyListApiVersion string
type ByokV1KeyListDataApiVersion string
type ByokV1KeyList_Data_Key struct {
	union json.RawMessage
}
type ByokV1KeyListDataKind string
type ByokV1KeyListKind string
type ByokV1KeyValidation struct {
	// Message A message describing validation events.
	Message *string `json:"message,omitempty"`

	// Phase The validation phase of the key:
	//
	//   INITIALIZING: Initial phase for new keys awaiting first successful validation.
	//
	//   VALID: Last validation attempt succeeded.
	//
	//   INVALID: Last validation attempt failed.
	Phase string `json:"phase"`

	// Region The cloud region where the key is deployed. This value is computed by the
	// API after the key is successfully validated.
	Region *string `json:"region,omitempty"`

	// Since The timestamp since which the key is in the current validation phase.
	// Changes to the validation message or phase will update this timestamp.
	Since time.Time `json:"since"`
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
func (t ByokV1Key_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1Key_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *ByokV1Key_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1Key_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1Key_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *ByokV1Key_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1Key_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1Key_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *ByokV1Key_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1Key_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ByokV1Key_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ByokV1Key_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ByokV1Key_Key) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t ByokV1KeyList_Data_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1KeyList_Data_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *ByokV1KeyList_Data_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1KeyList_Data_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1KeyList_Data_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *ByokV1KeyList_Data_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1KeyList_Data_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ByokV1KeyList_Data_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *ByokV1KeyList_Data_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ByokV1KeyList_Data_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ByokV1KeyList_Data_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ByokV1KeyList_Data_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ByokV1KeyList_Data_Key) UnmarshalJSON(b []byte) error {
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
func (t CreateByokV1KeyJSONBody_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1KeyJSONBody_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1KeyJSONBody_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1KeyJSONBody_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1KeyJSONBody_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1KeyJSONBody_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1KeyJSONBody_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1KeyJSONBody_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1KeyJSONBody_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1KeyJSONBody_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateByokV1KeyJSONBody_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateByokV1KeyJSONBody_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateByokV1KeyJSONBody_Key) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreateByokV1Key201JSONResponseBody_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1Key201JSONResponseBody_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1Key201JSONResponseBody_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateByokV1Key201JSONResponseBody_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateByokV1Key201JSONResponseBody_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateByokV1Key201JSONResponseBody_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateByokV1Key201JSONResponseBody_Key) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetByokV1Key200JSONResponseBody_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetByokV1Key200JSONResponseBody_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *GetByokV1Key200JSONResponseBody_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetByokV1Key200JSONResponseBody_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetByokV1Key200JSONResponseBody_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *GetByokV1Key200JSONResponseBody_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetByokV1Key200JSONResponseBody_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetByokV1Key200JSONResponseBody_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *GetByokV1Key200JSONResponseBody_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetByokV1Key200JSONResponseBody_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetByokV1Key200JSONResponseBody_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetByokV1Key200JSONResponseBody_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetByokV1Key200JSONResponseBody_Key) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdateByokV1Key200JSONResponseBody_Key) AsByokV1AwsKey() (ByokV1AwsKey, error) {
	var body ByokV1AwsKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) FromByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	t.union = b
	return err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) MergeByokV1AwsKey(v ByokV1AwsKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdateByokV1Key200JSONResponseBody_Key) AsByokV1AzureKey() (ByokV1AzureKey, error) {
	var body ByokV1AzureKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) FromByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	t.union = b
	return err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) MergeByokV1AzureKey(v ByokV1AzureKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdateByokV1Key200JSONResponseBody_Key) AsByokV1GcpKey() (ByokV1GcpKey, error) {
	var body ByokV1GcpKey
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) FromByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	t.union = b
	return err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) MergeByokV1GcpKey(v ByokV1GcpKey) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpKey"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t UpdateByokV1Key200JSONResponseBody_Key) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdateByokV1Key200JSONResponseBody_Key) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsKey":
		return t.AsByokV1AwsKey()
	case "AzureKey":
		return t.AsByokV1AzureKey()
	case "GcpKey":
		return t.AsByokV1GcpKey()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdateByokV1Key200JSONResponseBody_Key) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdateByokV1Key200JSONResponseBody_Key) UnmarshalJSON(b []byte) error {
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

	// ListByokV1Keys List of Keys
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all keys.
	//
	// Corresponds with GET /byok/v1/keys (the `ListByokV1Keys` operationId).
	listByokV1Keys(ctx context.Context, params *ListByokV1KeysParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateByokV1KeyWithBody Create a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a key.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /byok/v1/keys (the `CreateByokV1Key` operationId).
	createByokV1KeyWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateByokV1Key Create a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a key.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /byok/v1/keys (the `CreateByokV1Key` operationId).
	createByokV1Key(ctx context.Context, body CreateByokV1KeyJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteByokV1Key Delete a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a key.
	//
	// Corresponds with DELETE /byok/v1/keys/{id} (the `DeleteByokV1Key` operationId).
	deleteByokV1Key(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetByokV1Key Read a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a key.
	//
	// Corresponds with GET /byok/v1/keys/{id} (the `GetByokV1Key` operationId).
	getByokV1Key(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateByokV1KeyWithBody Update a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a key.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /byok/v1/keys/{id} (the `UpdateByokV1Key` operationId).
	updateByokV1KeyWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateByokV1Key Update a Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a key.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /byok/v1/keys/{id} (the `UpdateByokV1Key` operationId).
	updateByokV1Key(ctx context.Context, id string, body UpdateByokV1KeyJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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
func (r ListByokV1KeysResponse) GetJSON200() *ByokV1KeyList {
	return r.JSON200
}
func (r ListByokV1KeysResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListByokV1KeysResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListByokV1KeysResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListByokV1KeysResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListByokV1KeysResponse) GetBody() []byte {
	return r.Body
}
func (r ListByokV1KeysResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListByokV1KeysResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListByokV1KeysResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateByokV1KeyResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateByokV1Key201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// DisplayName The human-readable name of the key object.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Key The cloud-specific key details.
	//
	// For AWS, provide the corresponding `key_arn`.
	//
	// For Azure, provide the corresponding `key_id`.
	//
	// For GCP, provide the corresponding `key_id`.
	Key CreateByokV1Key201JSONResponseBody_Key `json:"key"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateByokV1Key201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// Provider The cloud provider of the Key.
	Provider *string `json:"provider,omitempty"`

	// State The state of the key:
	//
	//   AVAILABLE: key can be used for a Kafka cluster provisioning.
	//
	//   IN_USE: key is already in use by a Kafka cluster provisioning.
	State *string `json:"state,omitempty"`

	// Validation The validation details of the key.
	Validation *ByokV1KeyValidation `json:"validation,omitempty"`
} {
	return r.JSON201
}
func (r CreateByokV1KeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateByokV1KeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateByokV1KeyResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateByokV1KeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateByokV1KeyResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateByokV1KeyResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateByokV1KeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateByokV1KeyResponse) GetBody() []byte {
	return r.Body
}
func (r CreateByokV1KeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateByokV1KeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateByokV1KeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteByokV1KeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteByokV1KeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteByokV1KeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteByokV1KeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteByokV1KeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteByokV1KeyResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteByokV1KeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteByokV1KeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteByokV1KeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetByokV1KeyResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetByokV1Key200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName The human-readable name of the key object.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Key The cloud-specific key details.
	//
	// For AWS, provide the corresponding `key_arn`.
	//
	// For Azure, provide the corresponding `key_id`.
	//
	// For GCP, provide the corresponding `key_id`.
	Key GetByokV1Key200JSONResponseBody_Key `json:"key"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetByokV1Key200JSONResponseBodyKind `json:"kind"`
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

	// Provider The cloud provider of the Key.
	Provider string `json:"provider"`

	// State The state of the key:
	//
	//   AVAILABLE: key can be used for a Kafka cluster provisioning.
	//
	//   IN_USE: key is already in use by a Kafka cluster provisioning.
	State string `json:"state"`

	// Validation The validation details of the key.
	Validation ByokV1KeyValidation `json:"validation"`
} {
	return r.JSON200
}
func (r GetByokV1KeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetByokV1KeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetByokV1KeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetByokV1KeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetByokV1KeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetByokV1KeyResponse) GetBody() []byte {
	return r.Body
}
func (r GetByokV1KeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetByokV1KeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetByokV1KeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateByokV1KeyResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateByokV1Key200JSONResponseBodyApiVersion `json:"api_version"`

	// DisplayName The human-readable name of the key object.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Key The cloud-specific key details.
	//
	// For AWS, provide the corresponding `key_arn`.
	//
	// For Azure, provide the corresponding `key_id`.
	//
	// For GCP, provide the corresponding `key_id`.
	Key UpdateByokV1Key200JSONResponseBody_Key `json:"key"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateByokV1Key200JSONResponseBodyKind `json:"kind"`
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

	// Provider The cloud provider of the Key.
	Provider string `json:"provider"`

	// State The state of the key:
	//
	//   AVAILABLE: key can be used for a Kafka cluster provisioning.
	//
	//   IN_USE: key is already in use by a Kafka cluster provisioning.
	State string `json:"state"`

	// Validation The validation details of the key.
	Validation ByokV1KeyValidation `json:"validation"`
} {
	return r.JSON200
}
func (r UpdateByokV1KeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateByokV1KeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateByokV1KeyResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateByokV1KeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateByokV1KeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateByokV1KeyResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateByokV1KeyResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateByokV1KeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateByokV1KeyResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateByokV1KeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateByokV1KeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateByokV1KeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
