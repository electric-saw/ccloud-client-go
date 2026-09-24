package scim

import (
	"bytes"
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
	ScimV2PatchOpOperationsOpAdd     ScimV2PatchOpOperationsOp = "add"
	ScimV2PatchOpOperationsOpRemove  ScimV2PatchOpOperationsOp = "remove"
	ScimV2PatchOpOperationsOpReplace ScimV2PatchOpOperationsOp = "replace"
)

func (e ScimV2PatchOpOperationsOp) Valid() bool {
	switch e {
	case ScimV2PatchOpOperationsOpAdd:
		return true
	case ScimV2PatchOpOperationsOpRemove:
		return true
	case ScimV2PatchOpOperationsOpReplace:
		return true
	default:
		return false
	}
}

const (
	ScimV2PatchOpApiVersionScimv2 ScimV2PatchOpApiVersion = "scim/v2"
)

func (e ScimV2PatchOpApiVersion) Valid() bool {
	switch e {
	case ScimV2PatchOpApiVersionScimv2:
		return true
	default:
		return false
	}
}

const (
	ScimV2PatchOpKindPatchOp ScimV2PatchOpKind = "PatchOp"
)

func (e ScimV2PatchOpKind) Valid() bool {
	switch e {
	case ScimV2PatchOpKindPatchOp:
		return true
	default:
		return false
	}
}

const (
	PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpAdd     PatchScimV2UserApplicationScimPlusJSONBodyOperationsOp = "add"
	PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpRemove  PatchScimV2UserApplicationScimPlusJSONBodyOperationsOp = "remove"
	PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpReplace PatchScimV2UserApplicationScimPlusJSONBodyOperationsOp = "replace"
)

func (e PatchScimV2UserApplicationScimPlusJSONBodyOperationsOp) Valid() bool {
	switch e {
	case PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpAdd:
		return true
	case PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpRemove:
		return true
	case PatchScimV2UserApplicationScimPlusJSONBodyOperationsOpReplace:
		return true
	default:
		return false
	}
}

const (
	PatchScimV2UserApplicationScimPlusJSONBodyApiVersionScimv2 PatchScimV2UserApplicationScimPlusJSONBodyApiVersion = "scim/v2"
)

func (e PatchScimV2UserApplicationScimPlusJSONBodyApiVersion) Valid() bool {
	switch e {
	case PatchScimV2UserApplicationScimPlusJSONBodyApiVersionScimv2:
		return true
	default:
		return false
	}
}

const (
	PatchScimV2UserApplicationScimPlusJSONBodyKindPatchOp PatchScimV2UserApplicationScimPlusJSONBodyKind = "PatchOp"
)

func (e PatchScimV2UserApplicationScimPlusJSONBodyKind) Valid() bool {
	switch e {
	case PatchScimV2UserApplicationScimPlusJSONBodyKindPatchOp:
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
type ScimV2Error struct {
	// Detail A human-readable description of the error.
	Detail *string `json:"detail,omitempty"`

	// Schemas The SCIM schema for the error response.
	Schemas *[]string `json:"schemas,omitempty"`

	// Status The HTTP status code of the error response.
	Status *string `json:"status,omitempty"`
}
type ScimV2Meta struct {
	// Created The DateTime the Resource was added to Confluent Cloud.
	Created *time.Time `json:"created,omitempty"`

	// LastModified The most recent DateTime that the details of this Resource were updated.
	LastModified *time.Time `json:"lastModified,omitempty"`

	// Location The URI of the SCIM resource being returned.
	Location *string `json:"location,omitempty"`

	// ResourceType The name of the resource type of the resource.
	ResourceType *string `json:"resourceType,omitempty"`
}
type ScimV2Name struct {
	// FamilyName The user's last name.
	FamilyName *string `json:"familyName,omitempty"`

	// GivenName The user's first name.
	GivenName *string `json:"givenName,omitempty"`
}
type ScimV2PatchOp struct {
	// Operations A list of patch operations.
	Operations *[]struct {
		// Op The operation to perform.
		Op ScimV2PatchOpOperationsOp `json:"op"`

		// Path The attribute path describing the target of the operation.
		Path *string `json:"path,omitempty"`

		// Value The value to be used within the operation.
		Value *ScimV2PatchOp_Operations_Value `json:"value,omitempty"`
	} `json:"Operations,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ScimV2PatchOpApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ScimV2PatchOpKind `json:"kind,omitempty"`
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

	// Schemas The SCIM schema.
	Schemas *[]string `json:"schemas,omitempty"`
}
type ScimV2PatchOpOperationsOp string
type ScimV2PatchOpOperationsValue0 = string
type ScimV2PatchOpOperationsValue1 = bool
type ScimV2PatchOpOperationsValue2 = map[string]interface{}
type ScimV2PatchOpOperationsValue3 = []interface{}
type ScimV2PatchOp_Operations_Value struct {
	union json.RawMessage
}
type ScimV2PatchOpApiVersion string
type ScimV2PatchOpKind string
type ScimV2User struct {
	// Active Indicates whether the user account is active and can access Confluent Cloud resources.
	Active *bool `json:"active,omitempty"`

	// Id Unique identifier for the User. Automatically assigned by Confluent Cloud.
	Id *string `json:"id,omitempty"`

	// Meta Metadata about the resource.
	Meta *ScimV2Meta `json:"meta,omitempty"`

	// Name The user's full name.
	Name *ScimV2Name `json:"name,omitempty"`

	// Schemas The SCIM schema.
	Schemas *[]string `json:"schemas,omitempty"`

	// UserName The user's email address, which serves as their unique identifier in Confluent Cloud.
	UserName *string `json:"userName,omitempty"`
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
func (t ScimV2PatchOp_Operations_Value) AsScimV2PatchOpOperationsValue0() (ScimV2PatchOpOperationsValue0, error) {
	var body ScimV2PatchOpOperationsValue0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ScimV2PatchOp_Operations_Value) FromScimV2PatchOpOperationsValue0(v ScimV2PatchOpOperationsValue0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *ScimV2PatchOp_Operations_Value) MergeScimV2PatchOpOperationsValue0(v ScimV2PatchOpOperationsValue0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ScimV2PatchOp_Operations_Value) AsScimV2PatchOpOperationsValue1() (ScimV2PatchOpOperationsValue1, error) {
	var body ScimV2PatchOpOperationsValue1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ScimV2PatchOp_Operations_Value) FromScimV2PatchOpOperationsValue1(v ScimV2PatchOpOperationsValue1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *ScimV2PatchOp_Operations_Value) MergeScimV2PatchOpOperationsValue1(v ScimV2PatchOpOperationsValue1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ScimV2PatchOp_Operations_Value) AsScimV2PatchOpOperationsValue2() (ScimV2PatchOpOperationsValue2, error) {
	var body ScimV2PatchOpOperationsValue2
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ScimV2PatchOp_Operations_Value) FromScimV2PatchOpOperationsValue2(v ScimV2PatchOpOperationsValue2) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *ScimV2PatchOp_Operations_Value) MergeScimV2PatchOpOperationsValue2(v ScimV2PatchOpOperationsValue2) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ScimV2PatchOp_Operations_Value) AsScimV2PatchOpOperationsValue3() (ScimV2PatchOpOperationsValue3, error) {
	var body ScimV2PatchOpOperationsValue3
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ScimV2PatchOp_Operations_Value) FromScimV2PatchOpOperationsValue3(v ScimV2PatchOpOperationsValue3) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *ScimV2PatchOp_Operations_Value) MergeScimV2PatchOpOperationsValue3(v ScimV2PatchOpOperationsValue3) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ScimV2PatchOp_Operations_Value) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ScimV2PatchOp_Operations_Value) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) AsPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0() (PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0, error) {
	var body PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) FromPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) MergePatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) AsPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1() (PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1, error) {
	var body PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) FromPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) MergePatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) AsPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2() (PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2, error) {
	var body PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) FromPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) MergePatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue2) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) AsPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3() (PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3, error) {
	var body PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) FromPatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) MergePatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3(v PatchScimV2UserApplicationScimPlusJSONBodyOperationsValue3) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PatchScimV2UserApplicationScimPlusJSONBody_Operations_Value) UnmarshalJSON(b []byte) error {
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

	// FindScimV2User Search for Users
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to find a user.
	//
	// Search for users in the organization using SCIM filter expressions. This endpoint supports
	// filtering by user attributes such as userName to locate existing users or verify if a user
	// account already exists before provisioning.
	//
	// Corresponds with GET /scim/v2/sso/{connection_name}/Users (the `FindScimV2User` operationId).
	FindScimV2User(ctx context.Context, connectionName string, params *FindScimV2UserParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateScimV2UserWithBody Create a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a user.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /scim/v2/sso/{connection_name}/Users (the `CreateScimV2User` operationId).
	CreateScimV2UserWithBody(ctx context.Context, connectionName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateScimV2UserWithApplicationScimPlusJSONBody Create a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a user.
	//
	// Takes a body of the `application/scim+json` content type.
	//
	// Corresponds with POST /scim/v2/sso/{connection_name}/Users (the `CreateScimV2User` operationId).
	CreateScimV2UserWithApplicationScimPlusJSONBody(ctx context.Context, connectionName string, body CreateScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteScimV2User Delete a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a user.
	//
	// Permanently removes the user from the organization. This operation will also cascade delete
	// all of the user's associated resources, including API keys and any other user-specific configurations.
	// This action cannot be undone.
	//
	// Corresponds with DELETE /scim/v2/sso/{connection_name}/Users/{id} (the `DeleteScimV2User` operationId).
	DeleteScimV2User(ctx context.Context, connectionName string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetScimV2User Read a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a user.
	//
	// Corresponds with GET /scim/v2/sso/{connection_name}/Users/{id} (the `GetScimV2User` operationId).
	GetScimV2User(ctx context.Context, connectionName string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PatchScimV2UserWithBody Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2UserWithBody(ctx context.Context, connectionName string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PatchScimV2User Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2User(ctx context.Context, connectionName string, id string, body PatchScimV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PatchScimV2UserWithApplicationScimPlusJSONBody Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes a body of the `application/scim+json` content type.
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2UserWithApplicationScimPlusJSONBody(ctx context.Context, connectionName string, id string, body PatchScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateScimV2UserWithBody Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /scim/v2/sso/{connection_name}/Users/{id} (the `UpdateScimV2User` operationId).
	UpdateScimV2UserWithBody(ctx context.Context, connectionName string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateScimV2UserWithApplicationScimPlusJSONBody Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes a body of the `application/scim+json` content type.
	//
	// Corresponds with PUT /scim/v2/sso/{connection_name}/Users/{id} (the `UpdateScimV2User` operationId).
	UpdateScimV2UserWithApplicationScimPlusJSONBody(ctx context.Context, connectionName string, id string, body UpdateScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func NewCreateScimV2UserRequestWithApplicationScimPlusJSONBody(server string, connectionName string, body CreateScimV2UserApplicationScimPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewCreateScimV2UserRequestWithBody(server, connectionName, "application/scim+json", bodyReader)
}
func NewPatchScimV2UserRequestWithApplicationScimPlusJSONBody(server string, connectionName string, id string, body PatchScimV2UserApplicationScimPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPatchScimV2UserRequestWithBody(server, connectionName, id, "application/scim+json", bodyReader)
}
func NewUpdateScimV2UserRequestWithApplicationScimPlusJSONBody(server string, connectionName string, id string, body UpdateScimV2UserApplicationScimPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateScimV2UserRequestWithBody(server, connectionName, id, "application/scim+json", bodyReader)
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

	// FindScimV2UserWithResponse Search for Users
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to find a user.
	//
	// Search for users in the organization using SCIM filter expressions. This endpoint supports
	// filtering by user attributes such as userName to locate existing users or verify if a user
	// account already exists before provisioning.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /scim/v2/sso/{connection_name}/Users (the `FindScimV2User` operationId).
	FindScimV2UserWithResponse(ctx context.Context, connectionName string, params *FindScimV2UserParams, reqEditors ...RequestEditorFn) (*FindScimV2UserResponse, error)

	// CreateScimV2UserWithBodyWithResponse Create a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a user.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /scim/v2/sso/{connection_name}/Users (the `CreateScimV2User` operationId).
	CreateScimV2UserWithBodyWithResponse(ctx context.Context, connectionName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateScimV2UserResponse, error)

	// CreateScimV2UserWithApplicationScimPlusJSONBodyWithResponse Create a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a user.
	//
	// Takes a body of the `application/scim+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /scim/v2/sso/{connection_name}/Users (the `CreateScimV2User` operationId).
	CreateScimV2UserWithApplicationScimPlusJSONBodyWithResponse(ctx context.Context, connectionName string, body CreateScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateScimV2UserResponse, error)

	// DeleteScimV2UserWithResponse Delete a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a user.
	//
	// Permanently removes the user from the organization. This operation will also cascade delete
	// all of the user's associated resources, including API keys and any other user-specific configurations.
	// This action cannot be undone.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /scim/v2/sso/{connection_name}/Users/{id} (the `DeleteScimV2User` operationId).
	DeleteScimV2UserWithResponse(ctx context.Context, connectionName string, id string, reqEditors ...RequestEditorFn) (*DeleteScimV2UserResponse, error)

	// GetScimV2UserWithResponse Read a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a user.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /scim/v2/sso/{connection_name}/Users/{id} (the `GetScimV2User` operationId).
	GetScimV2UserWithResponse(ctx context.Context, connectionName string, id string, reqEditors ...RequestEditorFn) (*GetScimV2UserResponse, error)

	// PatchScimV2UserWithBodyWithResponse Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2UserWithBodyWithResponse(ctx context.Context, connectionName string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PatchScimV2UserResponse, error)

	// PatchScimV2UserWithResponse Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2UserWithResponse(ctx context.Context, connectionName string, id string, body PatchScimV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*PatchScimV2UserResponse, error)

	// PatchScimV2UserWithApplicationScimPlusJSONBodyWithResponse Patch a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a user.
	//
	// Takes a body of the `application/scim+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /scim/v2/sso/{connection_name}/Users/{id} (the `PatchScimV2User` operationId).
	PatchScimV2UserWithApplicationScimPlusJSONBodyWithResponse(ctx context.Context, connectionName string, id string, body PatchScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*PatchScimV2UserResponse, error)

	// UpdateScimV2UserWithBodyWithResponse Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /scim/v2/sso/{connection_name}/Users/{id} (the `UpdateScimV2User` operationId).
	UpdateScimV2UserWithBodyWithResponse(ctx context.Context, connectionName string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateScimV2UserResponse, error)

	// UpdateScimV2UserWithApplicationScimPlusJSONBodyWithResponse Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes a body of the `application/scim+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /scim/v2/sso/{connection_name}/Users/{id} (the `UpdateScimV2User` operationId).
	UpdateScimV2UserWithApplicationScimPlusJSONBodyWithResponse(ctx context.Context, connectionName string, id string, body UpdateScimV2UserApplicationScimPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateScimV2UserResponse, error)
}

func (r FindScimV2UserResponse) GetApplicationscimJSON200() *struct {
	// Resources A multi-valued list of complex objects containing the requested resources.
	Resources *[]ScimV2User `json:"Resources,omitempty"`

	// ItemsPerPage The number of resources returned in a list response page.
	ItemsPerPage *int32 `json:"itemsPerPage,omitempty"`

	// Schemas The SCIM schema.
	Schemas *[]string `json:"schemas,omitempty"`

	// StartIndex The 1-based index of the first result in the current set of list results.
	StartIndex *int32 `json:"startIndex,omitempty"`

	// TotalResults The total number of results returned by the list or query operation.
	TotalResults *int32 `json:"totalResults,omitempty"`
} {
	return r.ApplicationscimJSON200
}
func (r FindScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r FindScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r FindScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r FindScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r FindScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r FindScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r FindScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r FindScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r FindScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON201() *ScimV2User {
	return r.ApplicationscimJSON201
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON402() *ScimV2Error {
	return r.ApplicationscimJSON402
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON409() *ScimV2Error {
	return r.ApplicationscimJSON409
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r CreateScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r CreateScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r CreateScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON404() *ScimV2Error {
	return r.ApplicationscimJSON404
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r DeleteScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r DeleteScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetScimV2UserResponse) GetApplicationscimJSON200() *ScimV2User {
	return r.ApplicationscimJSON200
}
func (r GetScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r GetScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r GetScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r GetScimV2UserResponse) GetApplicationscimJSON404() *ScimV2Error {
	return r.ApplicationscimJSON404
}
func (r GetScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r GetScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r GetScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r GetScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON200() *ScimV2User {
	return r.ApplicationscimJSON200
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON404() *ScimV2Error {
	return r.ApplicationscimJSON404
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r PatchScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r PatchScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r PatchScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PatchScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PatchScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON200() *ScimV2User {
	return r.ApplicationscimJSON200
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON400() *ScimV2Error {
	return r.ApplicationscimJSON400
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON401() *ScimV2Error {
	return r.ApplicationscimJSON401
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON403() *ScimV2Error {
	return r.ApplicationscimJSON403
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON404() *ScimV2Error {
	return r.ApplicationscimJSON404
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON429() *ScimV2Error {
	return r.ApplicationscimJSON429
}
func (r UpdateScimV2UserResponse) GetApplicationscimJSON500() *ScimV2Error {
	return r.ApplicationscimJSON500
}
func (r UpdateScimV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateScimV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateScimV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateScimV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
