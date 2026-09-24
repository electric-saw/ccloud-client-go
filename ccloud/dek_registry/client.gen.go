package dek_registry

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
	CreateDekRequestAlgorithmAES128GCM CreateDekRequestAlgorithm = "AES128_GCM"
	CreateDekRequestAlgorithmAES256GCM CreateDekRequestAlgorithm = "AES256_GCM"
	CreateDekRequestAlgorithmAES256SIV CreateDekRequestAlgorithm = "AES256_SIV"
)

func (e CreateDekRequestAlgorithm) Valid() bool {
	switch e {
	case CreateDekRequestAlgorithmAES128GCM:
		return true
	case CreateDekRequestAlgorithmAES256GCM:
		return true
	case CreateDekRequestAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	DekAlgorithmAES128GCM DekAlgorithm = "AES128_GCM"
	DekAlgorithmAES256GCM DekAlgorithm = "AES256_GCM"
	DekAlgorithmAES256SIV DekAlgorithm = "AES256_SIV"
)

func (e DekAlgorithm) Valid() bool {
	switch e {
	case DekAlgorithmAES128GCM:
		return true
	case DekAlgorithmAES256GCM:
		return true
	case DekAlgorithmAES256SIV:
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
	DeleteDekVersionsParamsAlgorithmAES128GCM DeleteDekVersionsParamsAlgorithm = "AES128_GCM"
	DeleteDekVersionsParamsAlgorithmAES256GCM DeleteDekVersionsParamsAlgorithm = "AES256_GCM"
	DeleteDekVersionsParamsAlgorithmAES256SIV DeleteDekVersionsParamsAlgorithm = "AES256_SIV"
)

func (e DeleteDekVersionsParamsAlgorithm) Valid() bool {
	switch e {
	case DeleteDekVersionsParamsAlgorithmAES128GCM:
		return true
	case DeleteDekVersionsParamsAlgorithmAES256GCM:
		return true
	case DeleteDekVersionsParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	GetDekParamsAlgorithmAES128GCM GetDekParamsAlgorithm = "AES128_GCM"
	GetDekParamsAlgorithmAES256GCM GetDekParamsAlgorithm = "AES256_GCM"
	GetDekParamsAlgorithmAES256SIV GetDekParamsAlgorithm = "AES256_SIV"
)

func (e GetDekParamsAlgorithm) Valid() bool {
	switch e {
	case GetDekParamsAlgorithmAES128GCM:
		return true
	case GetDekParamsAlgorithmAES256GCM:
		return true
	case GetDekParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	UndeleteDekVersionsParamsAlgorithmAES128GCM UndeleteDekVersionsParamsAlgorithm = "AES128_GCM"
	UndeleteDekVersionsParamsAlgorithmAES256GCM UndeleteDekVersionsParamsAlgorithm = "AES256_GCM"
	UndeleteDekVersionsParamsAlgorithmAES256SIV UndeleteDekVersionsParamsAlgorithm = "AES256_SIV"
)

func (e UndeleteDekVersionsParamsAlgorithm) Valid() bool {
	switch e {
	case UndeleteDekVersionsParamsAlgorithmAES128GCM:
		return true
	case UndeleteDekVersionsParamsAlgorithmAES256GCM:
		return true
	case UndeleteDekVersionsParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	GetDekVersionsParamsAlgorithmAES128GCM GetDekVersionsParamsAlgorithm = "AES128_GCM"
	GetDekVersionsParamsAlgorithmAES256GCM GetDekVersionsParamsAlgorithm = "AES256_GCM"
	GetDekVersionsParamsAlgorithmAES256SIV GetDekVersionsParamsAlgorithm = "AES256_SIV"
)

func (e GetDekVersionsParamsAlgorithm) Valid() bool {
	switch e {
	case GetDekVersionsParamsAlgorithmAES128GCM:
		return true
	case GetDekVersionsParamsAlgorithmAES256GCM:
		return true
	case GetDekVersionsParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	DeleteDekVersionParamsAlgorithmAES128GCM DeleteDekVersionParamsAlgorithm = "AES128_GCM"
	DeleteDekVersionParamsAlgorithmAES256GCM DeleteDekVersionParamsAlgorithm = "AES256_GCM"
	DeleteDekVersionParamsAlgorithmAES256SIV DeleteDekVersionParamsAlgorithm = "AES256_SIV"
)

func (e DeleteDekVersionParamsAlgorithm) Valid() bool {
	switch e {
	case DeleteDekVersionParamsAlgorithmAES128GCM:
		return true
	case DeleteDekVersionParamsAlgorithmAES256GCM:
		return true
	case DeleteDekVersionParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	GetDekByVersionParamsAlgorithmAES128GCM GetDekByVersionParamsAlgorithm = "AES128_GCM"
	GetDekByVersionParamsAlgorithmAES256GCM GetDekByVersionParamsAlgorithm = "AES256_GCM"
	GetDekByVersionParamsAlgorithmAES256SIV GetDekByVersionParamsAlgorithm = "AES256_SIV"
)

func (e GetDekByVersionParamsAlgorithm) Valid() bool {
	switch e {
	case GetDekByVersionParamsAlgorithmAES128GCM:
		return true
	case GetDekByVersionParamsAlgorithmAES256GCM:
		return true
	case GetDekByVersionParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

const (
	UndeleteDekVersionParamsAlgorithmAES128GCM UndeleteDekVersionParamsAlgorithm = "AES128_GCM"
	UndeleteDekVersionParamsAlgorithmAES256GCM UndeleteDekVersionParamsAlgorithm = "AES256_GCM"
	UndeleteDekVersionParamsAlgorithmAES256SIV UndeleteDekVersionParamsAlgorithm = "AES256_SIV"
)

func (e UndeleteDekVersionParamsAlgorithm) Valid() bool {
	switch e {
	case UndeleteDekVersionParamsAlgorithmAES128GCM:
		return true
	case UndeleteDekVersionParamsAlgorithmAES256GCM:
		return true
	case UndeleteDekVersionParamsAlgorithmAES256SIV:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type CreateDekRequest struct {
	// Algorithm Algorithm of the dek
	Algorithm *CreateDekRequestAlgorithm `json:"algorithm,omitempty"`

	// Deleted Whether the dek is deleted
	Deleted *bool `json:"deleted,omitempty"`

	// EncryptedKeyMaterial Encrypted key material of the dek
	EncryptedKeyMaterial *string `json:"encryptedKeyMaterial,omitempty"`

	// Subject Subject of the dek
	Subject *string `json:"subject,omitempty"`

	// Version Version of the dek
	Version *int32 `json:"version,omitempty"`
}
type CreateDekRequestAlgorithm string
type CreateKekRequest struct {
	// Deleted Whether the kek is deleted
	Deleted *bool `json:"deleted,omitempty"`

	// Doc Description of the kek
	Doc *string `json:"doc,omitempty"`

	// KmsKeyId KMS key ID of the kek
	KmsKeyId *string `json:"kmsKeyId,omitempty"`

	// KmsProps Properties of the kek
	KmsProps *map[string]string `json:"kmsProps,omitempty"`

	// KmsType KMS type of the kek
	KmsType *string `json:"kmsType,omitempty"`

	// Name Name of the kek
	Name *string `json:"name,omitempty"`

	// Shared Whether the kek is shared
	Shared *bool `json:"shared,omitempty"`
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
type Dek struct {
	// Algorithm Algorithm of the dek
	Algorithm *DekAlgorithm `json:"algorithm,omitempty"`

	// Deleted Whether the dek is deleted
	Deleted *bool `json:"deleted,omitempty"`

	// EncryptedKeyMaterial Encrypted key material of the dek
	EncryptedKeyMaterial *string `json:"encryptedKeyMaterial,omitempty"`

	// KekName Kek name of the dek
	KekName *string `json:"kekName,omitempty"`

	// KeyMaterial Raw key material of the dek
	KeyMaterial *string `json:"keyMaterial,omitempty"`

	// Subject Subject of the dek
	Subject *string `json:"subject,omitempty"`

	// Ts Timestamp of the dek
	Ts *int64 `json:"ts,omitempty"`

	// Version Version of the dek
	Version *int32 `json:"version,omitempty"`
}
type DekAlgorithm string
type Kek struct {
	// Deleted Whether the kek is deleted
	Deleted *bool `json:"deleted,omitempty"`

	// Doc Description of the kek
	Doc *string `json:"doc,omitempty"`

	// KmsKeyId KMS key ID of the kek
	KmsKeyId *string `json:"kmsKeyId,omitempty"`

	// KmsProps Properties of the kek
	KmsProps *map[string]string `json:"kmsProps,omitempty"`

	// KmsType KMS type of the kek
	KmsType *string `json:"kmsType,omitempty"`

	// Name Name of the kek
	Name *string `json:"name,omitempty"`

	// Shared Whether the kek is shared
	Shared *bool `json:"shared,omitempty"`

	// Ts Timestamp of the kek
	Ts *int64 `json:"ts,omitempty"`
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
type UpdateKekRequest struct {
	// Doc Description of the kek
	Doc *string `json:"doc,omitempty"`

	// KmsProps Properties of the kek
	KmsProps *map[string]string `json:"kmsProps,omitempty"`

	// Shared Whether the kek is shared
	Shared *bool `json:"shared,omitempty"`
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

	// GetKekNames Get a list of kek names
	//
	// Corresponds with GET /dek-registry/v1/keks (the `GetKekNames` operationId).
	GetKekNames(ctx context.Context, params *GetKekNamesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKekWithBody Create a kek
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithBody(ctx context.Context, params *CreateKekParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKek Create a kek
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKek(ctx context.Context, params *CreateKekParams, body CreateKekJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKekWithApplicationVndSchemaregistryPlusJSONBody Create a kek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKekWithApplicationVndSchemaregistryV1PlusJSONBody Create a kek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKek Delete a kek
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name} (the `DeleteKek` operationId).
	DeleteKek(ctx context.Context, name string, params *DeleteKekParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKek Get a kek by name
	//
	// Corresponds with GET /dek-registry/v1/keks/{name} (the `GetKek` operationId).
	GetKek(ctx context.Context, name string, params *GetKekParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PutKekWithBody Alters a kek
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithBody(ctx context.Context, name string, params *PutKekParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PutKek Alters a kek
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKek(ctx context.Context, name string, params *PutKekParams, body PutKekJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PutKekWithApplicationVndSchemaregistryPlusJSONBody Alters a kek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PutKekWithApplicationVndSchemaregistryV1PlusJSONBody Alters a kek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetDekSubjects Get a list of dek subjects
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks (the `GetDekSubjects` operationId).
	GetDekSubjects(ctx context.Context, name string, params *GetDekSubjectsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateDekWithBody Create a dek
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithBody(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateDek Create a dek
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDek(ctx context.Context, name string, body CreateDekJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateDekWithApplicationVndSchemaregistryPlusJSONBody Create a dek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, name string, body CreateDekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateDekWithApplicationVndSchemaregistryV1PlusJSONBody Create a dek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, name string, body CreateDekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteDekVersions Delete all versions of a dek
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name}/deks/{subject} (the `DeleteDekVersions` operationId).
	DeleteDekVersions(ctx context.Context, name string, subject string, params *DeleteDekVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetDek Get a dek by subject
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject} (the `GetDek` operationId).
	GetDek(ctx context.Context, name string, subject string, params *GetDekParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UndeleteDekVersions Undelete all versions of a dek
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks/{subject}/undelete (the `UndeleteDekVersions` operationId).
	UndeleteDekVersions(ctx context.Context, name string, subject string, params *UndeleteDekVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetDekVersions List versions of dek
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject}/versions (the `GetDekVersions` operationId).
	GetDekVersions(ctx context.Context, name string, subject string, params *GetDekVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteDekVersion Delete a dek version
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version} (the `DeleteDekVersion` operationId).
	DeleteDekVersion(ctx context.Context, name string, subject string, version string, params *DeleteDekVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetDekByVersion Get a dek by subject and version
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version} (the `GetDekByVersion` operationId).
	GetDekByVersion(ctx context.Context, name string, subject string, version string, params *GetDekByVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UndeleteDekVersion Undelete a dek version
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}/undelete (the `UndeleteDekVersion` operationId).
	UndeleteDekVersion(ctx context.Context, name string, subject string, version string, params *UndeleteDekVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// TestKek Test a kek
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/test (the `TestKek` operationId).
	TestKek(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UndeleteKek Undelete a kek
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/undelete (the `UndeleteKek` operationId).
	UndeleteKek(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func NewCreateKekRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewCreateKekRequestWithBody(server, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewCreateKekRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewCreateKekRequestWithBody(server, params, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewPutKekRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPutKekRequestWithBody(server, name, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewPutKekRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPutKekRequestWithBody(server, name, params, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewCreateDekRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, name string, body CreateDekApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewCreateDekRequestWithBody(server, name, "application/vnd.schemaregistry+json", bodyReader)
}
func NewCreateDekRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, name string, body CreateDekApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewCreateDekRequestWithBody(server, name, "application/vnd.schemaregistry.v1+json", bodyReader)
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

	// GetKekNamesWithResponse Get a list of kek names
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks (the `GetKekNames` operationId).
	GetKekNamesWithResponse(ctx context.Context, params *GetKekNamesParams, reqEditors ...RequestEditorFn) (*GetKekNamesResponse, error)

	// CreateKekWithBodyWithResponse Create a kek
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithBodyWithResponse(ctx context.Context, params *CreateKekParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKekResponse, error)

	// CreateKekWithResponse Create a kek
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithResponse(ctx context.Context, params *CreateKekParams, body CreateKekJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKekResponse, error)

	// CreateKekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Create a kek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKekResponse, error)

	// CreateKekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Create a kek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks (the `CreateKek` operationId).
	CreateKekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, params *CreateKekParams, body CreateKekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKekResponse, error)

	// DeleteKekWithResponse Delete a kek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name} (the `DeleteKek` operationId).
	DeleteKekWithResponse(ctx context.Context, name string, params *DeleteKekParams, reqEditors ...RequestEditorFn) (*DeleteKekResponse, error)

	// GetKekWithResponse Get a kek by name
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks/{name} (the `GetKek` operationId).
	GetKekWithResponse(ctx context.Context, name string, params *GetKekParams, reqEditors ...RequestEditorFn) (*GetKekResponse, error)

	// PutKekWithBodyWithResponse Alters a kek
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithBodyWithResponse(ctx context.Context, name string, params *PutKekParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PutKekResponse, error)

	// PutKekWithResponse Alters a kek
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithResponse(ctx context.Context, name string, params *PutKekParams, body PutKekJSONRequestBody, reqEditors ...RequestEditorFn) (*PutKekResponse, error)

	// PutKekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Alters a kek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*PutKekResponse, error)

	// PutKekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Alters a kek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /dek-registry/v1/keks/{name} (the `PutKek` operationId).
	PutKekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, name string, params *PutKekParams, body PutKekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*PutKekResponse, error)

	// GetDekSubjectsWithResponse Get a list of dek subjects
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks (the `GetDekSubjects` operationId).
	GetDekSubjectsWithResponse(ctx context.Context, name string, params *GetDekSubjectsParams, reqEditors ...RequestEditorFn) (*GetDekSubjectsResponse, error)

	// CreateDekWithBodyWithResponse Create a dek
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithBodyWithResponse(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateDekResponse, error)

	// CreateDekWithResponse Create a dek
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithResponse(ctx context.Context, name string, body CreateDekJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateDekResponse, error)

	// CreateDekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Create a dek
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, name string, body CreateDekApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateDekResponse, error)

	// CreateDekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Create a dek
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks (the `CreateDek` operationId).
	CreateDekWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, name string, body CreateDekApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateDekResponse, error)

	// DeleteDekVersionsWithResponse Delete all versions of a dek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name}/deks/{subject} (the `DeleteDekVersions` operationId).
	DeleteDekVersionsWithResponse(ctx context.Context, name string, subject string, params *DeleteDekVersionsParams, reqEditors ...RequestEditorFn) (*DeleteDekVersionsResponse, error)

	// GetDekWithResponse Get a dek by subject
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject} (the `GetDek` operationId).
	GetDekWithResponse(ctx context.Context, name string, subject string, params *GetDekParams, reqEditors ...RequestEditorFn) (*GetDekResponse, error)

	// UndeleteDekVersionsWithResponse Undelete all versions of a dek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks/{subject}/undelete (the `UndeleteDekVersions` operationId).
	UndeleteDekVersionsWithResponse(ctx context.Context, name string, subject string, params *UndeleteDekVersionsParams, reqEditors ...RequestEditorFn) (*UndeleteDekVersionsResponse, error)

	// GetDekVersionsWithResponse List versions of dek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject}/versions (the `GetDekVersions` operationId).
	GetDekVersionsWithResponse(ctx context.Context, name string, subject string, params *GetDekVersionsParams, reqEditors ...RequestEditorFn) (*GetDekVersionsResponse, error)

	// DeleteDekVersionWithResponse Delete a dek version
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version} (the `DeleteDekVersion` operationId).
	DeleteDekVersionWithResponse(ctx context.Context, name string, subject string, version string, params *DeleteDekVersionParams, reqEditors ...RequestEditorFn) (*DeleteDekVersionResponse, error)

	// GetDekByVersionWithResponse Get a dek by subject and version
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version} (the `GetDekByVersion` operationId).
	GetDekByVersionWithResponse(ctx context.Context, name string, subject string, version string, params *GetDekByVersionParams, reqEditors ...RequestEditorFn) (*GetDekByVersionResponse, error)

	// UndeleteDekVersionWithResponse Undelete a dek version
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}/undelete (the `UndeleteDekVersion` operationId).
	UndeleteDekVersionWithResponse(ctx context.Context, name string, subject string, version string, params *UndeleteDekVersionParams, reqEditors ...RequestEditorFn) (*UndeleteDekVersionResponse, error)

	// TestKekWithResponse Test a kek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/test (the `TestKek` operationId).
	TestKekWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*TestKekResponse, error)

	// UndeleteKekWithResponse Undelete a kek
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /dek-registry/v1/keks/{name}/undelete (the `UndeleteKek` operationId).
	UndeleteKekWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*UndeleteKekResponse, error)
}

func (r GetKekNamesResponse) GetApplicationjsonQs05200() *[]string {
	return r.ApplicationjsonQs05200
}
func (r GetKekNamesResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetKekNamesResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetKekNamesResponse) GetBody() []byte {
	return r.Body
}
func (r GetKekNamesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKekNamesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKekNamesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKekResponse) GetApplicationjsonQs05200() *Kek {
	return r.ApplicationjsonQs05200
}
func (r CreateKekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Kek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r CreateKekResponse) GetApplicationvndSchemaregistryV1JSON200() *Kek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r CreateKekResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKekResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKekResponse) GetApplicationjsonQs05200() *Kek {
	return r.ApplicationjsonQs05200
}
func (r GetKekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Kek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetKekResponse) GetApplicationvndSchemaregistryV1JSON200() *Kek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetKekResponse) GetBody() []byte {
	return r.Body
}
func (r GetKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r PutKekResponse) GetApplicationjsonQs05200() *Kek {
	return r.ApplicationjsonQs05200
}
func (r PutKekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Kek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r PutKekResponse) GetApplicationvndSchemaregistryV1JSON200() *Kek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r PutKekResponse) GetBody() []byte {
	return r.Body
}
func (r PutKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PutKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PutKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetDekSubjectsResponse) GetApplicationjsonQs05200() *[]string {
	return r.ApplicationjsonQs05200
}
func (r GetDekSubjectsResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetDekSubjectsResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetDekSubjectsResponse) GetBody() []byte {
	return r.Body
}
func (r GetDekSubjectsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetDekSubjectsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetDekSubjectsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateDekResponse) GetApplicationjsonQs05200() *Dek {
	return r.ApplicationjsonQs05200
}
func (r CreateDekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Dek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r CreateDekResponse) GetApplicationvndSchemaregistryV1JSON200() *Dek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r CreateDekResponse) GetBody() []byte {
	return r.Body
}
func (r CreateDekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateDekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateDekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteDekVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteDekVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteDekVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteDekVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetDekResponse) GetApplicationjsonQs05200() *Dek {
	return r.ApplicationjsonQs05200
}
func (r GetDekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Dek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetDekResponse) GetApplicationvndSchemaregistryV1JSON200() *Dek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetDekResponse) GetBody() []byte {
	return r.Body
}
func (r GetDekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetDekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetDekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UndeleteDekVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r UndeleteDekVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UndeleteDekVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UndeleteDekVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetDekVersionsResponse) GetApplicationjsonQs05200() *[]int32 {
	return r.ApplicationjsonQs05200
}
func (r GetDekVersionsResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]int32 {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetDekVersionsResponse) GetApplicationvndSchemaregistryV1JSON200() *[]int32 {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetDekVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r GetDekVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetDekVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetDekVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteDekVersionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteDekVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteDekVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteDekVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetDekByVersionResponse) GetApplicationjsonQs05200() *Dek {
	return r.ApplicationjsonQs05200
}
func (r GetDekByVersionResponse) GetApplicationvndSchemaregistryJSONQs09200() *Dek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetDekByVersionResponse) GetApplicationvndSchemaregistryV1JSON200() *Dek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetDekByVersionResponse) GetBody() []byte {
	return r.Body
}
func (r GetDekByVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetDekByVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetDekByVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UndeleteDekVersionResponse) GetBody() []byte {
	return r.Body
}
func (r UndeleteDekVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UndeleteDekVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UndeleteDekVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r TestKekResponse) GetApplicationjsonQs05200() *Kek {
	return r.ApplicationjsonQs05200
}
func (r TestKekResponse) GetApplicationvndSchemaregistryJSONQs09200() *Kek {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r TestKekResponse) GetApplicationvndSchemaregistryV1JSON200() *Kek {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r TestKekResponse) GetBody() []byte {
	return r.Body
}
func (r TestKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r TestKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r TestKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UndeleteKekResponse) GetBody() []byte {
	return r.Body
}
func (r UndeleteKekResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UndeleteKekResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UndeleteKekResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
