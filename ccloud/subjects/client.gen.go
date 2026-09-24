package subjects

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
	CONDITION RuleKind = "CONDITION"
	TRANSFORM RuleKind = "TRANSFORM"
)

func (e RuleKind) Valid() bool {
	switch e {
	case CONDITION:
		return true
	case TRANSFORM:
		return true
	default:
		return false
	}
}

const (
	DOWNGRADE RuleMode = "DOWNGRADE"
	READ      RuleMode = "READ"
	UPDOWN    RuleMode = "UPDOWN"
	UPGRADE   RuleMode = "UPGRADE"
	WRITE     RuleMode = "WRITE"
	WRITEREAD RuleMode = "WRITEREAD"
)

func (e RuleMode) Valid() bool {
	switch e {
	case DOWNGRADE:
		return true
	case READ:
		return true
	case UPDOWN:
		return true
	case UPGRADE:
		return true
	case WRITE:
		return true
	case WRITEREAD:
		return true
	default:
		return false
	}
}

const (
	SrField  SchemaEntityEntityType = "sr_field"
	SrRecord SchemaEntityEntityType = "sr_record"
)

func (e SchemaEntityEntityType) Valid() bool {
	switch e {
	case SrField:
		return true
	case SrRecord:
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
type ErrorMessage struct {
	// ErrorCode The error code
	ErrorCode *int32 `json:"error_code,omitempty"`

	// Message The error message
	Message *string `json:"message,omitempty"`
}
type Metadata struct {
	Properties *map[string]string   `json:"properties,omitempty"`
	Sensitive  *[]string            `json:"sensitive,omitempty"`
	Tags       *map[string][]string `json:"tags,omitempty"`
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
type RegisterSchemaRequest struct {
	// Id Globally unique identifier of the schema
	Id *int32 `json:"id,omitempty"`

	// Metadata User-defined metadata
	Metadata            *Metadata `json:"metadata,omitempty"`
	PropagateSchemaTags *bool     `json:"propagateSchemaTags,omitempty"`

	// References References to other schemas
	References *[]SchemaReference `json:"references,omitempty"`

	// RuleSet Schema rule set
	RuleSet *RuleSet `json:"ruleSet,omitempty"`

	// Schema Schema definition string
	Schema             *string       `json:"schema,omitempty"`
	SchemaTagsToAdd    *[]SchemaTags `json:"schemaTagsToAdd,omitempty"`
	SchemaTagsToRemove *[]SchemaTags `json:"schemaTagsToRemove,omitempty"`

	// SchemaType Schema type
	SchemaType *string `json:"schemaType,omitempty"`

	// Version Version number
	Version *int32 `json:"version,omitempty"`
}
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type Rule struct {
	// Disabled Whether the rule is disabled
	Disabled *bool `json:"disabled,omitempty"`

	// Doc Rule doc
	Doc *string `json:"doc,omitempty"`

	// Expr Rule expression
	Expr *string `json:"expr,omitempty"`

	// Kind Rule kind
	Kind *RuleKind `json:"kind,omitempty"`

	// Mode Rule mode
	Mode *RuleMode `json:"mode,omitempty"`

	// Name Rule name
	Name *string `json:"name,omitempty"`

	// OnFailure Rule action on failure
	OnFailure *string `json:"onFailure,omitempty"`

	// OnSuccess Rule action on success
	OnSuccess *string `json:"onSuccess,omitempty"`

	// Params Optional params for the rule
	Params *map[string]string `json:"params,omitempty"`

	// Tags The tags to which this rule applies
	Tags *[]string `json:"tags,omitempty"`

	// Type Rule type
	Type *string `json:"type,omitempty"`
}
type RuleKind string
type RuleMode string
type RuleSet struct {
	DomainRules    *[]Rule `json:"domainRules,omitempty"`
	MigrationRules *[]Rule `json:"migrationRules,omitempty"`
}
type Schema struct {
	// Id Globally unique identifier of the schema
	Id *int32 `json:"id,omitempty"`

	// Metadata User-defined metadata
	Metadata *Metadata `json:"metadata,omitempty"`

	// References References to other schemas
	References *[]SchemaReference `json:"references,omitempty"`

	// RuleSet Schema rule set
	RuleSet *RuleSet `json:"ruleSet,omitempty"`

	// Schema Schema definition string
	Schema *string `json:"schema,omitempty"`

	// SchemaTags Schema tags
	SchemaTags *[]SchemaTags `json:"schemaTags,omitempty"`

	// SchemaType Schema type
	SchemaType *string `json:"schemaType,omitempty"`

	// Subject Name of the subject
	Subject *string `json:"subject,omitempty"`

	// Version Version number
	Version *int32 `json:"version,omitempty"`
}
type SchemaEntity struct {
	EntityPath *string                 `json:"entityPath,omitempty"`
	EntityType *SchemaEntityEntityType `json:"entityType,omitempty"`
}
type SchemaEntityEntityType string
type SchemaReference struct {
	// Name Reference name
	Name *string `json:"name,omitempty"`

	// Subject Name of the referenced subject
	Subject *string `json:"subject,omitempty"`

	// Version Version number of the referenced subject
	Version *int32 `json:"version,omitempty"`
}
type SchemaTags struct {
	SchemaEntity *SchemaEntity `json:"schemaEntity,omitempty"`
	Tags         *[]string     `json:"tags,omitempty"`
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
type SchemaregistryV1BadRequestError = ErrorMessage
type SchemaregistryV1ForbiddenError = ErrorMessage
type SchemaregistryV1UnauthorizedError = ErrorMessage
type GetSchemaOnly1Params struct {
	// Deleted Whether to include deleted schema
	Deleted *bool `form:"deleted,omitempty" json:"deleted,omitempty"`
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

	// List List subjects
	//
	// Retrieves a list of registered subjects matching specified parameters.
	//
	// Corresponds with GET /subjects (the `List` operationId).
	list(ctx context.Context, params *ListParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSubject Delete subject
	//
	// Deletes the specified subject and its associated compatibility level if registered. It is recommended to use this API only when a topic needs to be recycled or in development environment.
	//
	// Corresponds with DELETE /subjects/{subject} (the `DeleteSubject` operationId).
	deleteSubject(ctx context.Context, subject string, params *DeleteSubjectParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// LookUpSchemaUnderSubjectWithBody Lookup schema under subject
	//
	// Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /subjects/{subject} (the `LookUpSchemaUnderSubject` operationId).
	lookUpSchemaUnderSubjectWithBody(ctx context.Context, subject string, params *LookUpSchemaUnderSubjectParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// LookUpSchemaUnderSubject Lookup schema under subject
	//
	// Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /subjects/{subject} (the `LookUpSchemaUnderSubject` operationId).
	lookUpSchemaUnderSubject(ctx context.Context, subject string, params *LookUpSchemaUnderSubjectParams, body LookUpSchemaUnderSubjectJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// LookUpSchemaUnderSubjectWithApplicationVndSchemaregistryPlusJSONBody Lookup schema under subject
	//
	// Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with POST /subjects/{subject} (the `LookUpSchemaUnderSubject` operationId).
	lookUpSchemaUnderSubjectWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, subject string, params *LookUpSchemaUnderSubjectParams, body LookUpSchemaUnderSubjectApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// LookUpSchemaUnderSubjectWithApplicationVndSchemaregistryV1PlusJSONBody Lookup schema under subject
	//
	// Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with POST /subjects/{subject} (the `LookUpSchemaUnderSubject` operationId).
	lookUpSchemaUnderSubjectWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, subject string, params *LookUpSchemaUnderSubjectParams, body LookUpSchemaUnderSubjectApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetLatestWithMetadata Retrieve the latest version with the given metadata.
	//
	// Corresponds with GET /subjects/{subject}/metadata (the `GetLatestWithMetadata` operationId).
	getLatestWithMetadata(ctx context.Context, subject string, params *GetLatestWithMetadataParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListVersions List versions under subject
	//
	// Retrieves a list of versions registered under the specified subject.
	//
	// Corresponds with GET /subjects/{subject}/versions (the `ListVersions` operationId).
	listVersions(ctx context.Context, subject string, params *ListVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterWithBody Register schema under a subject
	//
	// Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the schemas resource and is different from the schema's version which is associated with the subject. If the same schema is registered under a different subject, the same identifier will be returned. However, the version of the schema may be different under different subjects.
	// A schema should be compatible with the previously registered schema or schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a GET http:get:: /config/(string: subject). If that returns null, then GET http:get:: /config
	// When there are multiple instances of Schema Registry running in the same cluster, the schema registration request will be forwarded to one of the instances designated as the primary. If the primary is not available, the client will get an error code indicating that the forwarding has failed.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /subjects/{subject}/versions (the `Register` operationId).
	registerWithBody(ctx context.Context, subject string, params *RegisterParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// Register Register schema under a subject
	//
	// Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the schemas resource and is different from the schema's version which is associated with the subject. If the same schema is registered under a different subject, the same identifier will be returned. However, the version of the schema may be different under different subjects.
	// A schema should be compatible with the previously registered schema or schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a GET http:get:: /config/(string: subject). If that returns null, then GET http:get:: /config
	// When there are multiple instances of Schema Registry running in the same cluster, the schema registration request will be forwarded to one of the instances designated as the primary. If the primary is not available, the client will get an error code indicating that the forwarding has failed.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /subjects/{subject}/versions (the `Register` operationId).
	register(ctx context.Context, subject string, params *RegisterParams, body RegisterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterWithApplicationVndSchemaregistryPlusJSONBody Register schema under a subject
	//
	// Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the schemas resource and is different from the schema's version which is associated with the subject. If the same schema is registered under a different subject, the same identifier will be returned. However, the version of the schema may be different under different subjects.
	// A schema should be compatible with the previously registered schema or schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a GET http:get:: /config/(string: subject). If that returns null, then GET http:get:: /config
	// When there are multiple instances of Schema Registry running in the same cluster, the schema registration request will be forwarded to one of the instances designated as the primary. If the primary is not available, the client will get an error code indicating that the forwarding has failed.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with POST /subjects/{subject}/versions (the `Register` operationId).
	registerWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, subject string, params *RegisterParams, body RegisterApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterWithApplicationVndSchemaregistryV1PlusJSONBody Register schema under a subject
	//
	// Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the schemas resource and is different from the schema's version which is associated with the subject. If the same schema is registered under a different subject, the same identifier will be returned. However, the version of the schema may be different under different subjects.
	// A schema should be compatible with the previously registered schema or schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a GET http:get:: /config/(string: subject). If that returns null, then GET http:get:: /config
	// When there are multiple instances of Schema Registry running in the same cluster, the schema registration request will be forwarded to one of the instances designated as the primary. If the primary is not available, the client will get an error code indicating that the forwarding has failed.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with POST /subjects/{subject}/versions (the `Register` operationId).
	registerWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, subject string, params *RegisterParams, body RegisterApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSchemaVersion Delete schema version
	//
	// Deletes a specific version of the schema registered under this subject. This only deletes the version and the schema ID remains intact making it still possible to decode data using the schema ID. This API is recommended to be used only in development environments or under extreme circumstances where-in, its required to delete a previously registered schema for compatibility purposes or re-register previously registered schema.
	//
	// Corresponds with DELETE /subjects/{subject}/versions/{version} (the `DeleteSchemaVersion` operationId).
	deleteSchemaVersion(ctx context.Context, subject string, version string, params *DeleteSchemaVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSchemaByVersion Get schema by version
	//
	// Retrieves a specific version of the schema registered under this subject.
	//
	// Corresponds with GET /subjects/{subject}/versions/{version} (the `GetSchemaByVersion` operationId).
	getSchemaByVersion(ctx context.Context, subject string, version string, params *GetSchemaByVersionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetReferencedBy List schemas referencing a schema
	//
	// Retrieves the IDs of schemas that reference the specified schema.
	//
	// Corresponds with GET /subjects/{subject}/versions/{version}/referencedby (the `GetReferencedBy` operationId).
	getReferencedBy(ctx context.Context, subject string, version string, params *GetReferencedByParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSchemaOnly1 Get schema string by version
	//
	// Retrieves the schema for the specified version of this subject. Only the unescaped schema string is returned.
	//
	// Corresponds with GET /subjects/{subject}/versions/{version}/schema (the `GetSchemaOnly1` operationId).
	getSchemaOnly1(ctx context.Context, subject string, version string, params *GetSchemaOnly1Params, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *oasClient) getSchemaOnly1(ctx context.Context, subject string, version string, params *GetSchemaOnly1Params, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewGetSchemaOnly1Request(c.Server, subject, version, params)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewLookUpSchemaUnderSubjectRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, subject string, params *LookUpSchemaUnderSubjectParams, body LookUpSchemaUnderSubjectApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewLookUpSchemaUnderSubjectRequestWithBody(server, subject, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewLookUpSchemaUnderSubjectRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, subject string, params *LookUpSchemaUnderSubjectParams, body LookUpSchemaUnderSubjectApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewLookUpSchemaUnderSubjectRequestWithBody(server, subject, params, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewRegisterRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, subject string, params *RegisterParams, body RegisterApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewRegisterRequestWithBody(server, subject, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewRegisterRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, subject string, params *RegisterParams, body RegisterApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewRegisterRequestWithBody(server, subject, params, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewGetSchemaOnly1Request(server string, subject string, version string, params *GetSchemaOnly1Params) (*http.Request, error) {
	var err error

	var pathParam0 string

	pathParam0, err = runtime.StyleParamWithOptions("simple", false, "subject", subject, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	var pathParam1 string

	pathParam1, err = runtime.StyleParamWithOptions("simple", false, "version", version, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/subjects/%s/versions/%s/schema", pathParam0, pathParam1)
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	if params != nil {
		// queryValues collects non-styled parameters (passthrough, JSON)
		// that are safe to round-trip through url.Values.Encode().
		queryValues := queryURL.Query()
		// rawQueryFragments collects pre-encoded query fragments from
		// styled parameters, preserving literal commas as delimiters
		// per the OpenAPI spec (e.g. "color=blue,black,brown").
		var rawQueryFragments []string

		if params.Deleted != nil {

			if queryFrag, err := runtime.StyleParamWithOptions("form", true, "deleted", *params.Deleted, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationQuery, Type: "boolean", Format: ""}); err != nil {
				return nil, err
			} else {
				for _, qp := range strings.Split(queryFrag, "&") {
					rawQueryFragments = append(rawQueryFragments, qp)
				}
			}

		}

		if encoded := queryValues.Encode(); encoded != "" {
			rawQueryFragments = append(rawQueryFragments, encoded)
		}
		queryURL.RawQuery = strings.Join(rawQueryFragments, "&")
	}

	req, err := http.NewRequest(http.MethodGet, queryURL.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
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
func (r ListResponse) GetApplicationjsonQs05200() *[]string {
	return r.ApplicationjsonQs05200
}
func (r ListResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r ListResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r ListResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r ListResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r ListResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r ListResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r ListResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r ListResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r ListResponse) GetBody() []byte {
	return r.Body
}
func (r ListResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSubjectResponse) GetApplicationjsonQs05200() *[]int32 {
	return r.ApplicationjsonQs05200
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]int32 {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryV1JSON200() *[]int32 {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r DeleteSubjectResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r DeleteSubjectResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r DeleteSubjectResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r DeleteSubjectResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r DeleteSubjectResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r DeleteSubjectResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r DeleteSubjectResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSubjectResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSubjectResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSubjectResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationjsonQs05200() *Schema {
	return r.ApplicationjsonQs05200
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryJSONQs09200() *Schema {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryV1JSON200() *Schema {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r LookUpSchemaUnderSubjectResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r LookUpSchemaUnderSubjectResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r LookUpSchemaUnderSubjectResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r LookUpSchemaUnderSubjectResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r LookUpSchemaUnderSubjectResponse) GetBody() []byte {
	return r.Body
}
func (r LookUpSchemaUnderSubjectResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r LookUpSchemaUnderSubjectResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r LookUpSchemaUnderSubjectResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetLatestWithMetadataResponse) GetApplicationjsonQs05200() *Schema {
	return r.ApplicationjsonQs05200
}
func (r GetLatestWithMetadataResponse) GetApplicationvndSchemaregistryJSONQs09200() *Schema {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetLatestWithMetadataResponse) GetApplicationvndSchemaregistryV1JSON200() *Schema {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetLatestWithMetadataResponse) GetBody() []byte {
	return r.Body
}
func (r GetLatestWithMetadataResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetLatestWithMetadataResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetLatestWithMetadataResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListVersionsResponse) GetApplicationjsonQs05200() *[]int32 {
	return r.ApplicationjsonQs05200
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]int32 {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryV1JSON200() *[]int32 {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r ListVersionsResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r ListVersionsResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r ListVersionsResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r ListVersionsResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r ListVersionsResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r ListVersionsResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r ListVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r RegisterResponse) GetApplicationjsonQs05200() *RegisterSchemaResponse {
	return r.ApplicationjsonQs05200
}
func (r RegisterResponse) GetApplicationvndSchemaregistryJSONQs09200() *RegisterSchemaResponse {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r RegisterResponse) GetApplicationvndSchemaregistryV1JSON200() *RegisterSchemaResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r RegisterResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r RegisterResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r RegisterResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r RegisterResponse) GetApplicationjsonQs05409() *ErrorMessage {
	return r.ApplicationjsonQs05409
}
func (r RegisterResponse) GetApplicationvndSchemaregistryJSONQs09409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09409
}
func (r RegisterResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r RegisterResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r RegisterResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r RegisterResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r RegisterResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r RegisterResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r RegisterResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r RegisterResponse) GetBody() []byte {
	return r.Body
}
func (r RegisterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r RegisterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r RegisterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSchemaVersionResponse) GetApplicationjsonQs05200() *int32 {
	return r.ApplicationjsonQs05200
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryJSONQs09200() *int32 {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryV1JSON200() *int32 {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r DeleteSchemaVersionResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r DeleteSchemaVersionResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r DeleteSchemaVersionResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r DeleteSchemaVersionResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r DeleteSchemaVersionResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r DeleteSchemaVersionResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r DeleteSchemaVersionResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r DeleteSchemaVersionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSchemaVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSchemaVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSchemaVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSchemaByVersionResponse) GetApplicationjsonQs05200() *Schema {
	return r.ApplicationjsonQs05200
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryJSONQs09200() *Schema {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryV1JSON200() *Schema {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemaByVersionResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemaByVersionResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemaByVersionResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemaByVersionResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetSchemaByVersionResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r GetSchemaByVersionResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemaByVersionResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemaByVersionResponse) GetBody() []byte {
	return r.Body
}
func (r GetSchemaByVersionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemaByVersionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemaByVersionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetReferencedByResponse) GetApplicationjsonQs05200() *[]int32 {
	return r.ApplicationjsonQs05200
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]int32 {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryV1JSON200() *[]int32 {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetReferencedByResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetReferencedByResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetReferencedByResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetReferencedByResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetReferencedByResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r GetReferencedByResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetReferencedByResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetReferencedByResponse) GetBody() []byte {
	return r.Body
}
func (r GetReferencedByResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetReferencedByResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetReferencedByResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type GetSchemaOnly1Response429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type GetSchemaOnly1Response struct {
	Body         []byte
	HTTPResponse *http.Response
	// ApplicationjsonQs05200 the response for an HTTP 200 `application/json; qs=0.5` response
	ApplicationjsonQs05200 *string
	// ApplicationvndSchemaregistryJSONQs09200 the response for an HTTP 200 `application/vnd.schemaregistry+json; qs=0.9` response
	ApplicationvndSchemaregistryJSONQs09200 *string
	// ApplicationvndSchemaregistryV1JSON200 the response for an HTTP 200 `application/vnd.schemaregistry.v1+json` response
	ApplicationvndSchemaregistryV1JSON200 *string
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *SchemaregistryV1BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *SchemaregistryV1UnauthorizedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *SchemaregistryV1ForbiddenError
	// ApplicationjsonQs05404 the response for an HTTP 404 `application/json; qs=0.5` response
	ApplicationjsonQs05404 *ErrorMessage
	// ApplicationvndSchemaregistryJSONQs09404 the response for an HTTP 404 `application/vnd.schemaregistry+json; qs=0.9` response
	ApplicationvndSchemaregistryJSONQs09404 *ErrorMessage
	// ApplicationvndSchemaregistryV1JSON404 the response for an HTTP 404 `application/vnd.schemaregistry.v1+json` response
	ApplicationvndSchemaregistryV1JSON404 *ErrorMessage
	// ApplicationjsonQs05422 the response for an HTTP 422 `application/json; qs=0.5` response
	ApplicationjsonQs05422 *ErrorMessage
	// ApplicationvndSchemaregistryJSONQs09422 the response for an HTTP 422 `application/vnd.schemaregistry+json; qs=0.9` response
	ApplicationvndSchemaregistryJSONQs09422 *ErrorMessage
	// ApplicationvndSchemaregistryV1JSON422 the response for an HTTP 422 `application/vnd.schemaregistry.v1+json` response
	ApplicationvndSchemaregistryV1JSON422 *ErrorMessage
	// ApplicationjsonQs05500 the response for an HTTP 500 `application/json; qs=0.5` response
	ApplicationjsonQs05500 *ErrorMessage
	// ApplicationvndSchemaregistryJSONQs09500 the response for an HTTP 500 `application/vnd.schemaregistry+json; qs=0.9` response
	ApplicationvndSchemaregistryJSONQs09500 *ErrorMessage
	// ApplicationvndSchemaregistryV1JSON500 the response for an HTTP 500 `application/vnd.schemaregistry.v1+json` response
	ApplicationvndSchemaregistryV1JSON500 *ErrorMessage
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *GetSchemaOnly1Response429Headers
}

func (r GetSchemaOnly1Response) GetApplicationjsonQs05200() *string {
	return r.ApplicationjsonQs05200
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryJSONQs09200() *string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryV1JSON200() *string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemaOnly1Response) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemaOnly1Response) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemaOnly1Response) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemaOnly1Response) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetSchemaOnly1Response) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r GetSchemaOnly1Response) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemaOnly1Response) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemaOnly1Response) GetBody() []byte {
	return r.Body
}
func (r GetSchemaOnly1Response) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemaOnly1Response) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemaOnly1Response) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) GetSchemaOnly1WithResponse(ctx context.Context, subject string, version string, params *GetSchemaOnly1Params, reqEditors ...RequestEditorFn) (*GetSchemaOnly1Response, error) {
	rsp, err := c.getSchemaOnly1(ctx, subject, version, params, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseGetSchemaOnly1Response(rsp)
}
func ParseGetSchemaOnly1Response(rsp *http.Response) (*GetSchemaOnly1Response, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &GetSchemaOnly1Response{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case rsp.Header.Get("Content-Type") == "application/json; qs=0.5" && rsp.StatusCode == 200:
		var dest string
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationjsonQs05200 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry+json; qs=0.9" && rsp.StatusCode == 200:
		var dest string
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryJSONQs09200 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry.v1+json" && rsp.StatusCode == 200:
		var dest string
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryV1JSON200 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest SchemaregistryV1BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest SchemaregistryV1UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest SchemaregistryV1ForbiddenError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case rsp.Header.Get("Content-Type") == "application/json; qs=0.5" && rsp.StatusCode == 404:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationjsonQs05404 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry+json; qs=0.9" && rsp.StatusCode == 404:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryJSONQs09404 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry.v1+json" && rsp.StatusCode == 404:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryV1JSON404 = &dest

	case rsp.Header.Get("Content-Type") == "application/json; qs=0.5" && rsp.StatusCode == 422:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationjsonQs05422 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry+json; qs=0.9" && rsp.StatusCode == 422:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryJSONQs09422 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry.v1+json" && rsp.StatusCode == 422:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryV1JSON422 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case rsp.Header.Get("Content-Type") == "application/json; qs=0.5" && rsp.StatusCode == 500:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationjsonQs05500 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry+json; qs=0.9" && rsp.StatusCode == 500:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryJSONQs09500 = &dest

	case rsp.Header.Get("Content-Type") == "application/vnd.schemaregistry.v1+json" && rsp.StatusCode == 500:
		var dest ErrorMessage
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.ApplicationvndSchemaregistryV1JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 429:
		var headers GetSchemaOnly1Response429Headers
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
	}

	return response, nil
}
