package schemas

import (
	"context"
	"encoding/json"
	"errors"
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
type SchemaString struct {
	// MaxId Maximum ID
	MaxId *int32 `json:"maxId,omitempty"`

	// Metadata User-defined metadata
	Metadata *Metadata `json:"metadata,omitempty"`

	// References References to other schemas
	References *[]SchemaReference `json:"references,omitempty"`

	// RuleSet Schema rule set
	RuleSet *RuleSet `json:"ruleSet,omitempty"`

	// Schema Schema string identified by the ID
	Schema *string `json:"schema,omitempty"`

	// SchemaTags Schema tags
	SchemaTags *[]SchemaTags `json:"schemaTags,omitempty"`

	// SchemaType Schema type
	SchemaType *string `json:"schemaType,omitempty"`
}
type SchemaTags struct {
	SchemaEntity *SchemaEntity `json:"schemaEntity,omitempty"`
	Tags         *[]string     `json:"tags,omitempty"`
}
type SubjectVersion struct {
	// Subject Name of the subject
	Subject *string `json:"subject,omitempty"`

	// Version Version number
	Version *int32 `json:"version,omitempty"`
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

	// GetSchemas List schemas
	//
	// Get the schemas matching the specified parameters.
	//
	// Corresponds with GET /schemas (the `GetSchemas` operationId).
	getSchemas(ctx context.Context, params *GetSchemasParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSchema Get schema string by ID
	//
	// Retrieves the schema string identified by the input ID.
	//
	// Corresponds with GET /schemas/ids/{id} (the `GetSchema` operationId).
	getSchema(ctx context.Context, id int32, params *GetSchemaParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSchemaOnly Get schema by ID
	//
	// Retrieves the schema identified by the input ID.
	//
	// Corresponds with GET /schemas/ids/{id}/schema (the `GetSchemaOnly` operationId).
	getSchemaOnly(ctx context.Context, id int32, params *GetSchemaOnlyParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSubjects List subjects associated to schema ID
	//
	// Retrieves all the subjects associated with a particular schema ID.
	//
	// Corresponds with GET /schemas/ids/{id}/subjects (the `GetSubjects` operationId).
	getSubjects(ctx context.Context, id int32, params *GetSubjectsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetVersions List subject-versions associated to schema ID
	//
	// Get all the subject-version pairs associated with the input ID.
	//
	// Corresponds with GET /schemas/ids/{id}/versions (the `GetVersions` operationId).
	getVersions(ctx context.Context, id int32, params *GetVersionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSchemaTypes List supported schema types
	//
	// Retrieve the schema types supported by this registry.
	//
	// Corresponds with GET /schemas/types (the `GetSchemaTypes` operationId).
	getSchemaTypes(ctx context.Context, reqEditors ...RequestEditorFn) (*http.Response, error)
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
func (r GetSchemasResponse) GetApplicationjsonQs05200() *[]Schema {
	return r.ApplicationjsonQs05200
}
func (r GetSchemasResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]Schema {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemasResponse) GetApplicationvndSchemaregistryV1JSON200() *[]Schema {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemasResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemasResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemasResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemasResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemasResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemasResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemasResponse) GetBody() []byte {
	return r.Body
}
func (r GetSchemasResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemasResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemasResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSchemaResponse) GetApplicationjsonQs05200() *SchemaString {
	return r.ApplicationjsonQs05200
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryJSONQs09200() *SchemaString {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryV1JSON200() *SchemaString {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemaResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemaResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemaResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemaResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetSchemaResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemaResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemaResponse) GetBody() []byte {
	return r.Body
}
func (r GetSchemaResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemaResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemaResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSchemaOnlyResponse) GetApplicationjsonQs05200() *string {
	return r.ApplicationjsonQs05200
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryJSONQs09200() *string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryV1JSON200() *string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemaOnlyResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemaOnlyResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemaOnlyResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemaOnlyResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetSchemaOnlyResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemaOnlyResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemaOnlyResponse) GetBody() []byte {
	return r.Body
}
func (r GetSchemaOnlyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemaOnlyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemaOnlyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSubjectsResponse) GetApplicationjsonQs05200() *[]string {
	return r.ApplicationjsonQs05200
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSubjectsResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSubjectsResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSubjectsResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSubjectsResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetSubjectsResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSubjectsResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSubjectsResponse) GetBody() []byte {
	return r.Body
}
func (r GetSubjectsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSubjectsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSubjectsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetVersionsResponse) GetApplicationjsonQs05200() *[]SubjectVersion {
	return r.ApplicationjsonQs05200
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]SubjectVersion {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryV1JSON200() *[]SubjectVersion {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetVersionsResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetVersionsResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetVersionsResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetVersionsResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetVersionsResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetVersionsResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetVersionsResponse) GetBody() []byte {
	return r.Body
}
func (r GetVersionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetVersionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetVersionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSchemaTypesResponse) GetApplicationjsonQs05200() *[]string {
	return r.ApplicationjsonQs05200
}
func (r GetSchemaTypesResponse) GetApplicationvndSchemaregistryJSONQs09200() *[]string {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetSchemaTypesResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetSchemaTypesResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetSchemaTypesResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetSchemaTypesResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetSchemaTypesResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetSchemaTypesResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetSchemaTypesResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetSchemaTypesResponse) GetBody() []byte {
	return r.Body
}
func (r GetSchemaTypesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSchemaTypesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSchemaTypesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
