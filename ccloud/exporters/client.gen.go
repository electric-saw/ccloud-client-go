package exporters

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
type ExporterConfigResponse struct {
	// BasicAuthCredentialsSource Config SR Auth
	BasicAuthCredentialsSource *string `json:"basic.auth.credentials.source,omitempty"`

	// BasicAuthUserInfo Config SR User Info
	BasicAuthUserInfo *string `json:"basic.auth.user.info,omitempty"`

	// SchemaRegistryUrl Config SR URL
	SchemaRegistryUrl *string `json:"schema.registry.url,omitempty"`
}
type ExporterReference struct {
	// Config The map containing exporter's configurations
	Config *map[string]string `json:"config,omitempty"`

	// Context Customized context of the exporter if contextType equals CUSTOM.
	Context *string `json:"context,omitempty"`

	// ContextType Context type of the exporter. One of CUSTOM, NONE or AUTO (default)
	ContextType *string `json:"contextType,omitempty"`

	// KekRenameFormat Format string for the KEK name in the destination cluster, which may contain ${kek} as a placeholder for the originating KEK name. For example, dc_${kek} for the KEK aws_key will map to the destination KEK name dc_aws_key.
	KekRenameFormat *string `json:"kekRenameFormat,omitempty"`

	// Name Name of the exporter
	Name *string `json:"name,omitempty"`

	// SubjectRenameFormat Format string for the subject name in the destination cluster, which may contain ${subject} as a placeholder for the originating subject name. For example, dc_${subject} for the subject orders will map to the destination subject name dc_orders.
	SubjectRenameFormat *string `json:"subjectRenameFormat,omitempty"`

	// Subjects Name of each exporter subject
	Subjects *[]string `json:"subjects,omitempty"`
}
type ExporterResponse struct {
	// Name Name of the exporter
	Name *string `json:"name,omitempty"`
}
type ExporterStatusResponse struct {
	// Name Name of exporter.
	Name *string `json:"name,omitempty"`

	// Offset Offset of the exporter
	Offset *int64 `json:"offset,omitempty"`

	// State State of the exporter. Could be STARTING, RUNNING or PAUSED
	State *string `json:"state,omitempty"`

	// Trace Error trace of the exporter
	Trace *string `json:"trace,omitempty"`

	// Ts Timestamp of the exporter
	Ts *int64 `json:"ts,omitempty"`
}
type ExporterUpdateRequest struct {
	// Config The map containing exporter's configurations
	Config *map[string]string `json:"config,omitempty"`

	// Context Customized context of the exporter if contextType equals CUSTOM.
	Context *string `json:"context,omitempty"`

	// ContextType Context type of the exporter. One of CUSTOM, NONE or AUTO (default)
	ContextType *string `json:"contextType,omitempty"`

	// KekRenameFormat Format string for the KEK name in the destination cluster, which may
	// contain ${kek} as a placeholder for the originating KEK name. For
	// example, dc_${kek} for the KEK aws_key will map to the destination
	// KEK name dc_aws_key.
	KekRenameFormat *string `json:"kekRenameFormat,omitempty"`

	// SubjectRenameFormat Format string for the subject name in the destination cluster, which
	// may contain ${subject} as a placeholder for the originating subject
	// name. For example, dc_${subject} for the subject orders will map to
	// the destination subject name dc_orders.
	SubjectRenameFormat *string `json:"subjectRenameFormat,omitempty"`

	// Subjects Name of each exporter subject
	Subjects *[]string `json:"subjects,omitempty"`
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
type SchemaregistryV1AccountNotFoundError = ErrorMessage
type SchemaregistryV1BadRequestError = ErrorMessage
type SchemaregistryV1DefaultSystemError = ErrorMessage
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

type ClientInterface interface {

	// ListExporters Gets all schema exporters
	//
	// Retrieves a list of schema exporters that have been created.
	//
	// Corresponds with GET /exporters (the `ListExporters` operationId).
	ListExporters(ctx context.Context, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterExporterWithBody Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterExporter Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporter(ctx context.Context, body RegisterExporterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterExporterWithApplicationVndSchemaregistryPlusJSONBody Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, body RegisterExporterApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RegisterExporterWithApplicationVndSchemaregistryV1PlusJSONBody Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, body RegisterExporterApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteExporter Delete schema exporter by name
	//
	// Deletes the schema exporter.
	//
	// Corresponds with DELETE /exporters/{name} (the `DeleteExporter` operationId).
	DeleteExporter(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetExporterInfoByName Gets schema exporter by name
	//
	// Retrieves the information of the schema exporter.
	//
	// Corresponds with GET /exporters/{name} (the `GetExporterInfoByName` operationId).
	GetExporterInfoByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterInfoWithBody Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithBody(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterInfo Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfo(ctx context.Context, name string, body UpdateExporterInfoJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterInfoWithApplicationVndSchemaregistryPlusJSONBody Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, name string, body UpdateExporterInfoApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterInfoWithApplicationVndSchemaregistryV1PlusJSONBody Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, name string, body UpdateExporterInfoApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetExporterConfigByName Gets schema exporter config by name
	//
	// Retrieves the config of the schema exporter.
	//
	// Corresponds with GET /exporters/{name}/config (the `GetExporterConfigByName` operationId).
	GetExporterConfigByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterConfigByNameWithBody Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithBody(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterConfigByName Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByName(ctx context.Context, name string, body UpdateExporterConfigByNameJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterConfigByNameWithApplicationVndSchemaregistryPlusJSONBody Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateExporterConfigByNameWithApplicationVndSchemaregistryV1PlusJSONBody Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PauseExporterByName Pause schema exporter by name
	//
	// Pauses the state of the schema exporter.
	//
	// Corresponds with PUT /exporters/{name}/pause (the `PauseExporterByName` operationId).
	PauseExporterByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResetExporterByName Reset schema exporter by name
	//
	// Reset the state of the schema exporter.
	//
	// Corresponds with PUT /exporters/{name}/reset (the `ResetExporterByName` operationId).
	ResetExporterByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResumeExporterByName Resume schema exporter by name
	//
	// Resume running of the schema exporter.
	//
	// Corresponds with PUT /exporters/{name}/resume (the `ResumeExporterByName` operationId).
	ResumeExporterByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetExporterStatusByName Gets schema exporter status by name
	//
	// Retrieves the status of the schema exporter.
	//
	// Corresponds with GET /exporters/{name}/status (the `GetExporterStatusByName` operationId).
	GetExporterStatusByName(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func NewRegisterExporterRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, body RegisterExporterApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewRegisterExporterRequestWithBody(server, "application/vnd.schemaregistry+json", bodyReader)
}
func NewRegisterExporterRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, body RegisterExporterApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewRegisterExporterRequestWithBody(server, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewUpdateExporterInfoRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, name string, body UpdateExporterInfoApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateExporterInfoRequestWithBody(server, name, "application/vnd.schemaregistry+json", bodyReader)
}
func NewUpdateExporterInfoRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, name string, body UpdateExporterInfoApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateExporterInfoRequestWithBody(server, name, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewUpdateExporterConfigByNameRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateExporterConfigByNameRequestWithBody(server, name, "application/vnd.schemaregistry+json", bodyReader)
}
func NewUpdateExporterConfigByNameRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateExporterConfigByNameRequestWithBody(server, name, "application/vnd.schemaregistry.v1+json", bodyReader)
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

	// ListExportersWithResponse Gets all schema exporters
	//
	// Retrieves a list of schema exporters that have been created.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /exporters (the `ListExporters` operationId).
	ListExportersWithResponse(ctx context.Context, reqEditors ...RequestEditorFn) (*ListExportersResponse, error)

	// RegisterExporterWithBodyWithResponse Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*RegisterExporterResponse, error)

	// RegisterExporterWithResponse Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithResponse(ctx context.Context, body RegisterExporterJSONRequestBody, reqEditors ...RequestEditorFn) (*RegisterExporterResponse, error)

	// RegisterExporterWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, body RegisterExporterApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*RegisterExporterResponse, error)

	// RegisterExporterWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Creates a new schema exporter
	//
	// Creates a new schema exporter. All attributes in request body are optional except config.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /exporters (the `RegisterExporter` operationId).
	RegisterExporterWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, body RegisterExporterApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*RegisterExporterResponse, error)

	// DeleteExporterWithResponse Delete schema exporter by name
	//
	// Deletes the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /exporters/{name} (the `DeleteExporter` operationId).
	DeleteExporterWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*DeleteExporterResponse, error)

	// GetExporterInfoByNameWithResponse Gets schema exporter by name
	//
	// Retrieves the information of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /exporters/{name} (the `GetExporterInfoByName` operationId).
	GetExporterInfoByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*GetExporterInfoByNameResponse, error)

	// UpdateExporterInfoWithBodyWithResponse Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithBodyWithResponse(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateExporterInfoResponse, error)

	// UpdateExporterInfoWithResponse Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithResponse(ctx context.Context, name string, body UpdateExporterInfoJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterInfoResponse, error)

	// UpdateExporterInfoWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, name string, body UpdateExporterInfoApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterInfoResponse, error)

	// UpdateExporterInfoWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Update schema exporter by name
	//
	// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name} (the `UpdateExporterInfo` operationId).
	UpdateExporterInfoWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, name string, body UpdateExporterInfoApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterInfoResponse, error)

	// GetExporterConfigByNameWithResponse Gets schema exporter config by name
	//
	// Retrieves the config of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /exporters/{name}/config (the `GetExporterConfigByName` operationId).
	GetExporterConfigByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*GetExporterConfigByNameResponse, error)

	// UpdateExporterConfigByNameWithBodyWithResponse Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithBodyWithResponse(ctx context.Context, name string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateExporterConfigByNameResponse, error)

	// UpdateExporterConfigByNameWithResponse Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithResponse(ctx context.Context, name string, body UpdateExporterConfigByNameJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterConfigByNameResponse, error)

	// UpdateExporterConfigByNameWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterConfigByNameResponse, error)

	// UpdateExporterConfigByNameWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Update schema exporter config by name
	//
	// Updates the configuration of the schema exporter.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/config (the `UpdateExporterConfigByName` operationId).
	UpdateExporterConfigByNameWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, name string, body UpdateExporterConfigByNameApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateExporterConfigByNameResponse, error)

	// PauseExporterByNameWithResponse Pause schema exporter by name
	//
	// Pauses the state of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/pause (the `PauseExporterByName` operationId).
	PauseExporterByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*PauseExporterByNameResponse, error)

	// ResetExporterByNameWithResponse Reset schema exporter by name
	//
	// Reset the state of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/reset (the `ResetExporterByName` operationId).
	ResetExporterByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*ResetExporterByNameResponse, error)

	// ResumeExporterByNameWithResponse Resume schema exporter by name
	//
	// Resume running of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /exporters/{name}/resume (the `ResumeExporterByName` operationId).
	ResumeExporterByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*ResumeExporterByNameResponse, error)

	// GetExporterStatusByNameWithResponse Gets schema exporter status by name
	//
	// Retrieves the status of the schema exporter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /exporters/{name}/status (the `GetExporterStatusByName` operationId).
	GetExporterStatusByNameWithResponse(ctx context.Context, name string, reqEditors ...RequestEditorFn) (*GetExporterStatusByNameResponse, error)
}

func (r ListExportersResponse) GetApplicationvndSchemaregistryV1JSON200() *[]string {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r ListExportersResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r ListExportersResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r ListExportersResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r ListExportersResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r ListExportersResponse) GetBody() []byte {
	return r.Body
}
func (r ListExportersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListExportersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListExportersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r RegisterExporterResponse) GetApplicationjsonQs05200() *ExporterResponse {
	return r.ApplicationjsonQs05200
}
func (r RegisterExporterResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r RegisterExporterResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r RegisterExporterResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r RegisterExporterResponse) GetApplicationjsonQs05409() *ErrorMessage {
	return r.ApplicationjsonQs05409
}
func (r RegisterExporterResponse) GetApplicationvndSchemaregistryJSONQs09409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09409
}
func (r RegisterExporterResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r RegisterExporterResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r RegisterExporterResponse) GetBody() []byte {
	return r.Body
}
func (r RegisterExporterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r RegisterExporterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r RegisterExporterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteExporterResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r DeleteExporterResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r DeleteExporterResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r DeleteExporterResponse) GetJSON404() *SchemaregistryV1AccountNotFoundError {
	return r.JSON404
}
func (r DeleteExporterResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r DeleteExporterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteExporterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteExporterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteExporterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetExporterInfoByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterReference {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetExporterInfoByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetExporterInfoByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetExporterInfoByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetExporterInfoByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetExporterInfoByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r GetExporterInfoByNameResponse) GetBody() []byte {
	return r.Body
}
func (r GetExporterInfoByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetExporterInfoByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetExporterInfoByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateExporterInfoResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r UpdateExporterInfoResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r UpdateExporterInfoResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r UpdateExporterInfoResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r UpdateExporterInfoResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r UpdateExporterInfoResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r UpdateExporterInfoResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r UpdateExporterInfoResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateExporterInfoResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateExporterInfoResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateExporterInfoResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetExporterConfigByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterConfigResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetExporterConfigByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetExporterConfigByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetExporterConfigByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetExporterConfigByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetExporterConfigByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r GetExporterConfigByNameResponse) GetBody() []byte {
	return r.Body
}
func (r GetExporterConfigByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetExporterConfigByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetExporterConfigByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateExporterConfigByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r UpdateExporterConfigByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r UpdateExporterConfigByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r UpdateExporterConfigByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r UpdateExporterConfigByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r UpdateExporterConfigByNameResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r UpdateExporterConfigByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r UpdateExporterConfigByNameResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateExporterConfigByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateExporterConfigByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateExporterConfigByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r PauseExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r PauseExporterByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r PauseExporterByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r PauseExporterByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r PauseExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r PauseExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r PauseExporterByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r PauseExporterByNameResponse) GetBody() []byte {
	return r.Body
}
func (r PauseExporterByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PauseExporterByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PauseExporterByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ResetExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r ResetExporterByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r ResetExporterByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r ResetExporterByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r ResetExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r ResetExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r ResetExporterByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r ResetExporterByNameResponse) GetBody() []byte {
	return r.Body
}
func (r ResetExporterByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ResetExporterByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ResetExporterByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ResumeExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r ResumeExporterByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r ResumeExporterByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r ResumeExporterByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r ResumeExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r ResumeExporterByNameResponse) GetApplicationvndSchemaregistryV1JSON409() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON409
}
func (r ResumeExporterByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r ResumeExporterByNameResponse) GetBody() []byte {
	return r.Body
}
func (r ResumeExporterByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ResumeExporterByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ResumeExporterByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetExporterStatusByNameResponse) GetApplicationvndSchemaregistryV1JSON200() *ExporterStatusResponse {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetExporterStatusByNameResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetExporterStatusByNameResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetExporterStatusByNameResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetExporterStatusByNameResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetExporterStatusByNameResponse) GetJSON500() *SchemaregistryV1DefaultSystemError {
	return r.JSON500
}
func (r GetExporterStatusByNameResponse) GetBody() []byte {
	return r.Body
}
func (r GetExporterStatusByNameResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetExporterStatusByNameResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetExporterStatusByNameResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
