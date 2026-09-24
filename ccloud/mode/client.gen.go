package mode

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
type Mode struct {
	// Mode Schema Registry operating mode
	Mode *string `json:"mode,omitempty"`
}
type ModeUpdateRequest struct {
	// Mode Schema Registry operating mode
	Mode *string `json:"mode,omitempty"`
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

	// GetTopLevelMode Get global mode
	//
	// Retrieves global mode.
	//
	// Corresponds with GET /mode (the `GetTopLevelMode` operationId).
	GetTopLevelMode(ctx context.Context, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTopLevelModeWithBody Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithBody(ctx context.Context, params *UpdateTopLevelModeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTopLevelMode Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelMode(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTopLevelModeWithApplicationVndSchemaregistryPlusJSONBody Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateTopLevelModeWithApplicationVndSchemaregistryV1PlusJSONBody Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSubjectMode Delete subject mode
	//
	// Deletes the specified subject-level mode and reverts to the global default.
	//
	// Corresponds with DELETE /mode/{subject} (the `DeleteSubjectMode` operationId).
	DeleteSubjectMode(ctx context.Context, subject string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetMode Get subject mode
	//
	// Retrieves the subject mode.
	//
	// Corresponds with GET /mode/{subject} (the `GetMode` operationId).
	GetMode(ctx context.Context, subject string, params *GetModeParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateModeWithBody Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithBody(ctx context.Context, subject string, params *UpdateModeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateMode Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateMode(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateModeWithApplicationVndSchemaregistryPlusJSONBody Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type.
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithApplicationVndSchemaregistryPlusJSONBody(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateModeWithApplicationVndSchemaregistryV1PlusJSONBody Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type.
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithApplicationVndSchemaregistryV1PlusJSONBody(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func NewUpdateTopLevelModeRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateTopLevelModeRequestWithBody(server, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewUpdateTopLevelModeRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateTopLevelModeRequestWithBody(server, params, "application/vnd.schemaregistry.v1+json", bodyReader)
}
func NewUpdateModeRequestWithApplicationVndSchemaregistryPlusJSONBody(server string, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateModeRequestWithBody(server, subject, params, "application/vnd.schemaregistry+json", bodyReader)
}
func NewUpdateModeRequestWithApplicationVndSchemaregistryV1PlusJSONBody(server string, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryV1PlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateModeRequestWithBody(server, subject, params, "application/vnd.schemaregistry.v1+json", bodyReader)
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

	// GetTopLevelModeWithResponse Get global mode
	//
	// Retrieves global mode.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /mode (the `GetTopLevelMode` operationId).
	GetTopLevelModeWithResponse(ctx context.Context, reqEditors ...RequestEditorFn) (*GetTopLevelModeResponse, error)

	// UpdateTopLevelModeWithBodyWithResponse Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithBodyWithResponse(ctx context.Context, params *UpdateTopLevelModeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateTopLevelModeResponse, error)

	// UpdateTopLevelModeWithResponse Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithResponse(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTopLevelModeResponse, error)

	// UpdateTopLevelModeWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTopLevelModeResponse, error)

	// UpdateTopLevelModeWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Update global mode
	//
	// Update global mode. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode (the `UpdateTopLevelMode` operationId).
	UpdateTopLevelModeWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, params *UpdateTopLevelModeParams, body UpdateTopLevelModeApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateTopLevelModeResponse, error)

	// DeleteSubjectModeWithResponse Delete subject mode
	//
	// Deletes the specified subject-level mode and reverts to the global default.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /mode/{subject} (the `DeleteSubjectMode` operationId).
	DeleteSubjectModeWithResponse(ctx context.Context, subject string, reqEditors ...RequestEditorFn) (*DeleteSubjectModeResponse, error)

	// GetModeWithResponse Get subject mode
	//
	// Retrieves the subject mode.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /mode/{subject} (the `GetMode` operationId).
	GetModeWithResponse(ctx context.Context, subject string, params *GetModeParams, reqEditors ...RequestEditorFn) (*GetModeResponse, error)

	// UpdateModeWithBodyWithResponse Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithBodyWithResponse(ctx context.Context, subject string, params *UpdateModeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateModeResponse, error)

	// UpdateModeWithResponse Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithResponse(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateModeResponse, error)

	// UpdateModeWithApplicationVndSchemaregistryPlusJSONBodyWithResponse Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithApplicationVndSchemaregistryPlusJSONBodyWithResponse(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateModeResponse, error)

	// UpdateModeWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse Update subject mode
	//
	// Update mode for the specified subject. On success, echoes the original request back to the client.
	//
	// Takes a body of the `application/vnd.schemaregistry.v1+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /mode/{subject} (the `UpdateMode` operationId).
	UpdateModeWithApplicationVndSchemaregistryV1PlusJSONBodyWithResponse(ctx context.Context, subject string, params *UpdateModeParams, body UpdateModeApplicationVndSchemaregistryV1PlusJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateModeResponse, error)
}

func (r GetTopLevelModeResponse) GetApplicationjsonQs05200() *Mode {
	return r.ApplicationjsonQs05200
}
func (r GetTopLevelModeResponse) GetApplicationvndSchemaregistryJSONQs09200() *Mode {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetTopLevelModeResponse) GetApplicationvndSchemaregistryV1JSON200() *Mode {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetTopLevelModeResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetTopLevelModeResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetTopLevelModeResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetTopLevelModeResponse) GetBody() []byte {
	return r.Body
}
func (r GetTopLevelModeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetTopLevelModeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetTopLevelModeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateTopLevelModeResponse) GetApplicationjsonQs05200() *ModeUpdateRequest {
	return r.ApplicationjsonQs05200
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryJSONQs09200() *ModeUpdateRequest {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryV1JSON200() *ModeUpdateRequest {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r UpdateTopLevelModeResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r UpdateTopLevelModeResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r UpdateTopLevelModeResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r UpdateTopLevelModeResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r UpdateTopLevelModeResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r UpdateTopLevelModeResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r UpdateTopLevelModeResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateTopLevelModeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateTopLevelModeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateTopLevelModeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSubjectModeResponse) GetApplicationjsonQs05200() *Mode {
	return r.ApplicationjsonQs05200
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryJSONQs09200() *Mode {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryV1JSON200() *Mode {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r DeleteSubjectModeResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r DeleteSubjectModeResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r DeleteSubjectModeResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r DeleteSubjectModeResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r DeleteSubjectModeResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r DeleteSubjectModeResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r DeleteSubjectModeResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSubjectModeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSubjectModeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSubjectModeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetModeResponse) GetApplicationjsonQs05200() *Mode {
	return r.ApplicationjsonQs05200
}
func (r GetModeResponse) GetApplicationvndSchemaregistryJSONQs09200() *Mode {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r GetModeResponse) GetApplicationvndSchemaregistryV1JSON200() *Mode {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r GetModeResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r GetModeResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r GetModeResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r GetModeResponse) GetApplicationjsonQs05404() *ErrorMessage {
	return r.ApplicationjsonQs05404
}
func (r GetModeResponse) GetApplicationvndSchemaregistryJSONQs09404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09404
}
func (r GetModeResponse) GetApplicationvndSchemaregistryV1JSON404() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON404
}
func (r GetModeResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r GetModeResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r GetModeResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r GetModeResponse) GetBody() []byte {
	return r.Body
}
func (r GetModeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetModeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetModeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateModeResponse) GetApplicationjsonQs05200() *ModeUpdateRequest {
	return r.ApplicationjsonQs05200
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryJSONQs09200() *ModeUpdateRequest {
	return r.ApplicationvndSchemaregistryJSONQs09200
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryV1JSON200() *ModeUpdateRequest {
	return r.ApplicationvndSchemaregistryV1JSON200
}
func (r UpdateModeResponse) GetJSON400() *SchemaregistryV1BadRequestError {
	return r.JSON400
}
func (r UpdateModeResponse) GetJSON401() *SchemaregistryV1UnauthorizedError {
	return r.JSON401
}
func (r UpdateModeResponse) GetJSON403() *SchemaregistryV1ForbiddenError {
	return r.JSON403
}
func (r UpdateModeResponse) GetApplicationjsonQs05422() *ErrorMessage {
	return r.ApplicationjsonQs05422
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryJSONQs09422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09422
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryV1JSON422() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON422
}
func (r UpdateModeResponse) GetApplicationjsonQs05500() *ErrorMessage {
	return r.ApplicationjsonQs05500
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryJSONQs09500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryJSONQs09500
}
func (r UpdateModeResponse) GetApplicationvndSchemaregistryV1JSON500() *ErrorMessage {
	return r.ApplicationvndSchemaregistryV1JSON500
}
func (r UpdateModeResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateModeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateModeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateModeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
