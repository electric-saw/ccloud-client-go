package cmk

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
	Basic CmkV2BasicKind = "Basic"
)

func (e CmkV2BasicKind) Valid() bool {
	switch e {
	case Basic:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterApiVersionCmkv2 CmkV2ClusterApiVersion = "cmk/v2"
)

func (e CmkV2ClusterApiVersion) Valid() bool {
	switch e {
	case CmkV2ClusterApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterKindCluster CmkV2ClusterKind = "Cluster"
)

func (e CmkV2ClusterKind) Valid() bool {
	switch e {
	case CmkV2ClusterKindCluster:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterListApiVersionCmkv2 CmkV2ClusterListApiVersion = "cmk/v2"
)

func (e CmkV2ClusterListApiVersion) Valid() bool {
	switch e {
	case CmkV2ClusterListApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterListDataApiVersionCmkv2 CmkV2ClusterListDataApiVersion = "cmk/v2"
)

func (e CmkV2ClusterListDataApiVersion) Valid() bool {
	switch e {
	case CmkV2ClusterListDataApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterListDataKindCluster CmkV2ClusterListDataKind = "Cluster"
)

func (e CmkV2ClusterListDataKind) Valid() bool {
	switch e {
	case CmkV2ClusterListDataKindCluster:
		return true
	default:
		return false
	}
}

const (
	CmkV2ClusterListKindClusterList CmkV2ClusterListKind = "ClusterList"
)

func (e CmkV2ClusterListKind) Valid() bool {
	switch e {
	case CmkV2ClusterListKindClusterList:
		return true
	default:
		return false
	}
}

const (
	Dedicated CmkV2DedicatedKind = "Dedicated"
)

func (e CmkV2DedicatedKind) Valid() bool {
	switch e {
	case Dedicated:
		return true
	default:
		return false
	}
}

const (
	Enterprise CmkV2EnterpriseKind = "Enterprise"
)

func (e CmkV2EnterpriseKind) Valid() bool {
	switch e {
	case Enterprise:
		return true
	default:
		return false
	}
}

const (
	Freight CmkV2FreightKind = "Freight"
)

func (e CmkV2FreightKind) Valid() bool {
	switch e {
	case Freight:
		return true
	default:
		return false
	}
}

const (
	Standard CmkV2StandardKind = "Standard"
)

func (e CmkV2StandardKind) Valid() bool {
	switch e {
	case Standard:
		return true
	default:
		return false
	}
}

const (
	ListCmkV2Clusters200JSONResponseBodyApiVersionCmkv2 ListCmkV2Clusters200JSONResponseBodyApiVersion = "cmk/v2"
)

func (e ListCmkV2Clusters200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCmkV2Clusters200JSONResponseBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	ListCmkV2Clusters200JSONResponseBodyKindClusterList ListCmkV2Clusters200JSONResponseBodyKind = "ClusterList"
)

func (e ListCmkV2Clusters200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCmkV2Clusters200JSONResponseBodyKindClusterList:
		return true
	default:
		return false
	}
}

const (
	CreateCmkV2ClusterJSONBodyApiVersionCmkv2 CreateCmkV2ClusterJSONBodyApiVersion = "cmk/v2"
)

func (e CreateCmkV2ClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCmkV2ClusterJSONBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	CreateCmkV2ClusterJSONBodyKindCluster CreateCmkV2ClusterJSONBodyKind = "Cluster"
)

func (e CreateCmkV2ClusterJSONBodyKind) Valid() bool {
	switch e {
	case CreateCmkV2ClusterJSONBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	CreateCmkV2Cluster202JSONResponseBodyApiVersionCmkv2 CreateCmkV2Cluster202JSONResponseBodyApiVersion = "cmk/v2"
)

func (e CreateCmkV2Cluster202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateCmkV2Cluster202JSONResponseBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	CreateCmkV2Cluster202JSONResponseBodyKindCluster CreateCmkV2Cluster202JSONResponseBodyKind = "Cluster"
)

func (e CreateCmkV2Cluster202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateCmkV2Cluster202JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	GetCmkV2Cluster200JSONResponseBodyApiVersionCmkv2 GetCmkV2Cluster200JSONResponseBodyApiVersion = "cmk/v2"
)

func (e GetCmkV2Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCmkV2Cluster200JSONResponseBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	GetCmkV2Cluster200JSONResponseBodyKindCluster GetCmkV2Cluster200JSONResponseBodyKind = "Cluster"
)

func (e GetCmkV2Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCmkV2Cluster200JSONResponseBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	UpdateCmkV2ClusterJSONBodyApiVersionCmkv2 UpdateCmkV2ClusterJSONBodyApiVersion = "cmk/v2"
)

func (e UpdateCmkV2ClusterJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCmkV2ClusterJSONBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	UpdateCmkV2ClusterJSONBodyKindCluster UpdateCmkV2ClusterJSONBodyKind = "Cluster"
)

func (e UpdateCmkV2ClusterJSONBodyKind) Valid() bool {
	switch e {
	case UpdateCmkV2ClusterJSONBodyKindCluster:
		return true
	default:
		return false
	}
}

const (
	UpdateCmkV2Cluster200JSONResponseBodyApiVersionCmkv2 UpdateCmkV2Cluster200JSONResponseBodyApiVersion = "cmk/v2"
)

func (e UpdateCmkV2Cluster200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCmkV2Cluster200JSONResponseBodyApiVersionCmkv2:
		return true
	default:
		return false
	}
}

const (
	UpdateCmkV2Cluster200JSONResponseBodyKindCluster UpdateCmkV2Cluster200JSONResponseBodyKind = "Cluster"
)

func (e UpdateCmkV2Cluster200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateCmkV2Cluster200JSONResponseBodyKindCluster:
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
type EnvScopedObjectReference struct {
	// Environment Environment of the referred resource, if env-scoped
	Environment *string `json:"environment,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type GlobalObjectReference struct {
	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type MultipleSearchFilter = []string
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
type CmkV2Basic struct {
	// Kind Basic cluster type.
	Kind CmkV2BasicKind `json:"kind"`

	// MaxEcku The maximum number of Elastic Confluent Kafka Units (eCKUs) that Kafka clusters should auto-scale to.
	// For more details, see [Maximum eCKU requirements](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#minimum-maximum-ecku-requirements).
	MaxEcku *int32 `json:"max_ecku,omitempty"`
}
type CmkV2BasicKind string
type CmkV2Cluster struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CmkV2ClusterApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CmkV2ClusterKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Cluster
	Spec *CmkV2ClusterSpec `json:"spec,omitempty"`

	// Status The status of the Cluster
	Status *CmkV2ClusterStatus `json:"status,omitempty"`
}
type CmkV2ClusterApiVersion string
type CmkV2ClusterKind string
type CmkV2ClusterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CmkV2ClusterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CmkV2ClusterListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CmkV2ClusterListDataKind `json:"kind,omitempty"`
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
		Spec map[string]interface{} `json:"spec"`

		// Status The status of the Cluster
		Status CmkV2ClusterStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CmkV2ClusterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CmkV2ClusterListApiVersion string
type CmkV2ClusterListDataApiVersion string
type CmkV2ClusterListDataKind string
type CmkV2ClusterListKind string
type CmkV2ClusterSpec struct {
	// ApiEndpoint The Kafka API cluster endpoint used by Kafka clients to connect to the cluster.
	//
	// DEPRECATED - Please use the `endpoints` attribute instead.
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	ApiEndpoint *string `json:"api_endpoint,omitempty"`

	// Availability The availability zone configuration of the cluster
	Availability *string `json:"availability,omitempty"`

	// Byok Note: For Pre-BYOK v1 clusters, API responses might show both
	// `encryption_key` and `byok`.
	// To manage Pre-BYOK v1 keys, refer to:
	// https://docs.confluent.io/cloud/current/security/encrypt/byok/legacy-byok.html
	// #manage-pre-byok-api-v1-self-managed-encryption-keys
	Byok *GlobalObjectReference `json:"byok,omitempty"`

	// Cloud The cloud service provider in which the cluster is running.
	Cloud *string `json:"cloud,omitempty"`

	// Config The configuration of the Kafka cluster.
	//
	// Note: Clusters can be upgraded from Basic to Standard, but cannot be downgraded from Standard to Basic.
	Config *CmkV2ClusterSpec_Config `json:"config,omitempty"`

	// DeletionProtection Enable deletion protection for the cluster
	DeletionProtection *bool `json:"deletion_protection,omitempty"`

	// DisplayName The name of the cluster.
	DisplayName *string `json:"display_name,omitempty"`

	// Endpoints A map of endpoints for connecting to the Kafka cluster,
	// keyed by access_point_id. Access Point ID 'PUBLIC' and 'PRIVATE_LINK' are reserved.
	// These can be used for different network access methods or regions.
	Endpoints *CmkV2EndpointsMap `json:"endpoints,omitempty"`

	// Environment The environment to which this belongs.
	Environment *EnvScopedObjectReference `json:"environment,omitempty"`

	// HttpEndpoint The cluster HTTP request URL.
	//
	// DEPRECATED - Please use the `endpoints` attribute instead.
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	HttpEndpoint *string `json:"http_endpoint,omitempty"`

	// KafkaBootstrapEndpoint The bootstrap endpoint used by Kafka clients to connect to the cluster.
	//
	// DEPRECATED - Please use the `endpoints` attribute instead.
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	KafkaBootstrapEndpoint *string `json:"kafka_bootstrap_endpoint,omitempty"`

	// Network The network associated with this object.
	Network *EnvScopedObjectReference `json:"network,omitempty"`

	// Region The cloud service provider region where the cluster is running.
	Region *string `json:"region,omitempty"`
}
type CmkV2ClusterSpec_Config struct {
	union json.RawMessage
}
type CmkV2ClusterStatus struct {
	// Cku The number of Confluent Kafka Units (CKUs) the Dedicated cluster currently has.
	Cku *int32 `json:"cku,omitempty"`

	// Phase The lifecyle phase of the cluster:
	//   PROVISIONED:  cluster is provisioned;
	//   PROVISIONING:  cluster provisioning is in progress;
	//   FAILED:  provisioning failed
	Phase string `json:"phase"`
}
type CmkV2Dedicated struct {
	// Cku The number of Confluent Kafka Units (CKUs) for Dedicated cluster types.
	// MULTI_ZONE dedicated clusters must have at least two CKUs.
	Cku int32 `json:"cku"`

	// EncryptionKey The id of the encryption key that is used to encrypt the data in the Kafka cluster.
	// (e.g. for Amazon Web Services, the Amazon Resource Name of the key).
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	EncryptionKey *string `json:"encryption_key,omitempty"`

	// Kind Dedicated cluster type.
	Kind CmkV2DedicatedKind `json:"kind"`

	// ReleasePriority Specifies the release priority for cluster updates. Defaults to REGULAR. Clusters with PRIORITY are updated before clusters with REGULAR.
	ReleasePriority *string `json:"release_priority,omitempty"`

	// Zones The list of zones the cluster is in.
	//
	// On AWS, zones are AWS [AZ IDs](https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html)
	//  (e.g. use1-az3)
	//
	// On GCP, zones are GCP [zones](https://cloud.google.com/compute/docs/regions-zones)
	//  (e.g. us-central1-c).
	Zones *[]string `json:"zones,omitempty"`
}
type CmkV2DedicatedKind string
type CmkV2Endpoints struct {
	// ConnectionType The type of connection used for the endpoint.
	ConnectionType string `json:"connection_type"`

	// HttpEndpoint The REST endpoint for the Kafka cluster.
	HttpEndpoint string `json:"http_endpoint"`

	// KafkaBootstrapEndpoint The bootstrap endpoint used by Kafka clients to connect to the cluster.
	KafkaBootstrapEndpoint string `json:"kafka_bootstrap_endpoint"`
}
type CmkV2EndpointsMap map[string]CmkV2Endpoints
type CmkV2Enterprise struct {
	// Kind Enterprise cluster type.
	Kind CmkV2EnterpriseKind `json:"kind"`

	// MaxEcku The maximum number of Elastic Confluent Kafka Units (eCKUs) that Kafka clusters should auto-scale to.
	// Kafka clusters with `HIGH` availability must have at least two eCKUs.
	// For more details, see [Maximum eCKU requirements](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#minimum-maximum-ecku-requirements).
	MaxEcku *int32 `json:"max_ecku,omitempty"`
}
type CmkV2EnterpriseKind string
type CmkV2Freight struct {
	// Kind Freight cluster type.
	Kind CmkV2FreightKind `json:"kind"`

	// MaxEcku The maximum number of Elastic Confluent Kafka Units (eCKUs) that Kafka clusters should auto-scale to.
	// Kafka clusters with `HIGH` availability must have at least two eCKUs.
	// For more details, see [Maximum eCKU requirements](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#minimum-maximum-ecku-requirements).
	MaxEcku *int32 `json:"max_ecku,omitempty"`

	// Zones The list of zones the cluster is in.
	//
	// On AWS, zones are AWS [AZ IDs](https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html)
	//  (e.g. use1-az3)
	//
	// On GCP, zones are GCP [zones](https://cloud.google.com/compute/docs/regions-zones)
	//  (e.g. us-central1-c).
	Zones *[]string `json:"zones,omitempty"`
}
type CmkV2FreightKind string
type CmkV2Standard struct {
	// Kind Standard cluster type.
	Kind CmkV2StandardKind `json:"kind"`

	// MaxEcku The maximum number of Elastic Confluent Kafka Units (eCKUs) that Kafka clusters should auto-scale to.
	// Kafka clusters with `HIGH` availability must have at least two eCKUs.
	// For more details, see [Maximum eCKU requirements](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#minimum-maximum-ecku-requirements).
	MaxEcku *int32 `json:"max_ecku,omitempty"`
}
type CmkV2StandardKind string
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
func (t CmkV2ClusterSpec_Config) AsCmkV2Basic() (CmkV2Basic, error) {
	var body CmkV2Basic
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CmkV2ClusterSpec_Config) FromCmkV2Basic(v CmkV2Basic) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Basic"}`))
	t.union = b
	return err
}
func (t *CmkV2ClusterSpec_Config) MergeCmkV2Basic(v CmkV2Basic) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Basic"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CmkV2ClusterSpec_Config) AsCmkV2Standard() (CmkV2Standard, error) {
	var body CmkV2Standard
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CmkV2ClusterSpec_Config) FromCmkV2Standard(v CmkV2Standard) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Standard"}`))
	t.union = b
	return err
}
func (t *CmkV2ClusterSpec_Config) MergeCmkV2Standard(v CmkV2Standard) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Standard"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CmkV2ClusterSpec_Config) AsCmkV2Dedicated() (CmkV2Dedicated, error) {
	var body CmkV2Dedicated
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CmkV2ClusterSpec_Config) FromCmkV2Dedicated(v CmkV2Dedicated) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Dedicated"}`))
	t.union = b
	return err
}
func (t *CmkV2ClusterSpec_Config) MergeCmkV2Dedicated(v CmkV2Dedicated) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Dedicated"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CmkV2ClusterSpec_Config) AsCmkV2Enterprise() (CmkV2Enterprise, error) {
	var body CmkV2Enterprise
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CmkV2ClusterSpec_Config) FromCmkV2Enterprise(v CmkV2Enterprise) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Enterprise"}`))
	t.union = b
	return err
}
func (t *CmkV2ClusterSpec_Config) MergeCmkV2Enterprise(v CmkV2Enterprise) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Enterprise"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CmkV2ClusterSpec_Config) AsCmkV2Freight() (CmkV2Freight, error) {
	var body CmkV2Freight
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CmkV2ClusterSpec_Config) FromCmkV2Freight(v CmkV2Freight) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Freight"}`))
	t.union = b
	return err
}
func (t *CmkV2ClusterSpec_Config) MergeCmkV2Freight(v CmkV2Freight) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Freight"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CmkV2ClusterSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CmkV2ClusterSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Basic":
		return t.AsCmkV2Basic()
	case "Dedicated":
		return t.AsCmkV2Dedicated()
	case "Enterprise":
		return t.AsCmkV2Enterprise()
	case "Freight":
		return t.AsCmkV2Freight()
	case "Standard":
		return t.AsCmkV2Standard()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CmkV2ClusterSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CmkV2ClusterSpec_Config) UnmarshalJSON(b []byte) error {
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

	// ListCmkV2Clusters List of Clusters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Corresponds with GET /cmk/v2/clusters (the `ListCmkV2Clusters` operationId).
	ListCmkV2Clusters(ctx context.Context, params *ListCmkV2ClustersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCmkV2ClusterWithBody Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cmk/v2/clusters (the `CreateCmkV2Cluster` operationId).
	CreateCmkV2ClusterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCmkV2Cluster Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cmk/v2/clusters (the `CreateCmkV2Cluster` operationId).
	CreateCmkV2Cluster(ctx context.Context, body CreateCmkV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCmkV2Cluster Delete a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a cluster.
	//
	// Corresponds with DELETE /cmk/v2/clusters/{id} (the `DeleteCmkV2Cluster` operationId).
	DeleteCmkV2Cluster(ctx context.Context, id string, params *DeleteCmkV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCmkV2Cluster Read a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Corresponds with GET /cmk/v2/clusters/{id} (the `GetCmkV2Cluster` operationId).
	GetCmkV2Cluster(ctx context.Context, id string, params *GetCmkV2ClusterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCmkV2ClusterWithBody Update a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /cmk/v2/clusters/{id} (the `UpdateCmkV2Cluster` operationId).
	UpdateCmkV2ClusterWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCmkV2Cluster Update a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /cmk/v2/clusters/{id} (the `UpdateCmkV2Cluster` operationId).
	UpdateCmkV2Cluster(ctx context.Context, id string, body UpdateCmkV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListCmkV2ClustersWithResponse List of Clusters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all clusters.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cmk/v2/clusters (the `ListCmkV2Clusters` operationId).
	ListCmkV2ClustersWithResponse(ctx context.Context, params *ListCmkV2ClustersParams, reqEditors ...RequestEditorFn) (*ListCmkV2ClustersResponse, error)

	// CreateCmkV2ClusterWithBodyWithResponse Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cmk/v2/clusters (the `CreateCmkV2Cluster` operationId).
	CreateCmkV2ClusterWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCmkV2ClusterResponse, error)

	// CreateCmkV2ClusterWithResponse Create a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a cluster.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cmk/v2/clusters (the `CreateCmkV2Cluster` operationId).
	CreateCmkV2ClusterWithResponse(ctx context.Context, body CreateCmkV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCmkV2ClusterResponse, error)

	// DeleteCmkV2ClusterWithResponse Delete a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /cmk/v2/clusters/{id} (the `DeleteCmkV2Cluster` operationId).
	DeleteCmkV2ClusterWithResponse(ctx context.Context, id string, params *DeleteCmkV2ClusterParams, reqEditors ...RequestEditorFn) (*DeleteCmkV2ClusterResponse, error)

	// GetCmkV2ClusterWithResponse Read a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cmk/v2/clusters/{id} (the `GetCmkV2Cluster` operationId).
	GetCmkV2ClusterWithResponse(ctx context.Context, id string, params *GetCmkV2ClusterParams, reqEditors ...RequestEditorFn) (*GetCmkV2ClusterResponse, error)

	// UpdateCmkV2ClusterWithBodyWithResponse Update a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cmk/v2/clusters/{id} (the `UpdateCmkV2Cluster` operationId).
	UpdateCmkV2ClusterWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateCmkV2ClusterResponse, error)

	// UpdateCmkV2ClusterWithResponse Update a Cluster
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a cluster.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cmk/v2/clusters/{id} (the `UpdateCmkV2Cluster` operationId).
	UpdateCmkV2ClusterWithResponse(ctx context.Context, id string, body UpdateCmkV2ClusterJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateCmkV2ClusterResponse, error)
}

func (r ListCmkV2ClustersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCmkV2Clusters200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Byok        interface{} `json:"byok,omitempty"`
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCmkV2Clusters200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
} {
	return r.JSON200
}
func (r ListCmkV2ClustersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCmkV2ClustersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCmkV2ClustersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCmkV2ClustersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCmkV2ClustersResponse) GetBody() []byte {
	return r.Body
}
func (r ListCmkV2ClustersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCmkV2ClustersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCmkV2ClustersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCmkV2ClusterResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateCmkV2Cluster202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateCmkV2Cluster202JSONResponseBodyKind `json:"kind,omitempty"`
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
	Spec struct {
		Byok        interface{} `json:"byok,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status CmkV2ClusterStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateCmkV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCmkV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCmkV2ClusterResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateCmkV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCmkV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCmkV2ClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCmkV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCmkV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCmkV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCmkV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCmkV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCmkV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCmkV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCmkV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCmkV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCmkV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r DeleteCmkV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCmkV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCmkV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCmkV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCmkV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCmkV2ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCmkV2Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCmkV2Cluster200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Byok        interface{} `json:"byok,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status CmkV2ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetCmkV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCmkV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCmkV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCmkV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCmkV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCmkV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetCmkV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCmkV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCmkV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateCmkV2ClusterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateCmkV2Cluster200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateCmkV2Cluster200JSONResponseBodyKind `json:"kind"`
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
	Spec struct {
		Byok        interface{} `json:"byok,omitempty"`
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Cluster
	Status CmkV2ClusterStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateCmkV2ClusterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateCmkV2ClusterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateCmkV2ClusterResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateCmkV2ClusterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateCmkV2ClusterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateCmkV2ClusterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateCmkV2ClusterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateCmkV2ClusterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateCmkV2ClusterResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateCmkV2ClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateCmkV2ClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateCmkV2ClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
