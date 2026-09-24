package cdx

import (
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
	openapi_types "github.com/oapi-codegen/runtime/types"
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
	AwsNetwork CdxV1AwsNetworkKind = "AwsNetwork"
)

func (e CdxV1AwsNetworkKind) Valid() bool {
	switch e {
	case AwsNetwork:
		return true
	default:
		return false
	}
}

const (
	AzureNetwork CdxV1AzureNetworkKind = "AzureNetwork"
)

func (e CdxV1AzureNetworkKind) Valid() bool {
	switch e {
	case AzureNetwork:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerShareApiVersionCdxv1 CdxV1ConsumerShareApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerShareApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerShareApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerShareKindConsumerShare CdxV1ConsumerShareKind = "ConsumerShare"
)

func (e CdxV1ConsumerShareKind) Valid() bool {
	switch e {
	case CdxV1ConsumerShareKindConsumerShare:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerShareListApiVersionCdxv1 CdxV1ConsumerShareListApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerShareListApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerShareListApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerShareListDataApiVersionCdxv1 CdxV1ConsumerShareListDataApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerShareListDataApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerShareListDataApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerShareListDataKindConsumerShare CdxV1ConsumerShareListDataKind = "ConsumerShare"
)

func (e CdxV1ConsumerShareListDataKind) Valid() bool {
	switch e {
	case CdxV1ConsumerShareListDataKindConsumerShare:
		return true
	default:
		return false
	}
}

const (
	ConsumerShareList CdxV1ConsumerShareListKind = "ConsumerShareList"
)

func (e CdxV1ConsumerShareListKind) Valid() bool {
	switch e {
	case ConsumerShareList:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerSharedResourceApiVersionCdxv1 CdxV1ConsumerSharedResourceApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerSharedResourceApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerSharedResourceApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerSharedResourceKindConsumerSharedResource CdxV1ConsumerSharedResourceKind = "ConsumerSharedResource"
)

func (e CdxV1ConsumerSharedResourceKind) Valid() bool {
	switch e {
	case CdxV1ConsumerSharedResourceKindConsumerSharedResource:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerSharedResourceListApiVersionCdxv1 CdxV1ConsumerSharedResourceListApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerSharedResourceListApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerSharedResourceListApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerSharedResourceListDataApiVersionCdxv1 CdxV1ConsumerSharedResourceListDataApiVersion = "cdx/v1"
)

func (e CdxV1ConsumerSharedResourceListDataApiVersion) Valid() bool {
	switch e {
	case CdxV1ConsumerSharedResourceListDataApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ConsumerSharedResourceListDataKindConsumerSharedResource CdxV1ConsumerSharedResourceListDataKind = "ConsumerSharedResource"
)

func (e CdxV1ConsumerSharedResourceListDataKind) Valid() bool {
	switch e {
	case CdxV1ConsumerSharedResourceListDataKindConsumerSharedResource:
		return true
	default:
		return false
	}
}

const (
	ConsumerSharedResourceList CdxV1ConsumerSharedResourceListKind = "ConsumerSharedResourceList"
)

func (e CdxV1ConsumerSharedResourceListKind) Valid() bool {
	switch e {
	case ConsumerSharedResourceList:
		return true
	default:
		return false
	}
}

const (
	CdxV1CreateProviderShareRequestApiVersionCdxv1 CdxV1CreateProviderShareRequestApiVersion = "cdx/v1"
)

func (e CdxV1CreateProviderShareRequestApiVersion) Valid() bool {
	switch e {
	case CdxV1CreateProviderShareRequestApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1CreateProviderShareRequestKindCreateProviderShareRequest CdxV1CreateProviderShareRequestKind = "CreateProviderShareRequest"
)

func (e CdxV1CreateProviderShareRequestKind) Valid() bool {
	switch e {
	case CdxV1CreateProviderShareRequestKindCreateProviderShareRequest:
		return true
	default:
		return false
	}
}

const (
	Email CdxV1EmailConsumerRestrictionKind = "Email"
)

func (e CdxV1EmailConsumerRestrictionKind) Valid() bool {
	switch e {
	case Email:
		return true
	default:
		return false
	}
}

const (
	GcpNetwork CdxV1GcpNetworkKind = "GcpNetwork"
)

func (e CdxV1GcpNetworkKind) Valid() bool {
	switch e {
	case GcpNetwork:
		return true
	default:
		return false
	}
}

const (
	CdxV1NetworkApiVersionCdxv1 CdxV1NetworkApiVersion = "cdx/v1"
)

func (e CdxV1NetworkApiVersion) Valid() bool {
	switch e {
	case CdxV1NetworkApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	Network CdxV1NetworkKind = "Network"
)

func (e CdxV1NetworkKind) Valid() bool {
	switch e {
	case Network:
		return true
	default:
		return false
	}
}

const (
	CdxV1OptInApiVersionCdxv1 CdxV1OptInApiVersion = "cdx/v1"
)

func (e CdxV1OptInApiVersion) Valid() bool {
	switch e {
	case CdxV1OptInApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1OptInKindOptIn CdxV1OptInKind = "OptIn"
)

func (e CdxV1OptInKind) Valid() bool {
	switch e {
	case CdxV1OptInKindOptIn:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareApiVersionCdxv1 CdxV1ProviderShareApiVersion = "cdx/v1"
)

func (e CdxV1ProviderShareApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderShareApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareKindProviderShare CdxV1ProviderShareKind = "ProviderShare"
)

func (e CdxV1ProviderShareKind) Valid() bool {
	switch e {
	case CdxV1ProviderShareKindProviderShare:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareListApiVersionCdxv1 CdxV1ProviderShareListApiVersion = "cdx/v1"
)

func (e CdxV1ProviderShareListApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderShareListApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareListDataApiVersionCdxv1 CdxV1ProviderShareListDataApiVersion = "cdx/v1"
)

func (e CdxV1ProviderShareListDataApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderShareListDataApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareListDataKindProviderShare CdxV1ProviderShareListDataKind = "ProviderShare"
)

func (e CdxV1ProviderShareListDataKind) Valid() bool {
	switch e {
	case CdxV1ProviderShareListDataKindProviderShare:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderShareListKindProviderShareList CdxV1ProviderShareListKind = "ProviderShareList"
)

func (e CdxV1ProviderShareListKind) Valid() bool {
	switch e {
	case CdxV1ProviderShareListKindProviderShareList:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceApiVersionCdxv1 CdxV1ProviderSharedResourceApiVersion = "cdx/v1"
)

func (e CdxV1ProviderSharedResourceApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceKindProviderSharedResource CdxV1ProviderSharedResourceKind = "ProviderSharedResource"
)

func (e CdxV1ProviderSharedResourceKind) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceKindProviderSharedResource:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceListApiVersionCdxv1 CdxV1ProviderSharedResourceListApiVersion = "cdx/v1"
)

func (e CdxV1ProviderSharedResourceListApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceListApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceListDataApiVersionCdxv1 CdxV1ProviderSharedResourceListDataApiVersion = "cdx/v1"
)

func (e CdxV1ProviderSharedResourceListDataApiVersion) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceListDataApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceListDataKindProviderSharedResource CdxV1ProviderSharedResourceListDataKind = "ProviderSharedResource"
)

func (e CdxV1ProviderSharedResourceListDataKind) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceListDataKindProviderSharedResource:
		return true
	default:
		return false
	}
}

const (
	CdxV1ProviderSharedResourceListKindProviderSharedResourceList CdxV1ProviderSharedResourceListKind = "ProviderSharedResourceList"
)

func (e CdxV1ProviderSharedResourceListKind) Valid() bool {
	switch e {
	case CdxV1ProviderSharedResourceListKindProviderSharedResourceList:
		return true
	default:
		return false
	}
}

const (
	CdxV1RedeemTokenRequestApiVersionCdxv1 CdxV1RedeemTokenRequestApiVersion = "cdx/v1"
)

func (e CdxV1RedeemTokenRequestApiVersion) Valid() bool {
	switch e {
	case CdxV1RedeemTokenRequestApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1RedeemTokenRequestKindRedeemTokenRequest CdxV1RedeemTokenRequestKind = "RedeemTokenRequest"
)

func (e CdxV1RedeemTokenRequestKind) Valid() bool {
	switch e {
	case CdxV1RedeemTokenRequestKindRedeemTokenRequest:
		return true
	default:
		return false
	}
}

const (
	CdxV1RedeemTokenResponseApiVersionCdxv1 CdxV1RedeemTokenResponseApiVersion = "cdx/v1"
)

func (e CdxV1RedeemTokenResponseApiVersion) Valid() bool {
	switch e {
	case CdxV1RedeemTokenResponseApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	RedeemTokenResponse CdxV1RedeemTokenResponseKind = "RedeemTokenResponse"
)

func (e CdxV1RedeemTokenResponseKind) Valid() bool {
	switch e {
	case RedeemTokenResponse:
		return true
	default:
		return false
	}
}

const (
	Group CdxV1SharedGroupKind = "Group"
)

func (e CdxV1SharedGroupKind) Valid() bool {
	switch e {
	case Group:
		return true
	default:
		return false
	}
}

const (
	Subject CdxV1SharedSubjectKind = "Subject"
)

func (e CdxV1SharedSubjectKind) Valid() bool {
	switch e {
	case Subject:
		return true
	default:
		return false
	}
}

const (
	CdxV1SharedTokenApiVersionCdxv1 CdxV1SharedTokenApiVersion = "cdx/v1"
)

func (e CdxV1SharedTokenApiVersion) Valid() bool {
	switch e {
	case CdxV1SharedTokenApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CdxV1SharedTokenKindSharedToken CdxV1SharedTokenKind = "SharedToken"
)

func (e CdxV1SharedTokenKind) Valid() bool {
	switch e {
	case CdxV1SharedTokenKindSharedToken:
		return true
	default:
		return false
	}
}

const (
	Topic CdxV1SharedTopicKind = "Topic"
)

func (e CdxV1SharedTopicKind) Valid() bool {
	switch e {
	case Topic:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ConsumerSharedResource200JSONResponseBodyApiVersionCdxv1 GetCdxV1ConsumerSharedResource200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e GetCdxV1ConsumerSharedResource200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCdxV1ConsumerSharedResource200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ConsumerSharedResource200JSONResponseBodyKindConsumerSharedResource GetCdxV1ConsumerSharedResource200JSONResponseBodyKind = "ConsumerSharedResource"
)

func (e GetCdxV1ConsumerSharedResource200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCdxV1ConsumerSharedResource200JSONResponseBodyKindConsumerSharedResource:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ConsumerShare200JSONResponseBodyApiVersionCdxv1 GetCdxV1ConsumerShare200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e GetCdxV1ConsumerShare200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCdxV1ConsumerShare200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ConsumerShare200JSONResponseBodyKindConsumerShare GetCdxV1ConsumerShare200JSONResponseBodyKind = "ConsumerShare"
)

func (e GetCdxV1ConsumerShare200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCdxV1ConsumerShare200JSONResponseBodyKindConsumerShare:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1OptIn200JSONResponseBodyApiVersionCdxv1 GetCdxV1OptIn200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e GetCdxV1OptIn200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCdxV1OptIn200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1OptIn200JSONResponseBodyKindOptIn GetCdxV1OptIn200JSONResponseBodyKind = "OptIn"
)

func (e GetCdxV1OptIn200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCdxV1OptIn200JSONResponseBodyKindOptIn:
		return true
	default:
		return false
	}
}

const (
	UpdateCdxV1OptIn200JSONResponseBodyApiVersionCdxv1 UpdateCdxV1OptIn200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e UpdateCdxV1OptIn200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCdxV1OptIn200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCdxV1OptIn200JSONResponseBodyKindOptIn UpdateCdxV1OptIn200JSONResponseBodyKind = "OptIn"
)

func (e UpdateCdxV1OptIn200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateCdxV1OptIn200JSONResponseBodyKindOptIn:
		return true
	default:
		return false
	}
}

const (
	ListCdxV1ProviderSharedResources200JSONResponseBodyApiVersionCdxv1 ListCdxV1ProviderSharedResources200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e ListCdxV1ProviderSharedResources200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCdxV1ProviderSharedResources200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	ListCdxV1ProviderSharedResources200JSONResponseBodyKindProviderSharedResourceList ListCdxV1ProviderSharedResources200JSONResponseBodyKind = "ProviderSharedResourceList"
)

func (e ListCdxV1ProviderSharedResources200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCdxV1ProviderSharedResources200JSONResponseBodyKindProviderSharedResourceList:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ProviderSharedResource200JSONResponseBodyApiVersionCdxv1 GetCdxV1ProviderSharedResource200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e GetCdxV1ProviderSharedResource200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCdxV1ProviderSharedResource200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ProviderSharedResource200JSONResponseBodyKindProviderSharedResource GetCdxV1ProviderSharedResource200JSONResponseBodyKind = "ProviderSharedResource"
)

func (e GetCdxV1ProviderSharedResource200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCdxV1ProviderSharedResource200JSONResponseBodyKindProviderSharedResource:
		return true
	default:
		return false
	}
}

const (
	UpdateCdxV1ProviderSharedResource200JSONResponseBodyApiVersionCdxv1 UpdateCdxV1ProviderSharedResource200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e UpdateCdxV1ProviderSharedResource200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateCdxV1ProviderSharedResource200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	UpdateCdxV1ProviderSharedResource200JSONResponseBodyKindProviderSharedResource UpdateCdxV1ProviderSharedResource200JSONResponseBodyKind = "ProviderSharedResource"
)

func (e UpdateCdxV1ProviderSharedResource200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateCdxV1ProviderSharedResource200JSONResponseBodyKindProviderSharedResource:
		return true
	default:
		return false
	}
}

const (
	ListCdxV1ProviderShares200JSONResponseBodyApiVersionCdxv1 ListCdxV1ProviderShares200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e ListCdxV1ProviderShares200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListCdxV1ProviderShares200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	ListCdxV1ProviderShares200JSONResponseBodyKindProviderShareList ListCdxV1ProviderShares200JSONResponseBodyKind = "ProviderShareList"
)

func (e ListCdxV1ProviderShares200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListCdxV1ProviderShares200JSONResponseBodyKindProviderShareList:
		return true
	default:
		return false
	}
}

const (
	CreateCdxV1ProviderShareJSONBodyApiVersionCdxv1 CreateCdxV1ProviderShareJSONBodyApiVersion = "cdx/v1"
)

func (e CreateCdxV1ProviderShareJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateCdxV1ProviderShareJSONBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	CreateCdxV1ProviderShareJSONBodyKindCreateProviderShareRequest CreateCdxV1ProviderShareJSONBodyKind = "CreateProviderShareRequest"
)

func (e CreateCdxV1ProviderShareJSONBodyKind) Valid() bool {
	switch e {
	case CreateCdxV1ProviderShareJSONBodyKindCreateProviderShareRequest:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ProviderShare200JSONResponseBodyApiVersionCdxv1 GetCdxV1ProviderShare200JSONResponseBodyApiVersion = "cdx/v1"
)

func (e GetCdxV1ProviderShare200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetCdxV1ProviderShare200JSONResponseBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	GetCdxV1ProviderShare200JSONResponseBodyKindProviderShare GetCdxV1ProviderShare200JSONResponseBodyKind = "ProviderShare"
)

func (e GetCdxV1ProviderShare200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetCdxV1ProviderShare200JSONResponseBodyKindProviderShare:
		return true
	default:
		return false
	}
}

const (
	RedeemCdxV1SharedTokenJSONBodyApiVersionCdxv1 RedeemCdxV1SharedTokenJSONBodyApiVersion = "cdx/v1"
)

func (e RedeemCdxV1SharedTokenJSONBodyApiVersion) Valid() bool {
	switch e {
	case RedeemCdxV1SharedTokenJSONBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	RedeemCdxV1SharedTokenJSONBodyKindRedeemTokenRequest RedeemCdxV1SharedTokenJSONBodyKind = "RedeemTokenRequest"
)

func (e RedeemCdxV1SharedTokenJSONBodyKind) Valid() bool {
	switch e {
	case RedeemCdxV1SharedTokenJSONBodyKindRedeemTokenRequest:
		return true
	default:
		return false
	}
}

const (
	ResourcesCdxV1SharedTokenJSONBodyApiVersionCdxv1 ResourcesCdxV1SharedTokenJSONBodyApiVersion = "cdx/v1"
)

func (e ResourcesCdxV1SharedTokenJSONBodyApiVersion) Valid() bool {
	switch e {
	case ResourcesCdxV1SharedTokenJSONBodyApiVersionCdxv1:
		return true
	default:
		return false
	}
}

const (
	ResourcesCdxV1SharedTokenJSONBodyKindSharedToken ResourcesCdxV1SharedTokenJSONBodyKind = "SharedToken"
)

func (e ResourcesCdxV1SharedTokenJSONBodyKind) Valid() bool {
	switch e {
	case ResourcesCdxV1SharedTokenJSONBodyKindSharedToken:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type BooleanFilter = bool
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
type CdxV1AwsNetwork struct {
	// Kind Network kind type.
	Kind CdxV1AwsNetworkKind `json:"kind"`

	// PrivateLinkEndpointService The AWS VPC endpoint service for the network (used for PrivateLink) if available.
	PrivateLinkEndpointService *string `json:"private_link_endpoint_service,omitempty"`
}
type CdxV1AwsNetworkKind string
type CdxV1AzureNetwork struct {
	// Kind Network kind type.
	Kind CdxV1AzureNetworkKind `json:"kind"`

	// PrivateLinkServiceAliases The mapping of zones to PrivateLink Service Aliases if available.  Keys are zones
	// and values are [Azure PrivateLink Service
	// Aliases](https://docs.microsoft.com/en-us/azure/private-link/private-link-service-overview#share-your-service)
	PrivateLinkServiceAliases *map[string]string `json:"private_link_service_aliases,omitempty"`
}
type CdxV1AzureNetworkKind string
type CdxV1ConnectionType = string
type CdxV1ConsumerShare struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1ConsumerShareApiVersion `json:"api_version,omitempty"`

	// ConsumerOrganizationName Consumer organization name. Deprecated
	ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

	// ConsumerUser The consumer user/invitee
	ConsumerUser *GlobalObjectReference `json:"consumer_user,omitempty"`

	// ConsumerUserName Name of the consumer. Deprecated
	ConsumerUserName *string `json:"consumer_user_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
	InviteExpiresAt *time.Time `json:"invite_expires_at,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1ConsumerShareKind `json:"kind,omitempty"`
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

	// ProviderOrganizationName Provider organization name
	ProviderOrganizationName *string `json:"provider_organization_name,omitempty"`

	// ProviderUserName Name or email of the provider user
	ProviderUserName *string `json:"provider_user_name,omitempty"`

	// Status The status of the Consumer Share
	Status *CdxV1ConsumerShareStatus `json:"status,omitempty"`
}
type CdxV1ConsumerShareApiVersion string
type CdxV1ConsumerShareKind string
type CdxV1ConsumerShareList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CdxV1ConsumerShareListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CdxV1ConsumerShareListDataApiVersion `json:"api_version,omitempty"`

		// ConsumerOrganizationName Consumer organization name. Deprecated
		ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

		// ConsumerUser The consumer user/invitee
		ConsumerUser GlobalObjectReference `json:"consumer_user"`

		// ConsumerUserName Name of the consumer. Deprecated
		ConsumerUserName *string `json:"consumer_user_name,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
		InviteExpiresAt *time.Time `json:"invite_expires_at,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CdxV1ConsumerShareListDataKind `json:"kind,omitempty"`
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

		// ProviderOrganizationName Provider organization name
		ProviderOrganizationName string `json:"provider_organization_name"`

		// ProviderUserName Name or email of the provider user
		ProviderUserName string `json:"provider_user_name"`

		// Status The status of the Consumer Share
		Status CdxV1ConsumerShareStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CdxV1ConsumerShareListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CdxV1ConsumerShareListApiVersion string
type CdxV1ConsumerShareListDataApiVersion string
type CdxV1ConsumerShareListDataKind string
type CdxV1ConsumerShareListKind string
type CdxV1ConsumerShareStatus struct {
	// Phase Status of share
	Phase string `json:"phase"`
}
type CdxV1ConsumerSharedResource struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1ConsumerSharedResourceApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider of the provider shared cluster.
	Cloud *string `json:"cloud,omitempty"`

	// Description Description of consumer resource
	Description *string `json:"description,omitempty"`

	// DisplayName Consumer resource display name
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CdxV1ConsumerSharedResourceKind `json:"kind,omitempty"`

	// LogoUrl Resource logo url
	LogoUrl  *string `json:"logo_url,omitempty"`
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

	// NetworkConnectionTypes The network connection types of the provider shared cluster. If the shared cluster is on public internet,
	// then the list will be empty
	NetworkConnectionTypes *[]CdxV1ConnectionType `json:"network_connection_types,omitempty"`

	// OrganizationContact Email of the shared resource's organization contact
	OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

	// OrganizationDescription Shared resource's organization description
	OrganizationDescription *string `json:"organization_description,omitempty"`

	// OrganizationName Shared resource's organization name
	OrganizationName *string `json:"organization_name,omitempty"`

	// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
	Schemas *[]struct {
		// Id Globally unique identifier of the schema
		Id *int32 `json:"id,omitempty"`

		// Schema Schema definition string
		Schema *string `json:"schema,omitempty"`

		// SchemaType Schema type
		SchemaType *string `json:"schema_type,omitempty"`

		// Subject Name of the subject
		Subject *string `json:"subject,omitempty"`

		// Version Version number
		Version *int32 `json:"version,omitempty"`
	} `json:"schemas,omitempty"`

	// Tags list of tags
	Tags *[]string `json:"tags,omitempty"`
}
type CdxV1ConsumerSharedResourceApiVersion string
type CdxV1ConsumerSharedResourceKind string
type CdxV1ConsumerSharedResourceList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CdxV1ConsumerSharedResourceListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CdxV1ConsumerSharedResourceListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider of the provider shared cluster.
		Cloud string `json:"cloud"`

		// Description Description of consumer resource
		Description *string `json:"description,omitempty"`

		// DisplayName Consumer resource display name
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *CdxV1ConsumerSharedResourceListDataKind `json:"kind,omitempty"`

		// LogoUrl Resource logo url
		LogoUrl  *string `json:"logo_url,omitempty"`
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

		// NetworkConnectionTypes The network connection types of the provider shared cluster. If the shared cluster is on public internet,
		// then the list will be empty
		NetworkConnectionTypes *[]CdxV1ConnectionType `json:"network_connection_types,omitempty"`

		// OrganizationContact Email of the shared resource's organization contact
		OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

		// OrganizationDescription Shared resource's organization description
		OrganizationDescription *string `json:"organization_description,omitempty"`

		// OrganizationName Shared resource's organization name
		OrganizationName string `json:"organization_name"`

		// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
		Schemas *[]struct {
			// Id Globally unique identifier of the schema
			Id *int32 `json:"id,omitempty"`

			// Schema Schema definition string
			Schema *string `json:"schema,omitempty"`

			// SchemaType Schema type
			SchemaType *string `json:"schema_type,omitempty"`

			// Subject Name of the subject
			Subject *string `json:"subject,omitempty"`

			// Version Version number
			Version *int32 `json:"version,omitempty"`
		} `json:"schemas,omitempty"`

		// Tags list of tags
		Tags *[]string `json:"tags,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CdxV1ConsumerSharedResourceListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CdxV1ConsumerSharedResourceListApiVersion string
type CdxV1ConsumerSharedResourceListDataApiVersion string
type CdxV1ConsumerSharedResourceListDataKind string
type CdxV1ConsumerSharedResourceListKind string
type CdxV1CreateProviderShareRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1CreateProviderShareRequestApiVersion `json:"api_version,omitempty"`

	// ConsumerRestriction Restrictions on the consumer that can redeem this token
	ConsumerRestriction *CdxV1CreateProviderShareRequest_ConsumerRestriction `json:"consumer_restriction,omitempty"`

	// DeliveryMethod Method by which the invite will be delivered
	DeliveryMethod *string `json:"delivery_method,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1CreateProviderShareRequestKind `json:"kind,omitempty"`
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

	// Resources List of resource crns to be shared
	Resources *[]string `json:"resources,omitempty"`
}
type CdxV1CreateProviderShareRequestApiVersion string
type CdxV1CreateProviderShareRequest_ConsumerRestriction struct {
	union json.RawMessage
}
type CdxV1CreateProviderShareRequestKind string
type CdxV1EmailConsumerRestriction struct {
	// Email Email based matching for the consumers
	Email openapi_types.Email `json:"email"`

	// Kind The resource kind
	Kind CdxV1EmailConsumerRestrictionKind `json:"kind"`
}
type CdxV1EmailConsumerRestrictionKind string
type CdxV1GcpNetwork struct {
	// Kind Network kind type.
	Kind CdxV1GcpNetworkKind `json:"kind"`

	// PrivateServiceConnectServiceAttachments The mapping of zones to Private Service Connect Service
	// Attachments if available. Keys are zones and values are
	// [GCP Private Service Connect Service
	// Attachment](https://cloud.google.com/vpc/docs/configure-private-service-connect-producer#api_7)
	PrivateServiceConnectServiceAttachments *map[string]string `json:"private_service_connect_service_attachments,omitempty"`
}
type CdxV1GcpNetworkKind string
type CdxV1Network struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1NetworkApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud-specific network details. These will be populated when the network reaches the READY state.
	Cloud *CdxV1Network_Cloud `json:"cloud,omitempty"`

	// DnsDomain The root DNS domain for the network if applicable.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// KafkaBootstrapUrl The kafka cluster bootstrap url
	KafkaBootstrapUrl *string `json:"kafka_bootstrap_url,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1NetworkKind `json:"kind,omitempty"`
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

	// ZonalSubdomains The DNS subdomain for each zone. Present on networks that support PrivateLink. Keys are zones and
	// values are DNS domains.
	ZonalSubdomains *map[string]string `json:"zonal_subdomains,omitempty"`

	// Zones The 3 availability zones for this network. They can optionally be specified for AWS networks
	// used with PrivateLink. Otherwise, they are automatically chosen by Confluent Cloud.
	//
	// On AWS, zones are AWS [AZ IDs](https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html)
	//  (e.g. use1-az3)
	//
	// On GCP, zones are GCP [zones](https://cloud.google.com/compute/docs/regions-zones)
	//  (e.g. us-central1-c).
	//
	// On Azure, zones are Confluent-chosen names (e.g. 1, 2, 3) since Azure does not
	//  have universal zone identifiers.
	Zones *[]string `json:"zones,omitempty"`
}
type CdxV1NetworkApiVersion string
type CdxV1Network_Cloud struct {
	union json.RawMessage
}
type CdxV1NetworkKind string
type CdxV1OptIn struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1OptInApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CdxV1OptInKind `json:"kind,omitempty"`

	// StreamShareEnabled Enable stream sharing for the organization
	StreamShareEnabled *bool `json:"stream_share_enabled,omitempty"`
}
type CdxV1OptInApiVersion string
type CdxV1OptInKind string
type CdxV1ProviderShare struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1ProviderShareApiVersion `json:"api_version,omitempty"`

	// CloudCluster The cloud cluster to which this belongs.
	CloudCluster *EnvScopedObjectReference `json:"cloud_cluster,omitempty"`

	// ConsumerOrganizationName Consumer organization name
	ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

	// ConsumerRestriction Restrictions on the consumer that can redeem this token
	ConsumerRestriction *CdxV1ProviderShare_ConsumerRestriction `json:"consumer_restriction,omitempty"`

	// ConsumerUserName Name of the consumer
	ConsumerUserName *string `json:"consumer_user_name,omitempty"`

	// DeliveryMethod Method by which the invite will be delivered
	DeliveryMethod *string `json:"delivery_method,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
	InviteExpiresAt *time.Time `json:"invite_expires_at,omitempty"`

	// InvitedAt The date and time at which consumer was invited
	InvitedAt *time.Time `json:"invited_at,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1ProviderShareKind `json:"kind,omitempty"`
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

	// ProviderUser The provider user/inviter
	ProviderUser *GlobalObjectReference `json:"provider_user,omitempty"`

	// ProviderUserName Name or email of the provider user. Deprecated
	ProviderUserName *string `json:"provider_user_name,omitempty"`

	// RedeemedAt The date and time at which the invite was redeemed
	RedeemedAt *time.Time `json:"redeemed_at,omitempty"`

	// ServiceAccount The service account associated with this object.
	ServiceAccount *GlobalObjectReference `json:"service_account,omitempty"`

	// Status The status of the Provider Share
	Status *CdxV1ProviderShareStatus `json:"status,omitempty"`
}
type CdxV1ProviderShareApiVersion string
type CdxV1ProviderShare_ConsumerRestriction struct {
	union json.RawMessage
}
type CdxV1ProviderShareKind string
type CdxV1ProviderShareList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CdxV1ProviderShareListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CdxV1ProviderShareListDataApiVersion `json:"api_version,omitempty"`

		// CloudCluster The cloud cluster to which this belongs.
		CloudCluster EnvScopedObjectReference `json:"cloud_cluster"`

		// ConsumerOrganizationName Consumer organization name
		ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

		// ConsumerRestriction Restrictions on the consumer that can redeem this token
		ConsumerRestriction *CdxV1ProviderShareList_Data_ConsumerRestriction `json:"consumer_restriction,omitempty"`

		// ConsumerUserName Name of the consumer
		ConsumerUserName *string `json:"consumer_user_name,omitempty"`

		// DeliveryMethod Method by which the invite will be delivered
		DeliveryMethod string `json:"delivery_method"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
		InviteExpiresAt time.Time `json:"invite_expires_at"`

		// InvitedAt The date and time at which consumer was invited
		InvitedAt time.Time `json:"invited_at"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *CdxV1ProviderShareListDataKind `json:"kind,omitempty"`
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

		// ProviderUser The provider user/inviter
		ProviderUser GlobalObjectReference `json:"provider_user"`

		// ProviderUserName Name or email of the provider user. Deprecated
		ProviderUserName string `json:"provider_user_name"`

		// RedeemedAt The date and time at which the invite was redeemed
		RedeemedAt *time.Time `json:"redeemed_at,omitempty"`

		// ServiceAccount The service account associated with this object.
		ServiceAccount *GlobalObjectReference `json:"service_account,omitempty"`

		// Status The status of the Provider Share
		Status CdxV1ProviderShareStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CdxV1ProviderShareListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CdxV1ProviderShareListApiVersion string
type CdxV1ProviderShareListDataApiVersion string
type CdxV1ProviderShareList_Data_ConsumerRestriction struct {
	union json.RawMessage
}
type CdxV1ProviderShareListDataKind string
type CdxV1ProviderShareListKind string
type CdxV1ProviderShareStatus struct {
	// Phase Status of share
	Phase string `json:"phase"`
}
type CdxV1ProviderSharedResource struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1ProviderSharedResourceApiVersion `json:"api_version,omitempty"`

	// CloudCluster The cloud cluster to which this belongs.
	CloudCluster *EnvScopedObjectReference `json:"cloud_cluster,omitempty"`

	// ClusterName The cluster display name of the shared resource. Deprecated
	ClusterName *string `json:"cluster_name,omitempty"`

	// Crn Deprecated please use resources attribute.
	Crn *string `json:"crn,omitempty"`

	// Description Description of shared resource
	Description *string `json:"description,omitempty"`

	// DisplayName Shared resource display name
	DisplayName *string `json:"display_name,omitempty"`

	// EnvironmentName The environment name of the shared resource. Deprecated
	EnvironmentName *string `json:"environment_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *CdxV1ProviderSharedResourceKind `json:"kind,omitempty"`

	// LogoUrl Resource logo url
	LogoUrl  *string `json:"logo_url,omitempty"`
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

	// OrganizationContact Email of contact person from the organization
	OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

	// OrganizationDescription Shared resource's organization description
	OrganizationDescription *string `json:"organization_description,omitempty"`

	// OrganizationName Organization to which the shared resource belongs. Deprecated
	OrganizationName interface{} `json:"organization_name,omitempty"`

	// Resources List of resource crns that are shared together
	Resources *[]string `json:"resources,omitempty"`

	// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
	Schemas *[]struct {
		// Id Globally unique identifier of the schema
		Id *int32 `json:"id,omitempty"`

		// Schema Schema definition string
		Schema *string `json:"schema,omitempty"`

		// SchemaType Schema type
		SchemaType *string `json:"schema_type,omitempty"`

		// Subject Name of the subject
		Subject *string `json:"subject,omitempty"`

		// Version Version number
		Version *int32 `json:"version,omitempty"`
	} `json:"schemas,omitempty"`

	// Tags list of tags
	Tags *[]string `json:"tags,omitempty"`
}
type CdxV1ProviderSharedResourceApiVersion string
type CdxV1ProviderSharedResourceKind string
type CdxV1ProviderSharedResourceList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion CdxV1ProviderSharedResourceListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *CdxV1ProviderSharedResourceListDataApiVersion `json:"api_version,omitempty"`

		// CloudCluster The cloud cluster to which this belongs.
		CloudCluster EnvScopedObjectReference `json:"cloud_cluster"`

		// ClusterName The cluster display name of the shared resource. Deprecated
		ClusterName string `json:"cluster_name"`

		// Crn Deprecated please use resources attribute.
		Crn *string `json:"crn,omitempty"`

		// Description Description of shared resource
		Description *string `json:"description,omitempty"`

		// DisplayName Shared resource display name
		DisplayName string `json:"display_name"`

		// EnvironmentName The environment name of the shared resource. Deprecated
		EnvironmentName string `json:"environment_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *CdxV1ProviderSharedResourceListDataKind `json:"kind,omitempty"`

		// LogoUrl Resource logo url
		LogoUrl  *string `json:"logo_url,omitempty"`
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

		// OrganizationContact Email of contact person from the organization
		OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

		// OrganizationDescription Shared resource's organization description
		OrganizationDescription *string `json:"organization_description,omitempty"`

		// OrganizationName Organization to which the shared resource belongs. Deprecated
		OrganizationName interface{} `json:"organization_name"`

		// Resources List of resource crns that are shared together
		Resources *[]string `json:"resources,omitempty"`

		// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
		Schemas *[]struct {
			// Id Globally unique identifier of the schema
			Id *int32 `json:"id,omitempty"`

			// Schema Schema definition string
			Schema *string `json:"schema,omitempty"`

			// SchemaType Schema type
			SchemaType *string `json:"schema_type,omitempty"`

			// Subject Name of the subject
			Subject *string `json:"subject,omitempty"`

			// Version Version number
			Version *int32 `json:"version,omitempty"`
		} `json:"schemas,omitempty"`

		// Tags list of tags
		Tags *[]string `json:"tags,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     CdxV1ProviderSharedResourceListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type CdxV1ProviderSharedResourceListApiVersion string
type CdxV1ProviderSharedResourceListDataApiVersion string
type CdxV1ProviderSharedResourceListDataKind string
type CdxV1ProviderSharedResourceListKind string
type CdxV1RedeemTokenRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1RedeemTokenRequestApiVersion `json:"api_version,omitempty"`

	// AwsAccount Consumer's AWS account ID for PrivateLink access.
	AwsAccount *string `json:"aws_account,omitempty"`

	// AzureSubscription Consumer's Azure subscription ID for PrivateLink access.
	AzureSubscription *string `json:"azure_subscription,omitempty"`

	// GcpProject Consumer's GCP project ID for Private Service Connect access.
	GcpProject *string `json:"gcp_project,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1RedeemTokenRequestKind `json:"kind,omitempty"`
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

	// Token The encrypted token
	Token *string `json:"token,omitempty"`
}
type CdxV1RedeemTokenRequestApiVersion string
type CdxV1RedeemTokenRequestKind string
type CdxV1RedeemTokenResponse struct {
	// ApiKey The api key
	ApiKey *string `json:"api_key,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1RedeemTokenResponseApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// KafkaBootstrapUrl The kafka cluster bootstrap url
	KafkaBootstrapUrl *string `json:"kafka_bootstrap_url,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1RedeemTokenResponseKind `json:"kind,omitempty"`
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

	// Resources List of shared resources
	Resources *[]CdxV1RedeemTokenResponse_Resources_Item `json:"resources,omitempty"`

	// SchemaRegistryApiKey The api key for schema registry
	SchemaRegistryApiKey *string `json:"schema_registry_api_key,omitempty"`

	// SchemaRegistrySecret The api key secret for schema registry
	SchemaRegistrySecret *string `json:"schema_registry_secret,omitempty"`

	// SchemaRegistryUrl The schema registry endpoint url
	SchemaRegistryUrl *string `json:"schema_registry_url,omitempty"`

	// Secret The api key secret
	Secret *string `json:"secret,omitempty"`
}
type CdxV1RedeemTokenResponseApiVersion string
type CdxV1RedeemTokenResponseKind string
type CdxV1RedeemTokenResponse_Resources_Item struct {
	union json.RawMessage
}
type CdxV1Schema struct {
	// Id Globally unique identifier of the schema
	Id *int32 `json:"id,omitempty"`

	// Schema Schema definition string
	Schema *string `json:"schema,omitempty"`

	// SchemaType Schema type
	SchemaType *string `json:"schema_type,omitempty"`

	// Subject Name of the subject
	Subject *string `json:"subject,omitempty"`

	// Version Version number
	Version *int32 `json:"version,omitempty"`
}
type CdxV1SharedGroup struct {
	// GroupPrefix The consumer group prefix
	GroupPrefix string `json:"group_prefix"`

	// Kind The resource kind
	Kind CdxV1SharedGroupKind `json:"kind"`
}
type CdxV1SharedGroupKind string
type CdxV1SharedSubject struct {
	// Kind The shared resource kind
	Kind CdxV1SharedSubjectKind `json:"kind"`

	// Subject The subject name
	Subject string `json:"subject"`
}
type CdxV1SharedSubjectKind string
type CdxV1SharedToken struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CdxV1SharedTokenApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CdxV1SharedTokenKind `json:"kind,omitempty"`
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

	// Token The encrypted token
	Token *string `json:"token,omitempty"`
}
type CdxV1SharedTokenApiVersion string
type CdxV1SharedTokenKind string
type CdxV1SharedTopic struct {
	// Kind The shared resource kind
	Kind CdxV1SharedTopicKind `json:"kind"`

	// Topic The topic name
	Topic string `json:"topic"`
}
type CdxV1SharedTopicKind string
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
func (t CdxV1CreateProviderShareRequest_ConsumerRestriction) AsCdxV1EmailConsumerRestriction() (CdxV1EmailConsumerRestriction, error) {
	var body CdxV1EmailConsumerRestriction
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1CreateProviderShareRequest_ConsumerRestriction) FromCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	t.union = b
	return err
}
func (t *CdxV1CreateProviderShareRequest_ConsumerRestriction) MergeCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1CreateProviderShareRequest_ConsumerRestriction) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CdxV1CreateProviderShareRequest_ConsumerRestriction) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Email":
		return t.AsCdxV1EmailConsumerRestriction()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CdxV1CreateProviderShareRequest_ConsumerRestriction) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CdxV1CreateProviderShareRequest_ConsumerRestriction) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CdxV1Network_Cloud) AsCdxV1AwsNetwork() (CdxV1AwsNetwork, error) {
	var body CdxV1AwsNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1Network_Cloud) FromCdxV1AwsNetwork(v CdxV1AwsNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsNetwork"}`))
	t.union = b
	return err
}
func (t *CdxV1Network_Cloud) MergeCdxV1AwsNetwork(v CdxV1AwsNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1Network_Cloud) AsCdxV1AzureNetwork() (CdxV1AzureNetwork, error) {
	var body CdxV1AzureNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1Network_Cloud) FromCdxV1AzureNetwork(v CdxV1AzureNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureNetwork"}`))
	t.union = b
	return err
}
func (t *CdxV1Network_Cloud) MergeCdxV1AzureNetwork(v CdxV1AzureNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1Network_Cloud) AsCdxV1GcpNetwork() (CdxV1GcpNetwork, error) {
	var body CdxV1GcpNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1Network_Cloud) FromCdxV1GcpNetwork(v CdxV1GcpNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpNetwork"}`))
	t.union = b
	return err
}
func (t *CdxV1Network_Cloud) MergeCdxV1GcpNetwork(v CdxV1GcpNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1Network_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CdxV1Network_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsNetwork":
		return t.AsCdxV1AwsNetwork()
	case "AzureNetwork":
		return t.AsCdxV1AzureNetwork()
	case "GcpNetwork":
		return t.AsCdxV1GcpNetwork()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CdxV1Network_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CdxV1Network_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CdxV1ProviderShare_ConsumerRestriction) AsCdxV1EmailConsumerRestriction() (CdxV1EmailConsumerRestriction, error) {
	var body CdxV1EmailConsumerRestriction
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1ProviderShare_ConsumerRestriction) FromCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	t.union = b
	return err
}
func (t *CdxV1ProviderShare_ConsumerRestriction) MergeCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1ProviderShare_ConsumerRestriction) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CdxV1ProviderShare_ConsumerRestriction) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Email":
		return t.AsCdxV1EmailConsumerRestriction()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CdxV1ProviderShare_ConsumerRestriction) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CdxV1ProviderShare_ConsumerRestriction) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CdxV1ProviderShareList_Data_ConsumerRestriction) AsCdxV1EmailConsumerRestriction() (CdxV1EmailConsumerRestriction, error) {
	var body CdxV1EmailConsumerRestriction
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1ProviderShareList_Data_ConsumerRestriction) FromCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	t.union = b
	return err
}
func (t *CdxV1ProviderShareList_Data_ConsumerRestriction) MergeCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1ProviderShareList_Data_ConsumerRestriction) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CdxV1ProviderShareList_Data_ConsumerRestriction) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Email":
		return t.AsCdxV1EmailConsumerRestriction()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CdxV1ProviderShareList_Data_ConsumerRestriction) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CdxV1ProviderShareList_Data_ConsumerRestriction) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CdxV1RedeemTokenResponse_Resources_Item) AsCdxV1SharedTopic() (CdxV1SharedTopic, error) {
	var body CdxV1SharedTopic
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) FromCdxV1SharedTopic(v CdxV1SharedTopic) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Topic"}`))
	t.union = b
	return err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) MergeCdxV1SharedTopic(v CdxV1SharedTopic) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Topic"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1RedeemTokenResponse_Resources_Item) AsCdxV1SharedGroup() (CdxV1SharedGroup, error) {
	var body CdxV1SharedGroup
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) FromCdxV1SharedGroup(v CdxV1SharedGroup) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Group"}`))
	t.union = b
	return err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) MergeCdxV1SharedGroup(v CdxV1SharedGroup) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Group"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1RedeemTokenResponse_Resources_Item) AsCdxV1SharedSubject() (CdxV1SharedSubject, error) {
	var body CdxV1SharedSubject
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) FromCdxV1SharedSubject(v CdxV1SharedSubject) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Subject"}`))
	t.union = b
	return err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) MergeCdxV1SharedSubject(v CdxV1SharedSubject) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Subject"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CdxV1RedeemTokenResponse_Resources_Item) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CdxV1RedeemTokenResponse_Resources_Item) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Group":
		return t.AsCdxV1SharedGroup()
	case "Subject":
		return t.AsCdxV1SharedSubject()
	case "Topic":
		return t.AsCdxV1SharedTopic()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CdxV1RedeemTokenResponse_Resources_Item) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CdxV1RedeemTokenResponse_Resources_Item) UnmarshalJSON(b []byte) error {
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
func (t CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) AsCdxV1EmailConsumerRestriction() (CdxV1EmailConsumerRestriction, error) {
	var body CdxV1EmailConsumerRestriction
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) FromCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	t.union = b
	return err
}
func (t *CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) MergeCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Email":
		return t.AsCdxV1EmailConsumerRestriction()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateCdxV1ProviderShareJSONBody_ConsumerRestriction) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) AsCdxV1EmailConsumerRestriction() (CdxV1EmailConsumerRestriction, error) {
	var body CdxV1EmailConsumerRestriction
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) FromCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	t.union = b
	return err
}
func (t *GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) MergeCdxV1EmailConsumerRestriction(v CdxV1EmailConsumerRestriction) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Email"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "Email":
		return t.AsCdxV1EmailConsumerRestriction()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction) UnmarshalJSON(b []byte) error {
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

	// ListCdxV1ConsumerSharedResources List of Consumer Shared Resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all consumer shared resources.
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources (the `ListCdxV1ConsumerSharedResources` operationId).
	ListCdxV1ConsumerSharedResources(ctx context.Context, params *ListCdxV1ConsumerSharedResourcesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCdxV1ConsumerSharedResource Read a Consumer Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a consumer shared resource.
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id} (the `GetCdxV1ConsumerSharedResource` operationId).
	GetCdxV1ConsumerSharedResource(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ImageCdxV1ConsumerSharedResource Get image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the image file for the shared resource
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id}/images/{file_name} (the `ImageCdxV1ConsumerSharedResource` operationId).
	ImageCdxV1ConsumerSharedResource(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// NetworkCdxV1ConsumerSharedResource Get shared resource's network configuration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns network information of the shared resource
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id}:network (the `NetworkCdxV1ConsumerSharedResource` operationId).
	NetworkCdxV1ConsumerSharedResource(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListCdxV1ConsumerShares List of Consumer Shares
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all consumer shares.
	//
	// Corresponds with GET /cdx/v1/consumer-shares (the `ListCdxV1ConsumerShares` operationId).
	ListCdxV1ConsumerShares(ctx context.Context, params *ListCdxV1ConsumerSharesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCdxV1ConsumerShare Delete a Consumer Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a consumer share.
	//
	// Corresponds with DELETE /cdx/v1/consumer-shares/{id} (the `DeleteCdxV1ConsumerShare` operationId).
	DeleteCdxV1ConsumerShare(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCdxV1ConsumerShare Read a Consumer Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a consumer share.
	//
	// Corresponds with GET /cdx/v1/consumer-shares/{id} (the `GetCdxV1ConsumerShare` operationId).
	GetCdxV1ConsumerShare(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCdxV1OptIn Read the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the organization's stream sharing opt-in settings.
	//
	// Corresponds with GET /cdx/v1/opt-in (the `GetCdxV1OptIn` operationId).
	GetCdxV1OptIn(ctx context.Context, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCdxV1OptInWithBody Set the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Updates the organization's stream sharing opt-in settings.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /cdx/v1/opt-in (the `UpdateCdxV1OptIn` operationId).
	UpdateCdxV1OptInWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCdxV1OptIn Set the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Updates the organization's stream sharing opt-in settings.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /cdx/v1/opt-in (the `UpdateCdxV1OptIn` operationId).
	UpdateCdxV1OptIn(ctx context.Context, body UpdateCdxV1OptInJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListCdxV1ProviderSharedResources List of Provider Shared Resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all provider shared resources.
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources (the `ListCdxV1ProviderSharedResources` operationId).
	ListCdxV1ProviderSharedResources(ctx context.Context, params *ListCdxV1ProviderSharedResourcesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCdxV1ProviderSharedResource Read a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a provider shared resource.
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources/{id} (the `GetCdxV1ProviderSharedResource` operationId).
	GetCdxV1ProviderSharedResource(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCdxV1ProviderSharedResourceWithBody Update a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a provider shared resource.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /cdx/v1/provider-shared-resources/{id} (the `UpdateCdxV1ProviderSharedResource` operationId).
	UpdateCdxV1ProviderSharedResourceWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateCdxV1ProviderSharedResource Update a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a provider shared resource.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /cdx/v1/provider-shared-resources/{id} (the `UpdateCdxV1ProviderSharedResource` operationId).
	UpdateCdxV1ProviderSharedResource(ctx context.Context, id string, body UpdateCdxV1ProviderSharedResourceJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteImageCdxV1ProviderSharedResource Delete the shared resource's image
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Deletes the image file for the shared resource
	//
	// Corresponds with DELETE /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `DeleteImageCdxV1ProviderSharedResource` operationId).
	DeleteImageCdxV1ProviderSharedResource(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ViewImageCdxV1ProviderSharedResource Get image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the image file for the shared resource
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `ViewImageCdxV1ProviderSharedResource` operationId).
	ViewImageCdxV1ProviderSharedResource(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UploadImageCdxV1ProviderSharedResourceWithBody Upload image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Upload the image file for the shared resource
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `UploadImageCdxV1ProviderSharedResource` operationId).
	UploadImageCdxV1ProviderSharedResourceWithBody(ctx context.Context, id string, fileName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListCdxV1ProviderShares List of Provider Shares
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all provider shares.
	//
	// Corresponds with GET /cdx/v1/provider-shares (the `ListCdxV1ProviderShares` operationId).
	ListCdxV1ProviderShares(ctx context.Context, params *ListCdxV1ProviderSharesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCdxV1ProviderShareWithBody Create a provider share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Creates a share based on delivery method.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cdx/v1/provider-shares (the `CreateCdxV1ProviderShare` operationId).
	CreateCdxV1ProviderShareWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateCdxV1ProviderShare Create a provider share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Creates a share based on delivery method.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cdx/v1/provider-shares (the `CreateCdxV1ProviderShare` operationId).
	CreateCdxV1ProviderShare(ctx context.Context, body CreateCdxV1ProviderShareJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteCdxV1ProviderShare Delete a Provider Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a provider share.
	//
	// Corresponds with DELETE /cdx/v1/provider-shares/{id} (the `DeleteCdxV1ProviderShare` operationId).
	DeleteCdxV1ProviderShare(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetCdxV1ProviderShare Read a Provider Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a provider share.
	//
	// Corresponds with GET /cdx/v1/provider-shares/{id} (the `GetCdxV1ProviderShare` operationId).
	GetCdxV1ProviderShare(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResendCdxV1ProviderShare Resend
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Resend provider share
	//
	// Corresponds with POST /cdx/v1/provider-shares/{id}:resend (the `ResendCdxV1ProviderShare` operationId).
	ResendCdxV1ProviderShare(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RedeemCdxV1SharedTokenWithBody Redeem token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Redeem the shared token for shared topic and cluster access information
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cdx/v1/shared-tokens:redeem (the `RedeemCdxV1SharedToken` operationId).
	RedeemCdxV1SharedTokenWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RedeemCdxV1SharedToken Redeem token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Redeem the shared token for shared topic and cluster access information
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cdx/v1/shared-tokens:redeem (the `RedeemCdxV1SharedToken` operationId).
	RedeemCdxV1SharedToken(ctx context.Context, body RedeemCdxV1SharedTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResourcesCdxV1SharedTokenWithBody Validate token to view shared resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate and decrypt the shared token and view token's shared resources
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /cdx/v1/shared-tokens:resources (the `ResourcesCdxV1SharedToken` operationId).
	ResourcesCdxV1SharedTokenWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResourcesCdxV1SharedToken Validate token to view shared resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate and decrypt the shared token and view token's shared resources
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /cdx/v1/shared-tokens:resources (the `ResourcesCdxV1SharedToken` operationId).
	ResourcesCdxV1SharedToken(ctx context.Context, body ResourcesCdxV1SharedTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *Client) DeleteImageCdxV1ProviderSharedResource(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewDeleteImageCdxV1ProviderSharedResourceRequest(c.Server, id, fileName)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *Client) ViewImageCdxV1ProviderSharedResource(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewViewImageCdxV1ProviderSharedResourceRequest(c.Server, id, fileName)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *Client) UploadImageCdxV1ProviderSharedResourceWithBody(ctx context.Context, id string, fileName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewUploadImageCdxV1ProviderSharedResourceRequestWithBody(c.Server, id, fileName, contentType, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewDeleteImageCdxV1ProviderSharedResourceRequest(server string, id string, fileName string) (*http.Request, error) {
	var err error

	var pathParam0 string

	pathParam0, err = runtime.StyleParamWithOptions("simple", false, "id", id, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	var pathParam1 string

	pathParam1, err = runtime.StyleParamWithOptions("simple", false, "file_name", fileName, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/cdx/v1/provider-shared-resources/%s/images/%s", pathParam0, pathParam1)
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodDelete, queryURL.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}
func NewViewImageCdxV1ProviderSharedResourceRequest(server string, id string, fileName string) (*http.Request, error) {
	var err error

	var pathParam0 string

	pathParam0, err = runtime.StyleParamWithOptions("simple", false, "id", id, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	var pathParam1 string

	pathParam1, err = runtime.StyleParamWithOptions("simple", false, "file_name", fileName, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/cdx/v1/provider-shared-resources/%s/images/%s", pathParam0, pathParam1)
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, queryURL.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}
func NewUploadImageCdxV1ProviderSharedResourceRequestWithBody(server string, id string, fileName string, contentType string, body io.Reader) (*http.Request, error) {
	var err error

	var pathParam0 string

	pathParam0, err = runtime.StyleParamWithOptions("simple", false, "id", id, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	var pathParam1 string

	pathParam1, err = runtime.StyleParamWithOptions("simple", false, "file_name", fileName, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/cdx/v1/provider-shared-resources/%s/images/%s", pathParam0, pathParam1)
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, queryURL.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", contentType)

	return req, nil
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

	// ListCdxV1ConsumerSharedResourcesWithResponse List of Consumer Shared Resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all consumer shared resources.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources (the `ListCdxV1ConsumerSharedResources` operationId).
	ListCdxV1ConsumerSharedResourcesWithResponse(ctx context.Context, params *ListCdxV1ConsumerSharedResourcesParams, reqEditors ...RequestEditorFn) (*ListCdxV1ConsumerSharedResourcesResponse, error)

	// GetCdxV1ConsumerSharedResourceWithResponse Read a Consumer Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a consumer shared resource.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id} (the `GetCdxV1ConsumerSharedResource` operationId).
	GetCdxV1ConsumerSharedResourceWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetCdxV1ConsumerSharedResourceResponse, error)

	// ImageCdxV1ConsumerSharedResourceWithResponse Get image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the image file for the shared resource
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id}/images/{file_name} (the `ImageCdxV1ConsumerSharedResource` operationId).
	ImageCdxV1ConsumerSharedResourceWithResponse(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*ImageCdxV1ConsumerSharedResourceResponse, error)

	// NetworkCdxV1ConsumerSharedResourceWithResponse Get shared resource's network configuration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns network information of the shared resource
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shared-resources/{id}:network (the `NetworkCdxV1ConsumerSharedResource` operationId).
	NetworkCdxV1ConsumerSharedResourceWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*NetworkCdxV1ConsumerSharedResourceResponse, error)

	// ListCdxV1ConsumerSharesWithResponse List of Consumer Shares
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all consumer shares.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shares (the `ListCdxV1ConsumerShares` operationId).
	ListCdxV1ConsumerSharesWithResponse(ctx context.Context, params *ListCdxV1ConsumerSharesParams, reqEditors ...RequestEditorFn) (*ListCdxV1ConsumerSharesResponse, error)

	// DeleteCdxV1ConsumerShareWithResponse Delete a Consumer Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a consumer share.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /cdx/v1/consumer-shares/{id} (the `DeleteCdxV1ConsumerShare` operationId).
	DeleteCdxV1ConsumerShareWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteCdxV1ConsumerShareResponse, error)

	// GetCdxV1ConsumerShareWithResponse Read a Consumer Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a consumer share.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/consumer-shares/{id} (the `GetCdxV1ConsumerShare` operationId).
	GetCdxV1ConsumerShareWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetCdxV1ConsumerShareResponse, error)

	// GetCdxV1OptInWithResponse Read the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the organization's stream sharing opt-in settings.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/opt-in (the `GetCdxV1OptIn` operationId).
	GetCdxV1OptInWithResponse(ctx context.Context, reqEditors ...RequestEditorFn) (*GetCdxV1OptInResponse, error)

	// UpdateCdxV1OptInWithBodyWithResponse Set the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Updates the organization's stream sharing opt-in settings.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cdx/v1/opt-in (the `UpdateCdxV1OptIn` operationId).
	UpdateCdxV1OptInWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateCdxV1OptInResponse, error)

	// UpdateCdxV1OptInWithResponse Set the organization's stream sharing opt-in settings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Updates the organization's stream sharing opt-in settings.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cdx/v1/opt-in (the `UpdateCdxV1OptIn` operationId).
	UpdateCdxV1OptInWithResponse(ctx context.Context, body UpdateCdxV1OptInJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateCdxV1OptInResponse, error)

	// ListCdxV1ProviderSharedResourcesWithResponse List of Provider Shared Resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all provider shared resources.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources (the `ListCdxV1ProviderSharedResources` operationId).
	ListCdxV1ProviderSharedResourcesWithResponse(ctx context.Context, params *ListCdxV1ProviderSharedResourcesParams, reqEditors ...RequestEditorFn) (*ListCdxV1ProviderSharedResourcesResponse, error)

	// GetCdxV1ProviderSharedResourceWithResponse Read a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a provider shared resource.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources/{id} (the `GetCdxV1ProviderSharedResource` operationId).
	GetCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetCdxV1ProviderSharedResourceResponse, error)

	// UpdateCdxV1ProviderSharedResourceWithBodyWithResponse Update a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a provider shared resource.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cdx/v1/provider-shared-resources/{id} (the `UpdateCdxV1ProviderSharedResource` operationId).
	UpdateCdxV1ProviderSharedResourceWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateCdxV1ProviderSharedResourceResponse, error)

	// UpdateCdxV1ProviderSharedResourceWithResponse Update a Provider Shared Resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a provider shared resource.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /cdx/v1/provider-shared-resources/{id} (the `UpdateCdxV1ProviderSharedResource` operationId).
	UpdateCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, body UpdateCdxV1ProviderSharedResourceJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateCdxV1ProviderSharedResourceResponse, error)

	// DeleteImageCdxV1ProviderSharedResourceWithResponse Delete the shared resource's image
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Deletes the image file for the shared resource
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `DeleteImageCdxV1ProviderSharedResource` operationId).
	DeleteImageCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*DeleteImageCdxV1ProviderSharedResourceResponse, error)

	// ViewImageCdxV1ProviderSharedResourceWithResponse Get image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns the image file for the shared resource
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `ViewImageCdxV1ProviderSharedResource` operationId).
	ViewImageCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*ViewImageCdxV1ProviderSharedResourceResponse, error)

	// UploadImageCdxV1ProviderSharedResourceWithBodyWithResponse Upload image for shared resource
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Upload the image file for the shared resource
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/provider-shared-resources/{id}/images/{file_name} (the `UploadImageCdxV1ProviderSharedResource` operationId).
	UploadImageCdxV1ProviderSharedResourceWithBodyWithResponse(ctx context.Context, id string, fileName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UploadImageCdxV1ProviderSharedResourceResponse, error)

	// ListCdxV1ProviderSharesWithResponse List of Provider Shares
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all provider shares.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/provider-shares (the `ListCdxV1ProviderShares` operationId).
	ListCdxV1ProviderSharesWithResponse(ctx context.Context, params *ListCdxV1ProviderSharesParams, reqEditors ...RequestEditorFn) (*ListCdxV1ProviderSharesResponse, error)

	// CreateCdxV1ProviderShareWithBodyWithResponse Create a provider share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Creates a share based on delivery method.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/provider-shares (the `CreateCdxV1ProviderShare` operationId).
	CreateCdxV1ProviderShareWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateCdxV1ProviderShareResponse, error)

	// CreateCdxV1ProviderShareWithResponse Create a provider share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Creates a share based on delivery method.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/provider-shares (the `CreateCdxV1ProviderShare` operationId).
	CreateCdxV1ProviderShareWithResponse(ctx context.Context, body CreateCdxV1ProviderShareJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateCdxV1ProviderShareResponse, error)

	// DeleteCdxV1ProviderShareWithResponse Delete a Provider Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a provider share.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /cdx/v1/provider-shares/{id} (the `DeleteCdxV1ProviderShare` operationId).
	DeleteCdxV1ProviderShareWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteCdxV1ProviderShareResponse, error)

	// GetCdxV1ProviderShareWithResponse Read a Provider Share
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a provider share.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /cdx/v1/provider-shares/{id} (the `GetCdxV1ProviderShare` operationId).
	GetCdxV1ProviderShareWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetCdxV1ProviderShareResponse, error)

	// ResendCdxV1ProviderShareWithResponse Resend
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Resend provider share
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/provider-shares/{id}:resend (the `ResendCdxV1ProviderShare` operationId).
	ResendCdxV1ProviderShareWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*ResendCdxV1ProviderShareResponse, error)

	// RedeemCdxV1SharedTokenWithBodyWithResponse Redeem token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Redeem the shared token for shared topic and cluster access information
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/shared-tokens:redeem (the `RedeemCdxV1SharedToken` operationId).
	RedeemCdxV1SharedTokenWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*RedeemCdxV1SharedTokenResponse, error)

	// RedeemCdxV1SharedTokenWithResponse Redeem token
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Redeem the shared token for shared topic and cluster access information
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/shared-tokens:redeem (the `RedeemCdxV1SharedToken` operationId).
	RedeemCdxV1SharedTokenWithResponse(ctx context.Context, body RedeemCdxV1SharedTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*RedeemCdxV1SharedTokenResponse, error)

	// ResourcesCdxV1SharedTokenWithBodyWithResponse Validate token to view shared resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate and decrypt the shared token and view token's shared resources
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/shared-tokens:resources (the `ResourcesCdxV1SharedToken` operationId).
	ResourcesCdxV1SharedTokenWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*ResourcesCdxV1SharedTokenResponse, error)

	// ResourcesCdxV1SharedTokenWithResponse Validate token to view shared resources
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate and decrypt the shared token and view token's shared resources
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /cdx/v1/shared-tokens:resources (the `ResourcesCdxV1SharedToken` operationId).
	ResourcesCdxV1SharedTokenWithResponse(ctx context.Context, body ResourcesCdxV1SharedTokenJSONRequestBody, reqEditors ...RequestEditorFn) (*ResourcesCdxV1SharedTokenResponse, error)
}

func (r ListCdxV1ConsumerSharedResourcesResponse) GetJSON200() *CdxV1ConsumerSharedResourceList {
	return r.JSON200
}
func (r ListCdxV1ConsumerSharedResourcesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCdxV1ConsumerSharedResourcesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCdxV1ConsumerSharedResourcesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCdxV1ConsumerSharedResourcesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCdxV1ConsumerSharedResourcesResponse) GetBody() []byte {
	return r.Body
}
func (r ListCdxV1ConsumerSharedResourcesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCdxV1ConsumerSharedResourcesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCdxV1ConsumerSharedResourcesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCdxV1ConsumerSharedResource200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud The cloud service provider of the provider shared cluster.
	Cloud string `json:"cloud"`

	// Description Description of consumer resource
	Description *string `json:"description,omitempty"`

	// DisplayName Consumer resource display name
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetCdxV1ConsumerSharedResource200JSONResponseBodyKind `json:"kind"`

	// LogoUrl Resource logo url
	LogoUrl  *string `json:"logo_url,omitempty"`
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

	// NetworkConnectionTypes The network connection types of the provider shared cluster. If the shared cluster is on public internet,
	// then the list will be empty
	NetworkConnectionTypes *[]CdxV1ConnectionType `json:"network_connection_types,omitempty"`

	// OrganizationContact Email of the shared resource's organization contact
	OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

	// OrganizationDescription Shared resource's organization description
	OrganizationDescription *string `json:"organization_description,omitempty"`

	// OrganizationName Shared resource's organization name
	OrganizationName string `json:"organization_name"`

	// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
	Schemas *[]struct {
		// Id Globally unique identifier of the schema
		Id *int32 `json:"id,omitempty"`

		// Schema Schema definition string
		Schema *string `json:"schema,omitempty"`

		// SchemaType Schema type
		SchemaType *string `json:"schema_type,omitempty"`

		// Subject Name of the subject
		Subject *string `json:"subject,omitempty"`

		// Version Version number
		Version *int32 `json:"version,omitempty"`
	} `json:"schemas,omitempty"`

	// Tags list of tags
	Tags *[]string `json:"tags,omitempty"`
} {
	return r.JSON200
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCdxV1ConsumerSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r GetCdxV1ConsumerSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCdxV1ConsumerSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCdxV1ConsumerSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ImageCdxV1ConsumerSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r ImageCdxV1ConsumerSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ImageCdxV1ConsumerSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ImageCdxV1ConsumerSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON200() *CdxV1Network {
	return r.JSON200
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r NetworkCdxV1ConsumerSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListCdxV1ConsumerSharesResponse) GetJSON200() *CdxV1ConsumerShareList {
	return r.JSON200
}
func (r ListCdxV1ConsumerSharesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCdxV1ConsumerSharesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCdxV1ConsumerSharesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCdxV1ConsumerSharesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCdxV1ConsumerSharesResponse) GetBody() []byte {
	return r.Body
}
func (r ListCdxV1ConsumerSharesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCdxV1ConsumerSharesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCdxV1ConsumerSharesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCdxV1ConsumerShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCdxV1ConsumerShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCdxV1ConsumerShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCdxV1ConsumerShareResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCdxV1ConsumerShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCdxV1ConsumerShareResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCdxV1ConsumerShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCdxV1ConsumerShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCdxV1ConsumerShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCdxV1ConsumerShareResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCdxV1ConsumerShare200JSONResponseBodyApiVersion `json:"api_version"`

	// ConsumerOrganizationName Consumer organization name. Deprecated
	ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

	// ConsumerUser The consumer user/invitee
	ConsumerUser GlobalObjectReference `json:"consumer_user"`

	// ConsumerUserName Name of the consumer. Deprecated
	ConsumerUserName *string `json:"consumer_user_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
	InviteExpiresAt *time.Time `json:"invite_expires_at,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCdxV1ConsumerShare200JSONResponseBodyKind `json:"kind"`
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

	// ProviderOrganizationName Provider organization name
	ProviderOrganizationName string `json:"provider_organization_name"`

	// ProviderUserName Name or email of the provider user
	ProviderUserName string `json:"provider_user_name"`

	// Status The status of the Consumer Share
	Status CdxV1ConsumerShareStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetCdxV1ConsumerShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCdxV1ConsumerShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCdxV1ConsumerShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCdxV1ConsumerShareResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCdxV1ConsumerShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCdxV1ConsumerShareResponse) GetBody() []byte {
	return r.Body
}
func (r GetCdxV1ConsumerShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCdxV1ConsumerShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCdxV1ConsumerShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCdxV1OptInResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetCdxV1OptIn200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetCdxV1OptIn200JSONResponseBodyKind `json:"kind"`

	// StreamShareEnabled Enable stream sharing for the organization
	StreamShareEnabled *bool `json:"stream_share_enabled,omitempty"`
} {
	return r.JSON200
}
func (r GetCdxV1OptInResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCdxV1OptInResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCdxV1OptInResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCdxV1OptInResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCdxV1OptInResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCdxV1OptInResponse) GetBody() []byte {
	return r.Body
}
func (r GetCdxV1OptInResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCdxV1OptInResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCdxV1OptInResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateCdxV1OptInResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateCdxV1OptIn200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind UpdateCdxV1OptIn200JSONResponseBodyKind `json:"kind"`

	// StreamShareEnabled Enable stream sharing for the organization
	StreamShareEnabled *bool `json:"stream_share_enabled,omitempty"`
} {
	return r.JSON200
}
func (r UpdateCdxV1OptInResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateCdxV1OptInResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateCdxV1OptInResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateCdxV1OptInResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateCdxV1OptInResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateCdxV1OptInResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateCdxV1OptInResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateCdxV1OptInResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateCdxV1OptInResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateCdxV1OptInResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateCdxV1OptInResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListCdxV1ProviderSharedResourcesResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCdxV1ProviderSharedResources200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		CloudCluster interface{} `json:"cloud_cluster,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCdxV1ProviderSharedResources200JSONResponseBodyKind `json:"kind"`
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
func (r ListCdxV1ProviderSharedResourcesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCdxV1ProviderSharedResourcesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCdxV1ProviderSharedResourcesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCdxV1ProviderSharedResourcesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCdxV1ProviderSharedResourcesResponse) GetBody() []byte {
	return r.Body
}
func (r ListCdxV1ProviderSharedResourcesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCdxV1ProviderSharedResourcesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCdxV1ProviderSharedResourcesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion   GetCdxV1ProviderSharedResource200JSONResponseBodyApiVersion `json:"api_version"`
	CloudCluster interface{}                                                 `json:"cloud_cluster"`

	// ClusterName The cluster display name of the shared resource. Deprecated
	ClusterName string `json:"cluster_name"`

	// Crn Deprecated please use resources attribute.
	Crn *string `json:"crn,omitempty"`

	// Description Description of shared resource
	Description *string `json:"description,omitempty"`

	// DisplayName Shared resource display name
	DisplayName string `json:"display_name"`

	// EnvironmentName The environment name of the shared resource. Deprecated
	EnvironmentName string `json:"environment_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetCdxV1ProviderSharedResource200JSONResponseBodyKind `json:"kind"`

	// LogoUrl Resource logo url
	LogoUrl  *string `json:"logo_url,omitempty"`
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

	// OrganizationContact Email of contact person from the organization
	OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

	// OrganizationDescription Shared resource's organization description
	OrganizationDescription *string `json:"organization_description,omitempty"`

	// OrganizationName Organization to which the shared resource belongs. Deprecated
	OrganizationName interface{} `json:"organization_name"`

	// Resources List of resource crns that are shared together
	Resources *[]string `json:"resources,omitempty"`

	// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
	Schemas *[]struct {
		// Id Globally unique identifier of the schema
		Id *int32 `json:"id,omitempty"`

		// Schema Schema definition string
		Schema *string `json:"schema,omitempty"`

		// SchemaType Schema type
		SchemaType *string `json:"schema_type,omitempty"`

		// Subject Name of the subject
		Subject *string `json:"subject,omitempty"`

		// Version Version number
		Version *int32 `json:"version,omitempty"`
	} `json:"schemas,omitempty"`

	// Tags list of tags
	Tags *[]string `json:"tags,omitempty"`
} {
	return r.JSON200
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCdxV1ProviderSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCdxV1ProviderSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r GetCdxV1ProviderSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCdxV1ProviderSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCdxV1ProviderSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion   UpdateCdxV1ProviderSharedResource200JSONResponseBodyApiVersion `json:"api_version"`
	CloudCluster interface{}                                                    `json:"cloud_cluster"`

	// ClusterName The cluster display name of the shared resource. Deprecated
	ClusterName string `json:"cluster_name"`

	// Crn Deprecated please use resources attribute.
	Crn *string `json:"crn,omitempty"`

	// Description Description of shared resource
	Description *string `json:"description,omitempty"`

	// DisplayName Shared resource display name
	DisplayName string `json:"display_name"`

	// EnvironmentName The environment name of the shared resource. Deprecated
	EnvironmentName string `json:"environment_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind UpdateCdxV1ProviderSharedResource200JSONResponseBodyKind `json:"kind"`

	// LogoUrl Resource logo url
	LogoUrl  *string `json:"logo_url,omitempty"`
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

	// OrganizationContact Email of contact person from the organization
	OrganizationContact *openapi_types.Email `json:"organization_contact,omitempty"`

	// OrganizationDescription Shared resource's organization description
	OrganizationDescription *string `json:"organization_description,omitempty"`

	// OrganizationName Organization to which the shared resource belongs. Deprecated
	OrganizationName interface{} `json:"organization_name"`

	// Resources List of resource crns that are shared together
	Resources *[]string `json:"resources,omitempty"`

	// Schemas List of schemas in JSON format. This field is work in progress and subject to changes.
	Schemas *[]struct {
		// Id Globally unique identifier of the schema
		Id *int32 `json:"id,omitempty"`

		// Schema Schema definition string
		Schema *string `json:"schema,omitempty"`

		// SchemaType Schema type
		SchemaType *string `json:"schema_type,omitempty"`

		// Subject Name of the subject
		Subject *string `json:"subject,omitempty"`

		// Version Version number
		Version *int32 `json:"version,omitempty"`
	} `json:"schemas,omitempty"`

	// Tags list of tags
	Tags *[]string `json:"tags,omitempty"`
} {
	return r.JSON200
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateCdxV1ProviderSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateCdxV1ProviderSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateCdxV1ProviderSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateCdxV1ProviderSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type DeleteImageCdxV1ProviderSharedResourceResponse400Headers struct {
	XRequestId *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse403Headers struct {
	XRequestId *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse404Headers struct {
	XRequestId *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse500Headers struct {
	XRequestId *string
}
type DeleteImageCdxV1ProviderSharedResourceResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *UnauthenticatedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *UnauthorizedError
	// JSON404 the response for an HTTP 404 `application/json` response
	JSON404 *NotFoundError
	// JSON500 the response for an HTTP 500 `application/json` response
	JSON500 *DefaultSystemError
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *DeleteImageCdxV1ProviderSharedResourceResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *DeleteImageCdxV1ProviderSharedResourceResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *DeleteImageCdxV1ProviderSharedResourceResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *DeleteImageCdxV1ProviderSharedResourceResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *DeleteImageCdxV1ProviderSharedResourceResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *DeleteImageCdxV1ProviderSharedResourceResponse500Headers
}

func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteImageCdxV1ProviderSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type ViewImageCdxV1ProviderSharedResourceResponse400Headers struct {
	XRequestId *string
}
type ViewImageCdxV1ProviderSharedResourceResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type ViewImageCdxV1ProviderSharedResourceResponse403Headers struct {
	XRequestId *string
}
type ViewImageCdxV1ProviderSharedResourceResponse404Headers struct {
	XRequestId *string
}
type ViewImageCdxV1ProviderSharedResourceResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type ViewImageCdxV1ProviderSharedResourceResponse500Headers struct {
	XRequestId *string
}
type ViewImageCdxV1ProviderSharedResourceResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *UnauthenticatedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *UnauthorizedError
	// JSON404 the response for an HTTP 404 `application/json` response
	JSON404 *NotFoundError
	// JSON500 the response for an HTTP 500 `application/json` response
	JSON500 *DefaultSystemError
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *ViewImageCdxV1ProviderSharedResourceResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *ViewImageCdxV1ProviderSharedResourceResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *ViewImageCdxV1ProviderSharedResourceResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *ViewImageCdxV1ProviderSharedResourceResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *ViewImageCdxV1ProviderSharedResourceResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *ViewImageCdxV1ProviderSharedResourceResponse500Headers
}

func (r ViewImageCdxV1ProviderSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ViewImageCdxV1ProviderSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type UploadImageCdxV1ProviderSharedResourceResponse201Headers struct {
	Location   *string
	XRequestId *string
}
type UploadImageCdxV1ProviderSharedResourceResponse400Headers struct {
	XRequestId *string
}
type UploadImageCdxV1ProviderSharedResourceResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type UploadImageCdxV1ProviderSharedResourceResponse403Headers struct {
	XRequestId *string
}
type UploadImageCdxV1ProviderSharedResourceResponse404Headers struct {
	XRequestId *string
}
type UploadImageCdxV1ProviderSharedResourceResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type UploadImageCdxV1ProviderSharedResourceResponse500Headers struct {
	XRequestId *string
}
type UploadImageCdxV1ProviderSharedResourceResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *UnauthenticatedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *UnauthorizedError
	// JSON404 the response for an HTTP 404 `application/json` response
	JSON404 *NotFoundError
	// JSON500 the response for an HTTP 500 `application/json` response
	JSON500 *DefaultSystemError
	// Headers201 the parsed response headers for an HTTP 201 response
	Headers201 *UploadImageCdxV1ProviderSharedResourceResponse201Headers
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *UploadImageCdxV1ProviderSharedResourceResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *UploadImageCdxV1ProviderSharedResourceResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *UploadImageCdxV1ProviderSharedResourceResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *UploadImageCdxV1ProviderSharedResourceResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *UploadImageCdxV1ProviderSharedResourceResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *UploadImageCdxV1ProviderSharedResourceResponse500Headers
}

func (r UploadImageCdxV1ProviderSharedResourceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) GetBody() []byte {
	return r.Body
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UploadImageCdxV1ProviderSharedResourceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListCdxV1ProviderSharesResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListCdxV1ProviderShares200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		CloudCluster   interface{} `json:"cloud_cluster,omitempty"`
		ServiceAccount interface{} `json:"service_account,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListCdxV1ProviderShares200JSONResponseBodyKind `json:"kind"`
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
func (r ListCdxV1ProviderSharesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListCdxV1ProviderSharesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListCdxV1ProviderSharesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListCdxV1ProviderSharesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListCdxV1ProviderSharesResponse) GetBody() []byte {
	return r.Body
}
func (r ListCdxV1ProviderSharesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListCdxV1ProviderSharesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListCdxV1ProviderSharesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateCdxV1ProviderShareResponse) GetJSON201() *CdxV1ProviderShare {
	return r.JSON201
}
func (r CreateCdxV1ProviderShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateCdxV1ProviderShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateCdxV1ProviderShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateCdxV1ProviderShareResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateCdxV1ProviderShareResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateCdxV1ProviderShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateCdxV1ProviderShareResponse) GetBody() []byte {
	return r.Body
}
func (r CreateCdxV1ProviderShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateCdxV1ProviderShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateCdxV1ProviderShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteCdxV1ProviderShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteCdxV1ProviderShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteCdxV1ProviderShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteCdxV1ProviderShareResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteCdxV1ProviderShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteCdxV1ProviderShareResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteCdxV1ProviderShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteCdxV1ProviderShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteCdxV1ProviderShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetCdxV1ProviderShareResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion   GetCdxV1ProviderShare200JSONResponseBodyApiVersion `json:"api_version"`
	CloudCluster interface{}                                        `json:"cloud_cluster"`

	// ConsumerOrganizationName Consumer organization name
	ConsumerOrganizationName *string `json:"consumer_organization_name,omitempty"`

	// ConsumerRestriction Restrictions on the consumer that can redeem this token
	ConsumerRestriction *GetCdxV1ProviderShare200JSONResponseBody_ConsumerRestriction `json:"consumer_restriction,omitempty"`

	// ConsumerUserName Name of the consumer
	ConsumerUserName *string `json:"consumer_user_name,omitempty"`

	// DeliveryMethod Method by which the invite will be delivered
	DeliveryMethod string `json:"delivery_method"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// InviteExpiresAt The date and time at which the invitation will expire. Only for invited shares
	InviteExpiresAt time.Time `json:"invite_expires_at"`

	// InvitedAt The date and time at which consumer was invited
	InvitedAt time.Time `json:"invited_at"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetCdxV1ProviderShare200JSONResponseBodyKind `json:"kind"`
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

	// ProviderUser The provider user/inviter
	ProviderUser GlobalObjectReference `json:"provider_user"`

	// ProviderUserName Name or email of the provider user. Deprecated
	ProviderUserName string `json:"provider_user_name"`

	// RedeemedAt The date and time at which the invite was redeemed
	RedeemedAt     *time.Time  `json:"redeemed_at,omitempty"`
	ServiceAccount interface{} `json:"service_account,omitempty"`

	// Status The status of the Provider Share
	Status CdxV1ProviderShareStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetCdxV1ProviderShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetCdxV1ProviderShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetCdxV1ProviderShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetCdxV1ProviderShareResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetCdxV1ProviderShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetCdxV1ProviderShareResponse) GetBody() []byte {
	return r.Body
}
func (r GetCdxV1ProviderShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetCdxV1ProviderShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetCdxV1ProviderShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ResendCdxV1ProviderShareResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ResendCdxV1ProviderShareResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ResendCdxV1ProviderShareResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ResendCdxV1ProviderShareResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ResendCdxV1ProviderShareResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ResendCdxV1ProviderShareResponse) GetBody() []byte {
	return r.Body
}
func (r ResendCdxV1ProviderShareResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ResendCdxV1ProviderShareResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ResendCdxV1ProviderShareResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON200() *CdxV1RedeemTokenResponse {
	return r.JSON200
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r RedeemCdxV1SharedTokenResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r RedeemCdxV1SharedTokenResponse) GetBody() []byte {
	return r.Body
}
func (r RedeemCdxV1SharedTokenResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r RedeemCdxV1SharedTokenResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r RedeemCdxV1SharedTokenResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON200() *struct {
	ConsumerSharedResources *[]CdxV1ConsumerSharedResource `json:"consumer_shared_resources,omitempty"`
} {
	return r.JSON200
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r ResourcesCdxV1SharedTokenResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ResourcesCdxV1SharedTokenResponse) GetBody() []byte {
	return r.Body
}
func (r ResourcesCdxV1SharedTokenResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ResourcesCdxV1SharedTokenResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ResourcesCdxV1SharedTokenResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) DeleteImageCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*DeleteImageCdxV1ProviderSharedResourceResponse, error) {
	rsp, err := c.DeleteImageCdxV1ProviderSharedResource(ctx, id, fileName, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseDeleteImageCdxV1ProviderSharedResourceResponse(rsp)
}
func (c *ClientWithResponses) ViewImageCdxV1ProviderSharedResourceWithResponse(ctx context.Context, id string, fileName string, reqEditors ...RequestEditorFn) (*ViewImageCdxV1ProviderSharedResourceResponse, error) {
	rsp, err := c.ViewImageCdxV1ProviderSharedResource(ctx, id, fileName, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseViewImageCdxV1ProviderSharedResourceResponse(rsp)
}
func (c *ClientWithResponses) UploadImageCdxV1ProviderSharedResourceWithBodyWithResponse(ctx context.Context, id string, fileName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UploadImageCdxV1ProviderSharedResourceResponse, error) {
	rsp, err := c.UploadImageCdxV1ProviderSharedResourceWithBody(ctx, id, fileName, contentType, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseUploadImageCdxV1ProviderSharedResourceResponse(rsp)
}
func ParseDeleteImageCdxV1ProviderSharedResourceResponse(rsp *http.Response) (*DeleteImageCdxV1ProviderSharedResourceResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &DeleteImageCdxV1ProviderSharedResourceResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case rsp.StatusCode == 204:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest UnauthenticatedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 404:
		var dest NotFoundError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON404 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 500:
		var dest DefaultSystemError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 400:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse401Headers
		if values := rsp.Header.Values("WWW-Authenticate"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "WWW-Authenticate", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.WWWAuthenticate = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers401 = &headers
	case rsp.StatusCode == 403:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse429Headers
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
	case rsp.StatusCode == 500:
		var headers DeleteImageCdxV1ProviderSharedResourceResponse500Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers500 = &headers
	}

	return response, nil
}
func ParseViewImageCdxV1ProviderSharedResourceResponse(rsp *http.Response) (*ViewImageCdxV1ProviderSharedResourceResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &ViewImageCdxV1ProviderSharedResourceResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest UnauthenticatedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 404:
		var dest NotFoundError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON404 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 500:
		var dest DefaultSystemError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 400:
		var headers ViewImageCdxV1ProviderSharedResourceResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers ViewImageCdxV1ProviderSharedResourceResponse401Headers
		if values := rsp.Header.Values("WWW-Authenticate"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "WWW-Authenticate", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.WWWAuthenticate = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers401 = &headers
	case rsp.StatusCode == 403:
		var headers ViewImageCdxV1ProviderSharedResourceResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers ViewImageCdxV1ProviderSharedResourceResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers ViewImageCdxV1ProviderSharedResourceResponse429Headers
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
	case rsp.StatusCode == 500:
		var headers ViewImageCdxV1ProviderSharedResourceResponse500Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers500 = &headers
	}

	return response, nil
}
func ParseUploadImageCdxV1ProviderSharedResourceResponse(rsp *http.Response) (*UploadImageCdxV1ProviderSharedResourceResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &UploadImageCdxV1ProviderSharedResourceResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case rsp.StatusCode == 201:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest UnauthenticatedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 404:
		var dest NotFoundError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON404 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 500:
		var dest DefaultSystemError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 201:
		var headers UploadImageCdxV1ProviderSharedResourceResponse201Headers
		if values := rsp.Header.Values("Location"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "Location", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: "uri"}); err != nil {
				return nil, err
			}
			headers.Location = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers201 = &headers
	case rsp.StatusCode == 400:
		var headers UploadImageCdxV1ProviderSharedResourceResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers UploadImageCdxV1ProviderSharedResourceResponse401Headers
		if values := rsp.Header.Values("WWW-Authenticate"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "WWW-Authenticate", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.WWWAuthenticate = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers401 = &headers
	case rsp.StatusCode == 403:
		var headers UploadImageCdxV1ProviderSharedResourceResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers UploadImageCdxV1ProviderSharedResourceResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers UploadImageCdxV1ProviderSharedResourceResponse429Headers
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
	case rsp.StatusCode == 500:
		var headers UploadImageCdxV1ProviderSharedResourceResponse500Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers500 = &headers
	}

	return response, nil
}
