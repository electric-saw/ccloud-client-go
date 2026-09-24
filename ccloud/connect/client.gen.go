package connect

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
	ArtifactV1UploadSourcePresignedUrlKindPresignedUrl ArtifactV1UploadSourcePresignedUrlKind = "PresignedUrl"
)

func (e ArtifactV1UploadSourcePresignedUrlKind) Valid() bool {
	switch e {
	case ArtifactV1UploadSourcePresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	DELETE ConnectV1AlterOffsetRequestType = "DELETE"
	PATCH  ConnectV1AlterOffsetRequestType = "PATCH"
)

func (e ConnectV1AlterOffsetRequestType) Valid() bool {
	switch e {
	case DELETE:
		return true
	case PATCH:
		return true
	default:
		return false
	}
}

const (
	ConnectV1ConnectorTypeSink   ConnectV1ConnectorType = "sink"
	ConnectV1ConnectorTypeSource ConnectV1ConnectorType = "source"
)

func (e ConnectV1ConnectorType) Valid() bool {
	switch e {
	case ConnectV1ConnectorTypeSink:
		return true
	case ConnectV1ConnectorTypeSource:
		return true
	default:
		return false
	}
}

const (
	ConnectV1ConnectorExpansionStatusConnectorStateDEGRADED     ConnectV1ConnectorExpansionStatusConnectorState = "DEGRADED"
	ConnectV1ConnectorExpansionStatusConnectorStateDELETED      ConnectV1ConnectorExpansionStatusConnectorState = "DELETED"
	ConnectV1ConnectorExpansionStatusConnectorStateFAILED       ConnectV1ConnectorExpansionStatusConnectorState = "FAILED"
	ConnectV1ConnectorExpansionStatusConnectorStateNONE         ConnectV1ConnectorExpansionStatusConnectorState = "NONE"
	ConnectV1ConnectorExpansionStatusConnectorStatePAUSED       ConnectV1ConnectorExpansionStatusConnectorState = "PAUSED"
	ConnectV1ConnectorExpansionStatusConnectorStatePROVISIONING ConnectV1ConnectorExpansionStatusConnectorState = "PROVISIONING"
	ConnectV1ConnectorExpansionStatusConnectorStateRUNNING      ConnectV1ConnectorExpansionStatusConnectorState = "RUNNING"
)

func (e ConnectV1ConnectorExpansionStatusConnectorState) Valid() bool {
	switch e {
	case ConnectV1ConnectorExpansionStatusConnectorStateDEGRADED:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStateDELETED:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStateFAILED:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStateNONE:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStatePAUSED:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStatePROVISIONING:
		return true
	case ConnectV1ConnectorExpansionStatusConnectorStateRUNNING:
		return true
	default:
		return false
	}
}

const (
	ConnectV1ConnectorExpansionStatusTypeSink   ConnectV1ConnectorExpansionStatusType = "sink"
	ConnectV1ConnectorExpansionStatusTypeSource ConnectV1ConnectorExpansionStatusType = "source"
)

func (e ConnectV1ConnectorExpansionStatusType) Valid() bool {
	switch e {
	case ConnectV1ConnectorExpansionStatusTypeSink:
		return true
	case ConnectV1ConnectorExpansionStatusTypeSource:
		return true
	default:
		return false
	}
}

const (
	ConnectV1ConnectorWithOffsetsTypeSink   ConnectV1ConnectorWithOffsetsType = "sink"
	ConnectV1ConnectorWithOffsetsTypeSource ConnectV1ConnectorWithOffsetsType = "source"
)

func (e ConnectV1ConnectorWithOffsetsType) Valid() bool {
	switch e {
	case ConnectV1ConnectorWithOffsetsTypeSink:
		return true
	case ConnectV1ConnectorWithOffsetsTypeSource:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorPluginApiVersionConnectv1 ConnectV1CustomConnectorPluginApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorPluginApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorPluginApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorPluginKindCustomConnectorPlugin ConnectV1CustomConnectorPluginKind = "CustomConnectorPlugin"
)

func (e ConnectV1CustomConnectorPluginKind) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorPluginKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorPluginListApiVersionConnectv1 ConnectV1CustomConnectorPluginListApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorPluginListApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorPluginListApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorPluginListDataApiVersionConnectv1 ConnectV1CustomConnectorPluginListDataApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorPluginListDataApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorPluginListDataApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorPluginListDataKindCustomConnectorPlugin ConnectV1CustomConnectorPluginListDataKind = "CustomConnectorPlugin"
)

func (e ConnectV1CustomConnectorPluginListDataKind) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorPluginListDataKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	CustomConnectorPluginList ConnectV1CustomConnectorPluginListKind = "CustomConnectorPluginList"
)

func (e ConnectV1CustomConnectorPluginListKind) Valid() bool {
	switch e {
	case CustomConnectorPluginList:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorRuntimeApiVersionConnectv1 ConnectV1CustomConnectorRuntimeApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorRuntimeApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorRuntimeApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorRuntimeKindCustomConnectorRuntime ConnectV1CustomConnectorRuntimeKind = "CustomConnectorRuntime"
)

func (e ConnectV1CustomConnectorRuntimeKind) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorRuntimeKindCustomConnectorRuntime:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorRuntimeListApiVersionConnectv1 ConnectV1CustomConnectorRuntimeListApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorRuntimeListApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorRuntimeListApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorRuntimeListDataApiVersionConnectv1 ConnectV1CustomConnectorRuntimeListDataApiVersion = "connect/v1"
)

func (e ConnectV1CustomConnectorRuntimeListDataApiVersion) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorRuntimeListDataApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1CustomConnectorRuntimeListDataKindCustomConnectorRuntime ConnectV1CustomConnectorRuntimeListDataKind = "CustomConnectorRuntime"
)

func (e ConnectV1CustomConnectorRuntimeListDataKind) Valid() bool {
	switch e {
	case ConnectV1CustomConnectorRuntimeListDataKindCustomConnectorRuntime:
		return true
	default:
		return false
	}
}

const (
	CustomConnectorRuntimeList ConnectV1CustomConnectorRuntimeListKind = "CustomConnectorRuntimeList"
)

func (e ConnectV1CustomConnectorRuntimeListKind) Valid() bool {
	switch e {
	case CustomConnectorRuntimeList:
		return true
	default:
		return false
	}
}

const (
	ConnectV1PresignedUrlApiVersionConnectv1 ConnectV1PresignedUrlApiVersion = "connect/v1"
)

func (e ConnectV1PresignedUrlApiVersion) Valid() bool {
	switch e {
	case ConnectV1PresignedUrlApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1PresignedUrlKindPresignedUrl ConnectV1PresignedUrlKind = "PresignedUrl"
)

func (e ConnectV1PresignedUrlKind) Valid() bool {
	switch e {
	case ConnectV1PresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	ConnectV1PresignedUrlRequestApiVersionConnectv1 ConnectV1PresignedUrlRequestApiVersion = "connect/v1"
)

func (e ConnectV1PresignedUrlRequestApiVersion) Valid() bool {
	switch e {
	case ConnectV1PresignedUrlRequestApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	ConnectV1PresignedUrlRequestKindPresignedUrlRequest ConnectV1PresignedUrlRequestKind = "PresignedUrlRequest"
)

func (e ConnectV1PresignedUrlRequestKind) Valid() bool {
	switch e {
	case ConnectV1PresignedUrlRequestKindPresignedUrlRequest:
		return true
	default:
		return false
	}
}

const (
	CreateConnectV1CustomConnectorPluginJSONBodyApiVersionConnectv1 CreateConnectV1CustomConnectorPluginJSONBodyApiVersion = "connect/v1"
)

func (e CreateConnectV1CustomConnectorPluginJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateConnectV1CustomConnectorPluginJSONBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	CreateConnectV1CustomConnectorPluginJSONBodyKindCustomConnectorPlugin CreateConnectV1CustomConnectorPluginJSONBodyKind = "CustomConnectorPlugin"
)

func (e CreateConnectV1CustomConnectorPluginJSONBodyKind) Valid() bool {
	switch e {
	case CreateConnectV1CustomConnectorPluginJSONBodyKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	CreateConnectV1CustomConnectorPlugin201JSONResponseBodyApiVersionConnectv1 CreateConnectV1CustomConnectorPlugin201JSONResponseBodyApiVersion = "connect/v1"
)

func (e CreateConnectV1CustomConnectorPlugin201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateConnectV1CustomConnectorPlugin201JSONResponseBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	CreateConnectV1CustomConnectorPlugin201JSONResponseBodyKindCustomConnectorPlugin CreateConnectV1CustomConnectorPlugin201JSONResponseBodyKind = "CustomConnectorPlugin"
)

func (e CreateConnectV1CustomConnectorPlugin201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateConnectV1CustomConnectorPlugin201JSONResponseBodyKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	GetConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersionConnectv1 GetConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion = "connect/v1"
)

func (e GetConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	GetConnectV1CustomConnectorPlugin200JSONResponseBodyKindCustomConnectorPlugin GetConnectV1CustomConnectorPlugin200JSONResponseBodyKind = "CustomConnectorPlugin"
)

func (e GetConnectV1CustomConnectorPlugin200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetConnectV1CustomConnectorPlugin200JSONResponseBodyKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersionConnectv1 UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion = "connect/v1"
)

func (e UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyKindCustomConnectorPlugin UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyKind = "CustomConnectorPlugin"
)

func (e UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyKindCustomConnectorPlugin:
		return true
	default:
		return false
	}
}

const (
	ListConnectv1ConnectorPlugins200JSONResponseBodyTypeSink   ListConnectv1ConnectorPlugins200JSONResponseBodyType = "sink"
	ListConnectv1ConnectorPlugins200JSONResponseBodyTypeSource ListConnectv1ConnectorPlugins200JSONResponseBodyType = "source"
)

func (e ListConnectv1ConnectorPlugins200JSONResponseBodyType) Valid() bool {
	switch e {
	case ListConnectv1ConnectorPlugins200JSONResponseBodyTypeSink:
		return true
	case ListConnectv1ConnectorPlugins200JSONResponseBodyTypeSource:
		return true
	default:
		return false
	}
}

const (
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceHIGH   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance = "HIGH"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceLOW    ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance = "LOW"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceMEDIUM ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance = "MEDIUM"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceNONE   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance = "NONE"
)

func (e ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance) Valid() bool {
	switch e {
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceHIGH:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceLOW:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceMEDIUM:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportanceNONE:
		return true
	default:
		return false
	}
}

const (
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeBOOLEAN  ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "BOOLEAN"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeDOUBLE   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "DOUBLE"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeENUM     ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "ENUM"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeINT      ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "INT"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeLIST     ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "LIST"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeLONG     ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "LONG"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeNONE     ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "NONE"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypePASSWORD ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "PASSWORD"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeSHORT    ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "SHORT"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeSTRING   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType = "STRING"
)

func (e ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType) Valid() bool {
	switch e {
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeBOOLEAN:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeDOUBLE:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeENUM:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeINT:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeLIST:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeLONG:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeNONE:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypePASSWORD:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeSHORT:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionTypeSTRING:
		return true
	default:
		return false
	}
}

const (
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthLONG   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth = "LONG"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthMEDIUM ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth = "MEDIUM"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthNONE   ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth = "NONE"
	ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthSHORT  ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth = "SHORT"
)

func (e ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth) Valid() bool {
	switch e {
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthLONG:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthMEDIUM:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthNONE:
		return true
	case ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidthSHORT:
		return true
	default:
		return false
	}
}

const (
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateDEGRADED     ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "DEGRADED"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateDELETED      ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "DELETED"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateFAILED       ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "FAILED"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateNONE         ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "NONE"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStatePAUSED       ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "PAUSED"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStatePROVISIONING ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "PROVISIONING"
	ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateRUNNING      ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState = "RUNNING"
)

func (e ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState) Valid() bool {
	switch e {
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateDEGRADED:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateDELETED:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateFAILED:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateNONE:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStatePAUSED:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStatePROVISIONING:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorStateRUNNING:
		return true
	default:
		return false
	}
}

const (
	ReadConnectv1ConnectorStatus200JSONResponseBodyTypeSink   ReadConnectv1ConnectorStatus200JSONResponseBodyType = "sink"
	ReadConnectv1ConnectorStatus200JSONResponseBodyTypeSource ReadConnectv1ConnectorStatus200JSONResponseBodyType = "source"
)

func (e ReadConnectv1ConnectorStatus200JSONResponseBodyType) Valid() bool {
	switch e {
	case ReadConnectv1ConnectorStatus200JSONResponseBodyTypeSink:
		return true
	case ReadConnectv1ConnectorStatus200JSONResponseBodyTypeSource:
		return true
	default:
		return false
	}
}

const (
	Id     ListConnectv1ConnectorsWithExpansionsParamsExpand = "id"
	Info   ListConnectv1ConnectorsWithExpansionsParamsExpand = "info"
	Status ListConnectv1ConnectorsWithExpansionsParamsExpand = "status"
)

func (e ListConnectv1ConnectorsWithExpansionsParamsExpand) Valid() bool {
	switch e {
	case Id:
		return true
	case Info:
		return true
	case Status:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersionConnectv1 PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersion = "connect/v1"
)

func (e PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlConnectV1PresignedUrlJSONBodyKindPresignedUrlRequest PresignedUploadUrlConnectV1PresignedUrlJSONBodyKind = "PresignedUrlRequest"
)

func (e PresignedUploadUrlConnectV1PresignedUrlJSONBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlConnectV1PresignedUrlJSONBodyKindPresignedUrlRequest:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersionConnectv1 PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion = "connect/v1"
)

func (e PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersionConnectv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKindPresignedUrl PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind = "PresignedUrl"
)

func (e PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKindPresignedUrl:
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
type ConnectV1AlterOffsetRequest struct {
	// Offsets Array of offsets which are categorised into partitions.
	Offsets *ConnectV1Offsets `json:"offsets,omitempty"`

	// Type The type of alter operation. PATCH will update the offset to the provided values.
	// The update will only happen for the partitions provided in the request.
	// DELETE will delete the offset for the provided partitions and reset them back to the
	// base state. It is as if, a fresh new connector was created.
	//
	// For sink connectors PATCH/DELETE will move the offsets to the provided point in the
	// topic partition. If the offset provided is not present in the topic partition it will
	// by default reset to the earliest offset in the topic partition.
	//
	// For source connectors, post PATCH/DELETE the connector will attempt to read from the
	// position defined in the altered offsets.
	Type ConnectV1AlterOffsetRequestType `json:"type"`
}
type ConnectV1AlterOffsetRequestInfo struct {
	// Id The ID of the connector.
	Id string `json:"id"`

	// Name The name of the connector.
	Name string `json:"name"`

	// Offsets Array of offsets which are categorised into partitions.
	Offsets *ConnectV1Offsets `json:"offsets,omitempty"`

	// RequestedAt The time at which the request was made. The time is in UTC, ISO 8601 format.
	RequestedAt time.Time `json:"requested_at"`

	// Type The type of alter operation. PATCH will update the offset to the provided values.
	// The update will only happen for the partitions provided in the request.
	// DELETE will delete the offset for the provided partitions and reset them back to the
	// base state. It is as if, a fresh new connector was created.
	//
	// For sink connectors PATCH/DELETE will move the offsets to the provided point in the
	// topic partition. If the offset provided is not present in the topic partition it will
	// by default reset to the earliest offset in the topic partition.
	//
	// For source connectors, post PATCH/DELETE the connector will attempt to read from the
	// position defined in the altered offsets.
	Type ConnectV1AlterOffsetRequestType `json:"type"`
}
type ConnectV1AlterOffsetRequestType string
type ConnectV1AlterOffsetStatus struct {
	// AppliedAt The time at which the offsets were applied. The time is in UTC, ISO 8601 format.
	AppliedAt *time.Time `json:"applied_at,omitempty"`

	// PreviousOffsets Array of offsets which are categorised into partitions.
	PreviousOffsets *ConnectV1Offsets `json:"previous_offsets,omitempty"`

	// Request The request made to alter offsets.
	Request ConnectV1AlterOffsetRequestInfo `json:"request"`

	// Status The response of the alter offsets operation.
	Status struct {
		// Message An info message from the alter offset operation.
		Message *string `json:"message,omitempty"`

		// Phase The phase of the alter offset operation.
		//
		// PENDING: The offset alter operation is in progress.
		//
		// APPLIED: The offset alter operation has been applied to the connector.
		//
		// FAILED:  The offset alter operation has failed to be applied to the connector.
		Phase string `json:"phase"`
	} `json:"status"`
}
type ConnectV1Connector struct {
	// Config Configuration parameters for the connector. These configurations
	// are the minimum set of key-value pairs (KVP) which can be used to
	// define how the connector connects Kafka to the external system.
	// Some of these KVPs are common to all the connectors, such as
	// connection parameters to Kafka, connector metadata, etc. The list
	// of common connector configurations is as follows
	//
	// - cloud.environment
	// - cloud.provider
	// - connector.class
	// - kafka.api.key
	// - kafka.api.secret
	// - kafka.endpoint
	// - kafka.region
	// - name
	//
	// A specific connector such as `GcsSink` would have additional
	// parameters such as `gcs.bucket.name`, `flush.size`, etc.
	Config ConnectV1Connector_Config `json:"config"`

	// Name Name of the connector
	Name string `json:"name"`

	// Tasks List of active tasks generated by the connector
	Tasks *[]struct {
		// Connector The name of the connector the task belongs to
		Connector string `json:"connector"`

		// Task Task ID within the connector
		Task int `json:"task"`
	} `json:"tasks,omitempty"`

	// Type Type of connector, sink or source
	Type *ConnectV1ConnectorType `json:"type,omitempty"`
}
type ConnectV1Connector_Config struct {
	// CloudEnvironment The cloud environment type.
	CloudEnvironment string `json:"cloud.environment"`

	// CloudProvider The cloud service provider, e.g. aws, azure, etc.
	CloudProvider string `json:"cloud.provider"`

	// ConnectorClass The connector class name. E.g. BigQuerySink, GcsSink, etc.
	ConnectorClass string `json:"connector.class"`

	// KafkaApiKey The kafka cluster api key.
	KafkaApiKey string `json:"kafka.api.key"`

	// KafkaApiSecret The kafka cluster api secret key.
	KafkaApiSecret string `json:"kafka.api.secret"`

	// KafkaEndpoint The kafka cluster endpoint.
	KafkaEndpoint string `json:"kafka.endpoint"`

	// KafkaRegion The kafka cluster region.
	KafkaRegion string `json:"kafka.region"`

	// Name Name or alias of the class (plugin) for this connector.
	Name                 string            `json:"name"`
	AdditionalProperties map[string]string `json:"-"`
}
type ConnectV1ConnectorType string
type ConnectV1ConnectorError struct {
	// Error Connector Error with error code and message.
	Error *struct {
		// Code Error code for the type of error
		Code *int `json:"code,omitempty"`

		// Message Human readable error message
		Message *string `json:"message,omitempty"`
	} `json:"error,omitempty"`
}
type ConnectV1ConnectorExpansion struct {
	// Id The ID of connector.
	Id *struct {
		// Id The ID of the connector.
		Id *string `json:"id,omitempty"`

		// IdType Type of the value in the `id` property.
		IdType *string `json:"id_type,omitempty"`
	} `json:"id,omitempty"`

	// Info Metadata of the connector.
	Info *struct {
		// Config Configuration parameters for the connector. These configurations
		// are the minimum set of key-value pairs (KVP) which are used to
		// define how the connector connects Kafka to the external system.
		// Some of these KVPs are common to all the connectors, such as
		// connection parameters to Kafka, connector metadata, etc. The list
		// of common connector configurations is as follows
		//
		//   - cloud.environment
		//   - cloud.provider
		//   - connector.class
		//   - kafka.api.key
		//   - kafka.api.secret
		//   - kafka.endpoint
		//   - kafka.region
		//   - name
		//
		// For example, a connector like `GcsSink` would have additional
		// parameters such as `gcs.bucket.name`, `flush.size`, etc.
		Config *ConnectV1ConnectorExpansion_Info_Config `json:"config,omitempty"`

		// Name Name of the connector.
		Name *string `json:"name,omitempty"`
	} `json:"info,omitempty"`

	// Status Status of the connector and its tasks.
	Status *struct {
		// Connector A map containing connector status.
		Connector struct {
			// State The state of the connector.
			State ConnectV1ConnectorExpansionStatusConnectorState `json:"state"`

			// Trace Exception message in case of an error.
			Trace *string `json:"trace,omitempty"`

			// WorkerId The worker ID of the connector.
			WorkerId string `json:"worker_id"`
		} `json:"connector"`

		// Name The name of the connector.
		Name string `json:"name"`

		// Tasks A map containing the task status.
		Tasks *[]struct {
			// Id The ID of task.
			Id  int     `json:"id"`
			Msg *string `json:"msg,omitempty"`

			// State The state of the task.
			State string `json:"state"`

			// WorkerId The worker ID of the task.
			WorkerId string `json:"worker_id"`
		} `json:"tasks,omitempty"`

		// Type Type of connector, sink or source.
		Type ConnectV1ConnectorExpansionStatusType `json:"type"`
	} `json:"status,omitempty"`
}
type ConnectV1ConnectorExpansion_Info_Config struct {
	// CloudEnvironment The cloud environment type.
	CloudEnvironment string `json:"cloud.environment"`

	// CloudProvider The cloud service provider, e.g. aws, azure, etc.
	CloudProvider string `json:"cloud.provider"`

	// ConnectorClass The connector class name. E.g. BigQuerySink, GcsSink, etc.
	ConnectorClass string `json:"connector.class"`

	// KafkaApiKey The kafka cluster api key.
	KafkaApiKey string `json:"kafka.api.key"`

	// KafkaApiSecret The kafka cluster api secret key.
	KafkaApiSecret string `json:"kafka.api.secret"`

	// KafkaEndpoint The kafka cluster endpoint.
	KafkaEndpoint string `json:"kafka.endpoint"`

	// KafkaRegion The kafka cluster region.
	KafkaRegion string `json:"kafka.region"`

	// Name Name or alias of the class (plugin) for this connector.
	Name                 string            `json:"name"`
	AdditionalProperties map[string]string `json:"-"`
}
type ConnectV1ConnectorExpansionStatusConnectorState string
type ConnectV1ConnectorExpansionStatusType string
type ConnectV1ConnectorExpansionMap map[string]ConnectV1ConnectorExpansion
type ConnectV1ConnectorOffsets struct {
	// Id The ID of the connector.
	Id *string `json:"id,omitempty"`

	// Metadata Metadata of the connector offset.
	Metadata *struct {
		// ObservedAt The time at which the offsets were observed. The time is in UTC, ISO 8601 format.
		ObservedAt *time.Time `json:"observed_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The name of the connector.
	Name *string `json:"name,omitempty"`

	// Offsets Array of offsets which are categorised into partitions.
	Offsets *ConnectV1Offsets `json:"offsets,omitempty"`
}
type ConnectV1ConnectorWithOffsets struct {
	// Config Configuration parameters for the connector. These configurations
	// are the minimum set of key-value pairs which can be used to
	// define how the connector connects Kafka to the external system.
	// Some of these key-value pairs are common to all the connectors, such as
	// connection parameters to Kafka, connector metadata, etc. The list
	// of common connector configurations is as follows
	// - cloud.environment
	// - cloud.provider
	// - connector.class
	// - kafka.api.key
	// - kafka.api.secret
	// - kafka.endpoint
	// - kafka.region
	// - name
	// A specific connector such as `GcsSink` would have additional
	// parameters such as `gcs.bucket.name`, `flush.size`, etc.
	Config ConnectV1ConnectorWithOffsets_Config `json:"config"`

	// Name Name of the connector
	Name string `json:"name"`

	// Offsets Array of offsets which are categorised into partitions.
	Offsets *ConnectV1Offsets `json:"offsets,omitempty"`

	// Tasks List of active tasks generated by the connector
	Tasks *[]struct {
		// Connector The name of the connector the task belongs to
		Connector string `json:"connector"`

		// Task Task ID within the connector
		Task int `json:"task"`
	} `json:"tasks,omitempty"`

	// Type Type of connector, sink or source
	Type *ConnectV1ConnectorWithOffsetsType `json:"type,omitempty"`
}
type ConnectV1ConnectorWithOffsets_Config struct {
	// CloudEnvironment The cloud environment type.
	CloudEnvironment string `json:"cloud.environment"`

	// CloudProvider The cloud service provider, e.g. aws, azure, etc.
	CloudProvider string `json:"cloud.provider"`

	// ConnectorClass The connector class name. E.g. BigQuerySink, GcsSink, etc.
	ConnectorClass string `json:"connector.class"`

	// KafkaApiKey The Kafka cluster API key.
	KafkaApiKey string `json:"kafka.api.key"`

	// KafkaApiSecret The Kafka cluster API secret.
	KafkaApiSecret string `json:"kafka.api.secret"`

	// KafkaEndpoint The Kafka cluster endpoint.
	KafkaEndpoint string `json:"kafka.endpoint"`

	// KafkaRegion The Kafka cluster region.
	KafkaRegion string `json:"kafka.region"`

	// Name Name or alias of the class (plugin) for this connector.
	Name                 string            `json:"name"`
	AdditionalProperties map[string]string `json:"-"`
}
type ConnectV1ConnectorWithOffsetsType string
type ConnectV1Connectors = []struct {
	// Config Configuration parameters for the connector. These configurations
	// are the minimum set of key-value pairs (KVP) which can be used to
	// define how the connector connects Kafka to the external system.
	// Some of these KVPs are common to all the connectors, such as
	// connection parameters to Kafka, connector metadata, etc. The list
	// of common connector configurations is as follows
	//
	//   - cloud.environment
	//   - cloud.provider
	//   - connector.class
	//   - kafka.api.key
	//   - kafka.api.secret
	//   - kafka.endpoint
	//   - kafka.region
	//   - name
	//
	// A specific connector such as `GcsSink` would have additional
	// parameters such as `gcs.bucket.name`, `flush.size`, etc.
	Config *ConnectV1Connectors_Config `json:"config,omitempty"`

	// Id The ID of task.
	Id *struct {
		// Connector The name of the connector the task belongs to.
		Connector *string `json:"connector,omitempty"`

		// Task Task ID within the connector.
		Task *int `json:"task,omitempty"`
	} `json:"id,omitempty"`
}
type ConnectV1Connectors_Config struct {
	// CloudEnvironment The cloud environment type.
	CloudEnvironment string `json:"cloud.environment"`

	// CloudProvider The cloud service provider, e.g. aws, azure, etc.
	CloudProvider string `json:"cloud.provider"`

	// ConnectorClass The connector class name. E.g. BigQuerySink, GcsSink, etc.
	ConnectorClass string `json:"connector.class"`

	// KafkaApiKey The kafka cluster api key.
	KafkaApiKey string `json:"kafka.api.key"`

	// KafkaApiSecret The kafka cluster api secret key.
	KafkaApiSecret string `json:"kafka.api.secret"`

	// KafkaEndpoint The kafka cluster endpoint.
	KafkaEndpoint string `json:"kafka.endpoint"`

	// KafkaRegion The kafka cluster region.
	KafkaRegion string `json:"kafka.region"`

	// Name Name or alias of the class (plugin) for this connector.
	Name                 string            `json:"name"`
	AdditionalProperties map[string]string `json:"-"`
}
type ConnectV1CustomConnectorPlugin struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ConnectV1CustomConnectorPluginApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ConnectorClass Java class or alias for connector. You can get connector class from connector documentation provided by developer.
	ConnectorClass *string `json:"connector_class,omitempty"`

	// ConnectorType Custom Connector type.
	ConnectorType *string `json:"connector_type,omitempty"`

	// ContentFormat Archive format of Custom Connector Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of Custom Connector Plugin.
	Description *string `json:"description,omitempty"`

	// DisplayName Display name of Custom Connector Plugin.
	DisplayName *string `json:"display_name,omitempty"`

	// DocumentationLink Document link of Custom Connector Plugin.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ConnectV1CustomConnectorPluginKind `json:"kind,omitempty"`
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

	// RuntimeLanguage Runtime language of Custom Connector Plugin.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
	// value when setting up connector.
	SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

	// UploadSource Upload source of Custom Connector Plugin. Only required in `create` request, will be ignored in `read`, `update` or `list`.
	UploadSource *ConnectV1CustomConnectorPlugin_UploadSource `json:"upload_source,omitempty"`
}
type ConnectV1CustomConnectorPluginApiVersion string
type ConnectV1CustomConnectorPluginKind string
type ConnectV1CustomConnectorPlugin_UploadSource struct {
	union json.RawMessage
}
type ConnectV1CustomConnectorPluginList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ConnectV1CustomConnectorPluginListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ConnectV1CustomConnectorPluginListDataApiVersion `json:"api_version,omitempty"`

		// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
		Cloud *string `json:"cloud,omitempty"`

		// ConnectorClass Java class or alias for connector. You can get connector class from connector documentation provided by developer.
		ConnectorClass string `json:"connector_class"`

		// ConnectorType Custom Connector type.
		ConnectorType string `json:"connector_type"`

		// ContentFormat Archive format of Custom Connector Plugin.
		ContentFormat *string `json:"content_format,omitempty"`

		// Description Description of Custom Connector Plugin.
		Description *string `json:"description,omitempty"`

		// DisplayName Display name of Custom Connector Plugin.
		DisplayName string `json:"display_name"`

		// DocumentationLink Document link of Custom Connector Plugin.
		DocumentationLink *string `json:"documentation_link,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *ConnectV1CustomConnectorPluginListDataKind `json:"kind,omitempty"`
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

		// RuntimeLanguage Runtime language of Custom Connector Plugin.
		RuntimeLanguage *string `json:"runtime_language,omitempty"`

		// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
		// value when setting up connector.
		SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

		// UploadSource Upload source of Custom Connector Plugin. Only required in `create` request, will be ignored in `read`, `update` or `list`.
		UploadSource ConnectV1CustomConnectorPluginList_Data_UploadSource `json:"upload_source"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ConnectV1CustomConnectorPluginListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ConnectV1CustomConnectorPluginListApiVersion string
type ConnectV1CustomConnectorPluginListDataApiVersion string
type ConnectV1CustomConnectorPluginListDataKind string
type ConnectV1CustomConnectorPluginList_Data_UploadSource struct {
	union json.RawMessage
}
type ConnectV1CustomConnectorPluginListKind string
type ConnectV1CustomConnectorRuntime struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ConnectV1CustomConnectorRuntimeApiVersion `json:"api_version,omitempty"`

	// CustomConnectPluginRuntimeName Name of the runtime that is being used while provisioning a custom connector. This corresponds to
	// the property custom.connect.plugin.runtime in the connector configuration.
	CustomConnectPluginRuntimeName *string `json:"custom_connect_plugin_runtime_name,omitempty"`

	// Description Description of the runtime
	Description *string `json:"description,omitempty"`

	// EndOfLifeAt End of Life date for the runtime
	EndOfLifeAt *time.Time `json:"end_of_life_at,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *ConnectV1CustomConnectorRuntimeKind `json:"kind,omitempty"`

	// ProductMaturity The product maturity phase for the plugin runtime.
	// EA (Early Access), GA (Generally Available), or Preview.
	ProductMaturity *string `json:"product_maturity,omitempty"`

	// RuntimeAkVersion The underlying version of Apache Kafka which bundles the connect runtime
	RuntimeAkVersion *string `json:"runtime_ak_version,omitempty"`

	// SupportedJavaVersions List of supported Java versions
	SupportedJavaVersions *[]string `json:"supported_java_versions,omitempty"`
}
type ConnectV1CustomConnectorRuntimeApiVersion string
type ConnectV1CustomConnectorRuntimeKind string
type ConnectV1CustomConnectorRuntimeList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ConnectV1CustomConnectorRuntimeListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ConnectV1CustomConnectorRuntimeListDataApiVersion `json:"api_version,omitempty"`

		// CustomConnectPluginRuntimeName Name of the runtime that is being used while provisioning a custom connector. This corresponds to
		// the property custom.connect.plugin.runtime in the connector configuration.
		CustomConnectPluginRuntimeName *string `json:"custom_connect_plugin_runtime_name,omitempty"`

		// Description Description of the runtime
		Description *string `json:"description,omitempty"`

		// EndOfLifeAt End of Life date for the runtime
		EndOfLifeAt *time.Time `json:"end_of_life_at,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind *ConnectV1CustomConnectorRuntimeListDataKind `json:"kind,omitempty"`

		// ProductMaturity The product maturity phase for the plugin runtime.
		// EA (Early Access), GA (Generally Available), or Preview.
		ProductMaturity *string `json:"product_maturity,omitempty"`

		// RuntimeAkVersion The underlying version of Apache Kafka which bundles the connect runtime
		RuntimeAkVersion *string `json:"runtime_ak_version,omitempty"`

		// SupportedJavaVersions List of supported Java versions
		SupportedJavaVersions *[]string `json:"supported_java_versions,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ConnectV1CustomConnectorRuntimeListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ConnectV1CustomConnectorRuntimeListApiVersion string
type ConnectV1CustomConnectorRuntimeListDataApiVersion string
type ConnectV1CustomConnectorRuntimeListDataKind string
type ConnectV1CustomConnectorRuntimeListKind string
type ConnectV1Offsets = []struct {
	// Offset The offset of the partition. For sink connectors this is the kafka offset. For
	// source connectors this is depends on the offset defined by the source connector.
	// For example, the timestamp and incrementing column info in a table, for a JDBC based
	// MySQL source connector.
	// Please refer to the [documentation](https://docs.confluent.io/cloud/current/connectors/offsets.html#manage-offsets-for-fully-managed-connectors-in-ccloud) for
	// more information.
	Offset *map[string]interface{} `json:"offset,omitempty"`

	// Partition The partition information. For sink connectors this is the kafka topic and
	// partition. For source connectors this is depends on the partitions defined by the
	// source connector. For example, the table which this task is pulling data from in a
	// JDBC based MySQL source connector.
	// Please refer to the [documentation](https://docs.confluent.io/cloud/current/connectors/offsets.html#manage-offsets-for-fully-managed-connectors-in-ccloud) for
	// more information.
	Partition *map[string]interface{} `json:"partition,omitempty"`
}
type ConnectV1PresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ConnectV1PresignedUrlApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Custom Connector Plugin archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *ConnectV1PresignedUrlKind `json:"kind,omitempty"`

	// UploadFormData Upload form data of the Custom Connector Plugin. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Custom Connector Plugin archive.
	UploadUrl *string `json:"upload_url,omitempty"`
}
type ConnectV1PresignedUrlApiVersion string
type ConnectV1PresignedUrlKind string
type ConnectV1PresignedUrlRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ConnectV1PresignedUrlRequestApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Custom Connector Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ConnectV1PresignedUrlRequestKind `json:"kind,omitempty"`
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
}
type ConnectV1PresignedUrlRequestApiVersion string
type ConnectV1PresignedUrlRequestKind string
type ConnectV1UploadSourcePresignedUrl struct {
	// Location Location of the Custom Connector Plugin source.
	Location string `json:"location"`

	// UploadId Upload ID returned by the `/presigned-upload-url` API. This field returns an empty string in all responses.
	UploadId string `json:"upload_id"`
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
type UnauthenticatedError = Failure
type UnauthorizedError = Failure
type ValidationError = Failure
type ConnectV1AccountNotFoundError = ConnectV1ConnectorError
type ConnectV1BadRequestError = ConnectV1ConnectorError
type ConnectV1DefaultSystemError = ConnectV1ConnectorError
type ConnectV1ForbiddenError = ConnectV1ConnectorError
type ConnectV1OK struct {
	Error *map[string]interface{} `json:"error,omitempty"`
}
type ConnectV1ResourceNotFoundError = ConnectV1ConnectorError
type ConnectV1UnauthenticatedError = ConnectV1ConnectorError
type PresignedUploadUrlConnectV1PresignedUrlJSONBody struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Custom Connector Plugin.
	ContentFormat string `json:"content_format"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *PresignedUploadUrlConnectV1PresignedUrlJSONBodyKind `json:"kind,omitempty"`
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
}
type PresignedUploadUrlConnectV1PresignedUrlJSONBodyApiVersion string
type PresignedUploadUrlConnectV1PresignedUrlJSONBodyKind string
type PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion string
type PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind string
type PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody PresignedUploadUrlConnectV1PresignedUrlJSONBody

func (a ConnectV1Connector_Config) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *ConnectV1Connector_Config) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *ConnectV1Connector_Config) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["cloud.environment"]; found {
		err = json.Unmarshal(raw, &a.CloudEnvironment)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.environment': %w", err)
		}
		delete(object, "cloud.environment")
	}

	if raw, found := object["cloud.provider"]; found {
		err = json.Unmarshal(raw, &a.CloudProvider)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.provider': %w", err)
		}
		delete(object, "cloud.provider")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["kafka.endpoint"]; found {
		err = json.Unmarshal(raw, &a.KafkaEndpoint)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.endpoint': %w", err)
		}
		delete(object, "kafka.endpoint")
	}

	if raw, found := object["kafka.region"]; found {
		err = json.Unmarshal(raw, &a.KafkaRegion)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.region': %w", err)
		}
		delete(object, "kafka.region")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a ConnectV1Connector_Config) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	object["cloud.environment"], err = json.Marshal(a.CloudEnvironment)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.environment': %w", err)
	}

	object["cloud.provider"], err = json.Marshal(a.CloudProvider)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.provider': %w", err)
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["kafka.endpoint"], err = json.Marshal(a.KafkaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.endpoint': %w", err)
	}

	object["kafka.region"], err = json.Marshal(a.KafkaRegion)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.region': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a ConnectV1ConnectorExpansion_Info_Config) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *ConnectV1ConnectorExpansion_Info_Config) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *ConnectV1ConnectorExpansion_Info_Config) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["cloud.environment"]; found {
		err = json.Unmarshal(raw, &a.CloudEnvironment)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.environment': %w", err)
		}
		delete(object, "cloud.environment")
	}

	if raw, found := object["cloud.provider"]; found {
		err = json.Unmarshal(raw, &a.CloudProvider)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.provider': %w", err)
		}
		delete(object, "cloud.provider")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["kafka.endpoint"]; found {
		err = json.Unmarshal(raw, &a.KafkaEndpoint)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.endpoint': %w", err)
		}
		delete(object, "kafka.endpoint")
	}

	if raw, found := object["kafka.region"]; found {
		err = json.Unmarshal(raw, &a.KafkaRegion)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.region': %w", err)
		}
		delete(object, "kafka.region")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a ConnectV1ConnectorExpansion_Info_Config) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	object["cloud.environment"], err = json.Marshal(a.CloudEnvironment)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.environment': %w", err)
	}

	object["cloud.provider"], err = json.Marshal(a.CloudProvider)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.provider': %w", err)
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["kafka.endpoint"], err = json.Marshal(a.KafkaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.endpoint': %w", err)
	}

	object["kafka.region"], err = json.Marshal(a.KafkaRegion)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.region': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a ConnectV1ConnectorWithOffsets_Config) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *ConnectV1ConnectorWithOffsets_Config) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *ConnectV1ConnectorWithOffsets_Config) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["cloud.environment"]; found {
		err = json.Unmarshal(raw, &a.CloudEnvironment)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.environment': %w", err)
		}
		delete(object, "cloud.environment")
	}

	if raw, found := object["cloud.provider"]; found {
		err = json.Unmarshal(raw, &a.CloudProvider)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.provider': %w", err)
		}
		delete(object, "cloud.provider")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["kafka.endpoint"]; found {
		err = json.Unmarshal(raw, &a.KafkaEndpoint)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.endpoint': %w", err)
		}
		delete(object, "kafka.endpoint")
	}

	if raw, found := object["kafka.region"]; found {
		err = json.Unmarshal(raw, &a.KafkaRegion)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.region': %w", err)
		}
		delete(object, "kafka.region")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a ConnectV1ConnectorWithOffsets_Config) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	object["cloud.environment"], err = json.Marshal(a.CloudEnvironment)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.environment': %w", err)
	}

	object["cloud.provider"], err = json.Marshal(a.CloudProvider)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.provider': %w", err)
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["kafka.endpoint"], err = json.Marshal(a.KafkaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.endpoint': %w", err)
	}

	object["kafka.region"], err = json.Marshal(a.KafkaRegion)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.region': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a ConnectV1Connectors_Config) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *ConnectV1Connectors_Config) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *ConnectV1Connectors_Config) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["cloud.environment"]; found {
		err = json.Unmarshal(raw, &a.CloudEnvironment)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.environment': %w", err)
		}
		delete(object, "cloud.environment")
	}

	if raw, found := object["cloud.provider"]; found {
		err = json.Unmarshal(raw, &a.CloudProvider)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.provider': %w", err)
		}
		delete(object, "cloud.provider")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["kafka.endpoint"]; found {
		err = json.Unmarshal(raw, &a.KafkaEndpoint)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.endpoint': %w", err)
		}
		delete(object, "kafka.endpoint")
	}

	if raw, found := object["kafka.region"]; found {
		err = json.Unmarshal(raw, &a.KafkaRegion)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.region': %w", err)
		}
		delete(object, "kafka.region")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a ConnectV1Connectors_Config) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	object["cloud.environment"], err = json.Marshal(a.CloudEnvironment)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.environment': %w", err)
	}

	object["cloud.provider"], err = json.Marshal(a.CloudProvider)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.provider': %w", err)
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["kafka.endpoint"], err = json.Marshal(a.KafkaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.endpoint': %w", err)
	}

	object["kafka.region"], err = json.Marshal(a.KafkaRegion)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.region': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a CreateConnectv1ConnectorJSONBody_Config) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *CreateConnectv1ConnectorJSONBody_Config) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *CreateConnectv1ConnectorJSONBody_Config) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["confluent.connector.type"]; found {
		err = json.Unmarshal(raw, &a.ConfluentConnectorType)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.connector.type': %w", err)
		}
		delete(object, "confluent.connector.type")
	}

	if raw, found := object["confluent.custom.connect.java.version"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectJavaVersion)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connect.java.version': %w", err)
		}
		delete(object, "confluent.custom.connect.java.version")
	}

	if raw, found := object["confluent.custom.connect.plugin.runtime"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectPluginRuntime)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connect.plugin.runtime': %w", err)
		}
		delete(object, "confluent.custom.connect.plugin.runtime")
	}

	if raw, found := object["confluent.custom.connection.endpoints"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectionEndpoints)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connection.endpoints': %w", err)
		}
		delete(object, "confluent.custom.connection.endpoints")
	}

	if raw, found := object["confluent.custom.plugin.id"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomPluginId)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.plugin.id': %w", err)
		}
		delete(object, "confluent.custom.plugin.id")
	}

	if raw, found := object["confluent.custom.schema.registry.auto"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomSchemaRegistryAuto)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.schema.registry.auto': %w", err)
		}
		delete(object, "confluent.custom.schema.registry.auto")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a CreateConnectv1ConnectorJSONBody_Config) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	if a.ConfluentConnectorType != nil {
		object["confluent.connector.type"], err = json.Marshal(a.ConfluentConnectorType)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.connector.type': %w", err)
		}
	}

	if a.ConfluentCustomConnectJavaVersion != nil {
		object["confluent.custom.connect.java.version"], err = json.Marshal(a.ConfluentCustomConnectJavaVersion)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connect.java.version': %w", err)
		}
	}

	if a.ConfluentCustomConnectPluginRuntime != nil {
		object["confluent.custom.connect.plugin.runtime"], err = json.Marshal(a.ConfluentCustomConnectPluginRuntime)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connect.plugin.runtime': %w", err)
		}
	}

	if a.ConfluentCustomConnectionEndpoints != nil {
		object["confluent.custom.connection.endpoints"], err = json.Marshal(a.ConfluentCustomConnectionEndpoints)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connection.endpoints': %w", err)
		}
	}

	if a.ConfluentCustomPluginId != nil {
		object["confluent.custom.plugin.id"], err = json.Marshal(a.ConfluentCustomPluginId)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.plugin.id': %w", err)
		}
	}

	if a.ConfluentCustomSchemaRegistryAuto != nil {
		object["confluent.custom.schema.registry.auto"], err = json.Marshal(a.ConfluentCustomSchemaRegistryAuto)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.schema.registry.auto': %w", err)
		}
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a GetConnectv1ConnectorConfig200JSONResponseBody) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *GetConnectv1ConnectorConfig200JSONResponseBody) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *GetConnectv1ConnectorConfig200JSONResponseBody) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["cloud.environment"]; found {
		err = json.Unmarshal(raw, &a.CloudEnvironment)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.environment': %w", err)
		}
		delete(object, "cloud.environment")
	}

	if raw, found := object["cloud.provider"]; found {
		err = json.Unmarshal(raw, &a.CloudProvider)
		if err != nil {
			return fmt.Errorf("error reading 'cloud.provider': %w", err)
		}
		delete(object, "cloud.provider")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["kafka.endpoint"]; found {
		err = json.Unmarshal(raw, &a.KafkaEndpoint)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.endpoint': %w", err)
		}
		delete(object, "kafka.endpoint")
	}

	if raw, found := object["kafka.region"]; found {
		err = json.Unmarshal(raw, &a.KafkaRegion)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.region': %w", err)
		}
		delete(object, "kafka.region")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a GetConnectv1ConnectorConfig200JSONResponseBody) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	object["cloud.environment"], err = json.Marshal(a.CloudEnvironment)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.environment': %w", err)
	}

	object["cloud.provider"], err = json.Marshal(a.CloudProvider)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'cloud.provider': %w", err)
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["kafka.endpoint"], err = json.Marshal(a.KafkaEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.endpoint': %w", err)
	}

	object["kafka.region"], err = json.Marshal(a.KafkaRegion)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.region': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
}
func (a CreateOrUpdateConnectv1ConnectorConfigJSONBody) Get(fieldName string) (value string, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}
func (a *CreateOrUpdateConnectv1ConnectorConfigJSONBody) Set(fieldName string, value string) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]string)
	}
	a.AdditionalProperties[fieldName] = value
}
func (a *CreateOrUpdateConnectv1ConnectorConfigJSONBody) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if raw, found := object["confluent.connector.type"]; found {
		err = json.Unmarshal(raw, &a.ConfluentConnectorType)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.connector.type': %w", err)
		}
		delete(object, "confluent.connector.type")
	}

	if raw, found := object["confluent.custom.connect.java.version"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectJavaVersion)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connect.java.version': %w", err)
		}
		delete(object, "confluent.custom.connect.java.version")
	}

	if raw, found := object["confluent.custom.connect.plugin.runtime"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectPluginRuntime)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connect.plugin.runtime': %w", err)
		}
		delete(object, "confluent.custom.connect.plugin.runtime")
	}

	if raw, found := object["confluent.custom.connection.endpoints"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomConnectionEndpoints)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.connection.endpoints': %w", err)
		}
		delete(object, "confluent.custom.connection.endpoints")
	}

	if raw, found := object["confluent.custom.plugin.id"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomPluginId)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.plugin.id': %w", err)
		}
		delete(object, "confluent.custom.plugin.id")
	}

	if raw, found := object["confluent.custom.schema.registry.auto"]; found {
		err = json.Unmarshal(raw, &a.ConfluentCustomSchemaRegistryAuto)
		if err != nil {
			return fmt.Errorf("error reading 'confluent.custom.schema.registry.auto': %w", err)
		}
		delete(object, "confluent.custom.schema.registry.auto")
	}

	if raw, found := object["connector.class"]; found {
		err = json.Unmarshal(raw, &a.ConnectorClass)
		if err != nil {
			return fmt.Errorf("error reading 'connector.class': %w", err)
		}
		delete(object, "connector.class")
	}

	if raw, found := object["kafka.api.key"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiKey)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.key': %w", err)
		}
		delete(object, "kafka.api.key")
	}

	if raw, found := object["kafka.api.secret"]; found {
		err = json.Unmarshal(raw, &a.KafkaApiSecret)
		if err != nil {
			return fmt.Errorf("error reading 'kafka.api.secret': %w", err)
		}
		delete(object, "kafka.api.secret")
	}

	if raw, found := object["name"]; found {
		err = json.Unmarshal(raw, &a.Name)
		if err != nil {
			return fmt.Errorf("error reading 'name': %w", err)
		}
		delete(object, "name")
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]string)
		for fieldName, fieldBuf := range object {
			var fieldVal string
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return fmt.Errorf("error unmarshaling field %s: %w", fieldName, err)
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}
func (a CreateOrUpdateConnectv1ConnectorConfigJSONBody) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	if a.ConfluentConnectorType != nil {
		object["confluent.connector.type"], err = json.Marshal(a.ConfluentConnectorType)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.connector.type': %w", err)
		}
	}

	if a.ConfluentCustomConnectJavaVersion != nil {
		object["confluent.custom.connect.java.version"], err = json.Marshal(a.ConfluentCustomConnectJavaVersion)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connect.java.version': %w", err)
		}
	}

	if a.ConfluentCustomConnectPluginRuntime != nil {
		object["confluent.custom.connect.plugin.runtime"], err = json.Marshal(a.ConfluentCustomConnectPluginRuntime)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connect.plugin.runtime': %w", err)
		}
	}

	if a.ConfluentCustomConnectionEndpoints != nil {
		object["confluent.custom.connection.endpoints"], err = json.Marshal(a.ConfluentCustomConnectionEndpoints)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.connection.endpoints': %w", err)
		}
	}

	if a.ConfluentCustomPluginId != nil {
		object["confluent.custom.plugin.id"], err = json.Marshal(a.ConfluentCustomPluginId)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.plugin.id': %w", err)
		}
	}

	if a.ConfluentCustomSchemaRegistryAuto != nil {
		object["confluent.custom.schema.registry.auto"], err = json.Marshal(a.ConfluentCustomSchemaRegistryAuto)
		if err != nil {
			return nil, fmt.Errorf("error marshaling 'confluent.custom.schema.registry.auto': %w", err)
		}
	}

	object["connector.class"], err = json.Marshal(a.ConnectorClass)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'connector.class': %w", err)
	}

	object["kafka.api.key"], err = json.Marshal(a.KafkaApiKey)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.key': %w", err)
	}

	object["kafka.api.secret"], err = json.Marshal(a.KafkaApiSecret)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'kafka.api.secret': %w", err)
	}

	object["name"], err = json.Marshal(a.Name)
	if err != nil {
		return nil, fmt.Errorf("error marshaling 'name': %w", err)
	}

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshaling '%s': %w", fieldName, err)
		}
	}
	return json.Marshal(object)
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
func (t ConnectV1CustomConnectorPlugin_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ConnectV1CustomConnectorPlugin_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *ConnectV1CustomConnectorPlugin_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t ConnectV1CustomConnectorPlugin_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ConnectV1CustomConnectorPlugin_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ConnectV1CustomConnectorPlugin_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ConnectV1CustomConnectorPlugin_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t ConnectV1CustomConnectorPluginList_Data_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ConnectV1CustomConnectorPluginList_Data_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *ConnectV1CustomConnectorPluginList_Data_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t ConnectV1CustomConnectorPluginList_Data_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ConnectV1CustomConnectorPluginList_Data_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ConnectV1CustomConnectorPluginList_Data_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ConnectV1CustomConnectorPluginList_Data_UploadSource) UnmarshalJSON(b []byte) error {
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
func (t CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateConnectV1CustomConnectorPluginJSONBody_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) AsConnectV1UploadSourcePresignedUrl() (ConnectV1UploadSourcePresignedUrl, error) {
	var body ConnectV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) FromConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) MergeConnectV1UploadSourcePresignedUrl(v ConnectV1UploadSourcePresignedUrl) error {
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
func (t UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsConnectV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource) UnmarshalJSON(b []byte) error {
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

	// ListConnectV1CustomConnectorPlugins List of Custom Connector Plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connector plugins.
	//
	// If no `cloud` filter is specified, returns custom connector plugins from all clouds.
	//
	// Corresponds with GET /connect/v1/custom-connector-plugins (the `ListConnectV1CustomConnectorPlugins` operationId).
	ListConnectV1CustomConnectorPlugins(ctx context.Context, params *ListConnectV1CustomConnectorPluginsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateConnectV1CustomConnectorPluginWithBody Create a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connector plugin.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /connect/v1/custom-connector-plugins (the `CreateConnectV1CustomConnectorPlugin` operationId).
	CreateConnectV1CustomConnectorPluginWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateConnectV1CustomConnectorPlugin Create a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connector plugin.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /connect/v1/custom-connector-plugins (the `CreateConnectV1CustomConnectorPlugin` operationId).
	CreateConnectV1CustomConnectorPlugin(ctx context.Context, body CreateConnectV1CustomConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteConnectV1CustomConnectorPlugin Delete a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connector plugin.
	//
	// Corresponds with DELETE /connect/v1/custom-connector-plugins/{id} (the `DeleteConnectV1CustomConnectorPlugin` operationId).
	DeleteConnectV1CustomConnectorPlugin(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetConnectV1CustomConnectorPlugin Read a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connector plugin.
	//
	// Corresponds with GET /connect/v1/custom-connector-plugins/{id} (the `GetConnectV1CustomConnectorPlugin` operationId).
	GetConnectV1CustomConnectorPlugin(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateConnectV1CustomConnectorPluginWithBody Update a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connector plugin.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /connect/v1/custom-connector-plugins/{id} (the `UpdateConnectV1CustomConnectorPlugin` operationId).
	UpdateConnectV1CustomConnectorPluginWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateConnectV1CustomConnectorPlugin Update a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connector plugin.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /connect/v1/custom-connector-plugins/{id} (the `UpdateConnectV1CustomConnectorPlugin` operationId).
	UpdateConnectV1CustomConnectorPlugin(ctx context.Context, id string, body UpdateConnectV1CustomConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListConnectV1CustomConnectorRuntimes List of Custom Connector Runtimes
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connector runtimes.
	//
	// Corresponds with GET /connect/v1/custom-connector-runtimes (the `ListConnectV1CustomConnectorRuntimes` operationId).
	ListConnectV1CustomConnectorRuntimes(ctx context.Context, params *ListConnectV1CustomConnectorRuntimesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListConnectv1ConnectorPlugins List of Managed Connector plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of Managed Connector plugins installed in the Kafka Connect cluster.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins (the `ListConnectv1ConnectorPlugins` operationId).
	ListConnectv1ConnectorPlugins(ctx context.Context, environmentId string, kafkaClusterId string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// TranslateConnectv1ConnectorPluginWithBody Translate Self Managed Connector Plugin Configurations to Fully Managed Connector Plugin Configurations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Translate the provided Self Managed configuration values. This API performs configuration translation
	// and returns the translated fully managed configuration along with any errors or warnings.
	// Query Parameter `mask_sensitive=true` redacts sensitive config values in response.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/translate?mask_sensitive=true (the `TranslateConnectv1ConnectorPlugin` operationId).
	TranslateConnectv1ConnectorPluginWithBody(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, params *TranslateConnectv1ConnectorPluginParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// TranslateConnectv1ConnectorPlugin Translate Self Managed Connector Plugin Configurations to Fully Managed Connector Plugin Configurations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Translate the provided Self Managed configuration values. This API performs configuration translation
	// and returns the translated fully managed configuration along with any errors or warnings.
	// Query Parameter `mask_sensitive=true` redacts sensitive config values in response.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/translate?mask_sensitive=true (the `TranslateConnectv1ConnectorPlugin` operationId).
	TranslateConnectv1ConnectorPlugin(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, params *TranslateConnectv1ConnectorPluginParams, body TranslateConnectv1ConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ValidateConnectv1ConnectorPluginWithBody Validate a Managed Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate the provided configuration values against the configuration definition. This API performs per config validation and returns suggested values and validation error messages.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/validate (the `ValidateConnectv1ConnectorPlugin` operationId).
	ValidateConnectv1ConnectorPluginWithBody(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ValidateConnectv1ConnectorPlugin Validate a Managed Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate the provided configuration values against the configuration definition. This API performs per config validation and returns suggested values and validation error messages.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/validate (the `ValidateConnectv1ConnectorPlugin` operationId).
	ValidateConnectv1ConnectorPlugin(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, body ValidateConnectv1ConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListConnectv1Connectors List of Connectors
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a list of "names" of the active connectors. You can then make a [read request](#operation/readConnectv1Connector) for a specific connector by name.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `ListConnectv1Connectors` operationId).
	ListConnectv1Connectors(ctx context.Context, environmentId string, kafkaClusterId string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateConnectv1ConnectorWithBody Create a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new connector. Returns the new connector information if successful.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `CreateConnectv1Connector` operationId).
	CreateConnectv1ConnectorWithBody(ctx context.Context, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateConnectv1Connector Create a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new connector. Returns the new connector information if successful.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `CreateConnectv1Connector` operationId).
	CreateConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, body CreateConnectv1ConnectorJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteConnectv1Connector Delete a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a connector. Halts all tasks and deletes the connector configuration.
	//
	// Corresponds with DELETE /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name} (the `DeleteConnectv1Connector` operationId).
	DeleteConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ReadConnectv1Connector Read a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get information about the connector.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name} (the `ReadConnectv1Connector` operationId).
	ReadConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetConnectv1ConnectorConfig Read a Connector Configuration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the configuration for the connector.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `GetConnectv1ConnectorConfig` operationId).
	GetConnectv1ConnectorConfig(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrUpdateConnectv1ConnectorConfigWithBody Create or Update a Connector Configuration
	//
	// Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `CreateOrUpdateConnectv1ConnectorConfig` operationId).
	CreateOrUpdateConnectv1ConnectorConfigWithBody(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateOrUpdateConnectv1ConnectorConfig Create or Update a Connector Configuration
	//
	// Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `CreateOrUpdateConnectv1ConnectorConfig` operationId).
	CreateOrUpdateConnectv1ConnectorConfig(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, body CreateOrUpdateConnectv1ConnectorConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetConnectv1ConnectorOffsets Get a Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the current offsets for the connector. The offsets provide information on the point in the source system,
	// from which the connector is pulling in data. The offsets of a connector are continuously observed periodically and are queryable via this API.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets (the `GetConnectv1ConnectorOffsets` operationId).
	GetConnectv1ConnectorOffsets(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// AlterConnectv1ConnectorOffsetsRequestWithBody Request to Alter the Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request to alter the offsets of a connector. This supports the ability to PATCH/DELETE the offsets of a connector.
	// Note, you will see momentary downtime as this will internally stop the connector, while the offsets are being altered.
	// You can only make one alter offsets request at a time for a connector.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request (the `AlterConnectv1ConnectorOffsetsRequest` operationId).
	AlterConnectv1ConnectorOffsetsRequestWithBody(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// AlterConnectv1ConnectorOffsetsRequest Request to Alter the Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request to alter the offsets of a connector. This supports the ability to PATCH/DELETE the offsets of a connector.
	// Note, you will see momentary downtime as this will internally stop the connector, while the offsets are being altered.
	// You can only make one alter offsets request at a time for a connector.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request (the `AlterConnectv1ConnectorOffsetsRequest` operationId).
	AlterConnectv1ConnectorOffsetsRequest(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, body AlterConnectv1ConnectorOffsetsRequestJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetConnectv1ConnectorOffsetsRequestStatus Get the Status of Alter Offset Request
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the status of the previous alter offset request.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request/status (the `GetConnectv1ConnectorOffsetsRequestStatus` operationId).
	GetConnectv1ConnectorOffsetsRequestStatus(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PauseConnectv1Connector Pause a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Pause the connector and its tasks. Stops message processing until the connector is resumed. This call is asynchronous and the tasks will not transition to PAUSED state at the same time.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/pause (the `PauseConnectv1Connector` operationId).
	PauseConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RestartConnectv1Connector Restart a Connector
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	// Restart the connector and its tasks. Stops message processing until the connector and tasks are restart. This call is asynchronous and the connector will not transition to another state at the same time.
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/restart (the `RestartConnectv1Connector` operationId).
	RestartConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ResumeConnectv1Connector Resume a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Resume a paused connector or do nothing if the connector is not paused. This call is asynchronous and the tasks will not transition to RUNNING state at the same time.
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/resume (the `ResumeConnectv1Connector` operationId).
	ResumeConnectv1Connector(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ReadConnectv1ConnectorStatus Read a Connector Status
	//
	// Get current status of the connector. This includes whether it is running, failed, or paused. Also includes which worker it is assigned to, error information if it has failed, and the state of all its tasks.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/status (the `ReadConnectv1ConnectorStatus` operationId).
	ReadConnectv1ConnectorStatus(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListConnectv1ConnectorTasks List of Connector Tasks
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get a list of tasks currently running for the connector.
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/tasks (the `ListConnectv1ConnectorTasks` operationId).
	ListConnectv1ConnectorTasks(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListConnectv1ConnectorsWithExpansions List of Connectors with Expansions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve an object with the queried expansions of all connectors. Without `expand` query parameter, this list connector’s endpoint will return a [list of only the connector names](#operation/listConnectv1Connectors).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors?expand=info,status,id (the `ListConnectv1ConnectorsWithExpansions` operationId).
	ListConnectv1ConnectorsWithExpansions(ctx context.Context, environmentId string, kafkaClusterId string, params *ListConnectv1ConnectorsWithExpansionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlConnectV1PresignedUrlWithBody Request a presigned upload URL for a new Custom Connector Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connector Plugin archive.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /connect/v1/presigned-upload-url (the `PresignedUploadUrlConnectV1PresignedUrl` operationId).
	PresignedUploadUrlConnectV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlConnectV1PresignedUrl Request a presigned upload URL for a new Custom Connector Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connector Plugin archive.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /connect/v1/presigned-upload-url (the `PresignedUploadUrlConnectV1PresignedUrl` operationId).
	PresignedUploadUrlConnectV1PresignedUrl(ctx context.Context, body PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *oasClient) PresignedUploadUrlConnectV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlConnectV1PresignedUrlRequestWithBody(c.Server, contentType, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *oasClient) PresignedUploadUrlConnectV1PresignedUrl(ctx context.Context, body PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlConnectV1PresignedUrlRequest(c.Server, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewPresignedUploadUrlConnectV1PresignedUrlRequest(server string, body PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPresignedUploadUrlConnectV1PresignedUrlRequestWithBody(server, "application/json", bodyReader)
}
func NewPresignedUploadUrlConnectV1PresignedUrlRequestWithBody(server string, contentType string, body io.Reader) (*http.Request, error) {
	var err error

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/connect/v1/presigned-upload-url")
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

	// ListConnectV1CustomConnectorPluginsWithResponse List of Custom Connector Plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connector plugins.
	//
	// If no `cloud` filter is specified, returns custom connector plugins from all clouds.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/custom-connector-plugins (the `ListConnectV1CustomConnectorPlugins` operationId).
	ListConnectV1CustomConnectorPluginsWithResponse(ctx context.Context, params *ListConnectV1CustomConnectorPluginsParams, reqEditors ...RequestEditorFn) (*ListConnectV1CustomConnectorPluginsResponse, error)

	// CreateConnectV1CustomConnectorPluginWithBodyWithResponse Create a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connector plugin.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/custom-connector-plugins (the `CreateConnectV1CustomConnectorPlugin` operationId).
	CreateConnectV1CustomConnectorPluginWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateConnectV1CustomConnectorPluginResponse, error)

	// CreateConnectV1CustomConnectorPluginWithResponse Create a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a custom connector plugin.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/custom-connector-plugins (the `CreateConnectV1CustomConnectorPlugin` operationId).
	CreateConnectV1CustomConnectorPluginWithResponse(ctx context.Context, body CreateConnectV1CustomConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateConnectV1CustomConnectorPluginResponse, error)

	// DeleteConnectV1CustomConnectorPluginWithResponse Delete a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a custom connector plugin.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /connect/v1/custom-connector-plugins/{id} (the `DeleteConnectV1CustomConnectorPlugin` operationId).
	DeleteConnectV1CustomConnectorPluginWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteConnectV1CustomConnectorPluginResponse, error)

	// GetConnectV1CustomConnectorPluginWithResponse Read a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a custom connector plugin.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/custom-connector-plugins/{id} (the `GetConnectV1CustomConnectorPlugin` operationId).
	GetConnectV1CustomConnectorPluginWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetConnectV1CustomConnectorPluginResponse, error)

	// UpdateConnectV1CustomConnectorPluginWithBodyWithResponse Update a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connector plugin.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /connect/v1/custom-connector-plugins/{id} (the `UpdateConnectV1CustomConnectorPlugin` operationId).
	UpdateConnectV1CustomConnectorPluginWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateConnectV1CustomConnectorPluginResponse, error)

	// UpdateConnectV1CustomConnectorPluginWithResponse Update a Custom Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a custom connector plugin.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /connect/v1/custom-connector-plugins/{id} (the `UpdateConnectV1CustomConnectorPlugin` operationId).
	UpdateConnectV1CustomConnectorPluginWithResponse(ctx context.Context, id string, body UpdateConnectV1CustomConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateConnectV1CustomConnectorPluginResponse, error)

	// ListConnectV1CustomConnectorRuntimesWithResponse List of Custom Connector Runtimes
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all custom connector runtimes.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/custom-connector-runtimes (the `ListConnectV1CustomConnectorRuntimes` operationId).
	ListConnectV1CustomConnectorRuntimesWithResponse(ctx context.Context, params *ListConnectV1CustomConnectorRuntimesParams, reqEditors ...RequestEditorFn) (*ListConnectV1CustomConnectorRuntimesResponse, error)

	// ListConnectv1ConnectorPluginsWithResponse List of Managed Connector plugins
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of Managed Connector plugins installed in the Kafka Connect cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins (the `ListConnectv1ConnectorPlugins` operationId).
	ListConnectv1ConnectorPluginsWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, reqEditors ...RequestEditorFn) (*ListConnectv1ConnectorPluginsResponse, error)

	// TranslateConnectv1ConnectorPluginWithBodyWithResponse Translate Self Managed Connector Plugin Configurations to Fully Managed Connector Plugin Configurations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Translate the provided Self Managed configuration values. This API performs configuration translation
	// and returns the translated fully managed configuration along with any errors or warnings.
	// Query Parameter `mask_sensitive=true` redacts sensitive config values in response.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/translate?mask_sensitive=true (the `TranslateConnectv1ConnectorPlugin` operationId).
	TranslateConnectv1ConnectorPluginWithBodyWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, params *TranslateConnectv1ConnectorPluginParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*TranslateConnectv1ConnectorPluginResponse, error)

	// TranslateConnectv1ConnectorPluginWithResponse Translate Self Managed Connector Plugin Configurations to Fully Managed Connector Plugin Configurations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Translate the provided Self Managed configuration values. This API performs configuration translation
	// and returns the translated fully managed configuration along with any errors or warnings.
	// Query Parameter `mask_sensitive=true` redacts sensitive config values in response.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/translate?mask_sensitive=true (the `TranslateConnectv1ConnectorPlugin` operationId).
	TranslateConnectv1ConnectorPluginWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, params *TranslateConnectv1ConnectorPluginParams, body TranslateConnectv1ConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*TranslateConnectv1ConnectorPluginResponse, error)

	// ValidateConnectv1ConnectorPluginWithBodyWithResponse Validate a Managed Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate the provided configuration values against the configuration definition. This API performs per config validation and returns suggested values and validation error messages.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/validate (the `ValidateConnectv1ConnectorPlugin` operationId).
	ValidateConnectv1ConnectorPluginWithBodyWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*ValidateConnectv1ConnectorPluginResponse, error)

	// ValidateConnectv1ConnectorPluginWithResponse Validate a Managed Connector Plugin
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Validate the provided configuration values against the configuration definition. This API performs per config validation and returns suggested values and validation error messages.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/validate (the `ValidateConnectv1ConnectorPlugin` operationId).
	ValidateConnectv1ConnectorPluginWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, pluginName string, body ValidateConnectv1ConnectorPluginJSONRequestBody, reqEditors ...RequestEditorFn) (*ValidateConnectv1ConnectorPluginResponse, error)

	// ListConnectv1ConnectorsWithResponse List of Connectors
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a list of "names" of the active connectors. You can then make a [read request](#operation/readConnectv1Connector) for a specific connector by name.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `ListConnectv1Connectors` operationId).
	ListConnectv1ConnectorsWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, reqEditors ...RequestEditorFn) (*ListConnectv1ConnectorsResponse, error)

	// CreateConnectv1ConnectorWithBodyWithResponse Create a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new connector. Returns the new connector information if successful.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `CreateConnectv1Connector` operationId).
	CreateConnectv1ConnectorWithBodyWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateConnectv1ConnectorResponse, error)

	// CreateConnectv1ConnectorWithResponse Create a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a new connector. Returns the new connector information if successful.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors (the `CreateConnectv1Connector` operationId).
	CreateConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, body CreateConnectv1ConnectorJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateConnectv1ConnectorResponse, error)

	// DeleteConnectv1ConnectorWithResponse Delete a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete a connector. Halts all tasks and deletes the connector configuration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name} (the `DeleteConnectv1Connector` operationId).
	DeleteConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*DeleteConnectv1ConnectorResponse, error)

	// ReadConnectv1ConnectorWithResponse Read a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get information about the connector.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name} (the `ReadConnectv1Connector` operationId).
	ReadConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*ReadConnectv1ConnectorResponse, error)

	// GetConnectv1ConnectorConfigWithResponse Read a Connector Configuration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the configuration for the connector.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `GetConnectv1ConnectorConfig` operationId).
	GetConnectv1ConnectorConfigWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*GetConnectv1ConnectorConfigResponse, error)

	// CreateOrUpdateConnectv1ConnectorConfigWithBodyWithResponse Create or Update a Connector Configuration
	//
	// Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `CreateOrUpdateConnectv1ConnectorConfig` operationId).
	CreateOrUpdateConnectv1ConnectorConfigWithBodyWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateOrUpdateConnectv1ConnectorConfigResponse, error)

	// CreateOrUpdateConnectv1ConnectorConfigWithResponse Create or Update a Connector Configuration
	//
	// Create a new connector using the given configuration, or update the configuration for an existing connector. Returns information about the connector after the change has been made.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config (the `CreateOrUpdateConnectv1ConnectorConfig` operationId).
	CreateOrUpdateConnectv1ConnectorConfigWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, body CreateOrUpdateConnectv1ConnectorConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateOrUpdateConnectv1ConnectorConfigResponse, error)

	// GetConnectv1ConnectorOffsetsWithResponse Get a Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the current offsets for the connector. The offsets provide information on the point in the source system,
	// from which the connector is pulling in data. The offsets of a connector are continuously observed periodically and are queryable via this API.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets (the `GetConnectv1ConnectorOffsets` operationId).
	GetConnectv1ConnectorOffsetsWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*GetConnectv1ConnectorOffsetsResponse, error)

	// AlterConnectv1ConnectorOffsetsRequestWithBodyWithResponse Request to Alter the Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request to alter the offsets of a connector. This supports the ability to PATCH/DELETE the offsets of a connector.
	// Note, you will see momentary downtime as this will internally stop the connector, while the offsets are being altered.
	// You can only make one alter offsets request at a time for a connector.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request (the `AlterConnectv1ConnectorOffsetsRequest` operationId).
	AlterConnectv1ConnectorOffsetsRequestWithBodyWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*AlterConnectv1ConnectorOffsetsRequestResponse, error)

	// AlterConnectv1ConnectorOffsetsRequestWithResponse Request to Alter the Connector Offsets
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request to alter the offsets of a connector. This supports the ability to PATCH/DELETE the offsets of a connector.
	// Note, you will see momentary downtime as this will internally stop the connector, while the offsets are being altered.
	// You can only make one alter offsets request at a time for a connector.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request (the `AlterConnectv1ConnectorOffsetsRequest` operationId).
	AlterConnectv1ConnectorOffsetsRequestWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, body AlterConnectv1ConnectorOffsetsRequestJSONRequestBody, reqEditors ...RequestEditorFn) (*AlterConnectv1ConnectorOffsetsRequestResponse, error)

	// GetConnectv1ConnectorOffsetsRequestStatusWithResponse Get the Status of Alter Offset Request
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the status of the previous alter offset request.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/offsets/request/status (the `GetConnectv1ConnectorOffsetsRequestStatus` operationId).
	GetConnectv1ConnectorOffsetsRequestStatusWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*GetConnectv1ConnectorOffsetsRequestStatusResponse, error)

	// PauseConnectv1ConnectorWithResponse Pause a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Pause the connector and its tasks. Stops message processing until the connector is resumed. This call is asynchronous and the tasks will not transition to PAUSED state at the same time.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/pause (the `PauseConnectv1Connector` operationId).
	PauseConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*PauseConnectv1ConnectorResponse, error)

	// RestartConnectv1ConnectorWithResponse Restart a Connector
	//
	// [![Preview](https://img.shields.io/badge/Lifecycle%20Stage-Preview-%2300afba)](#section/Versioning/API-Lifecycle-Policy)
	// Restart the connector and its tasks. Stops message processing until the connector and tasks are restart. This call is asynchronous and the connector will not transition to another state at the same time.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/restart (the `RestartConnectv1Connector` operationId).
	RestartConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*RestartConnectv1ConnectorResponse, error)

	// ResumeConnectv1ConnectorWithResponse Resume a Connector
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Resume a paused connector or do nothing if the connector is not paused. This call is asynchronous and the tasks will not transition to RUNNING state at the same time.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/resume (the `ResumeConnectv1Connector` operationId).
	ResumeConnectv1ConnectorWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*ResumeConnectv1ConnectorResponse, error)

	// ReadConnectv1ConnectorStatusWithResponse Read a Connector Status
	//
	// Get current status of the connector. This includes whether it is running, failed, or paused. Also includes which worker it is assigned to, error information if it has failed, and the state of all its tasks.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/status (the `ReadConnectv1ConnectorStatus` operationId).
	ReadConnectv1ConnectorStatusWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*ReadConnectv1ConnectorStatusResponse, error)

	// ListConnectv1ConnectorTasksWithResponse List of Connector Tasks
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get a list of tasks currently running for the connector.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/tasks (the `ListConnectv1ConnectorTasks` operationId).
	ListConnectv1ConnectorTasksWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, connectorName string, reqEditors ...RequestEditorFn) (*ListConnectv1ConnectorTasksResponse, error)

	// ListConnectv1ConnectorsWithExpansionsWithResponse List of Connectors with Expansions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve an object with the queried expansions of all connectors. Without `expand` query parameter, this list connector’s endpoint will return a [list of only the connector names](#operation/listConnectv1Connectors).
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors?expand=info,status,id (the `ListConnectv1ConnectorsWithExpansions` operationId).
	ListConnectv1ConnectorsWithExpansionsWithResponse(ctx context.Context, environmentId string, kafkaClusterId string, params *ListConnectv1ConnectorsWithExpansionsParams, reqEditors ...RequestEditorFn) (*ListConnectv1ConnectorsWithExpansionsResponse, error)

	// PresignedUploadUrlConnectV1PresignedUrlWithBodyWithResponse Request a presigned upload URL for a new Custom Connector Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connector Plugin archive.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/presigned-upload-url (the `PresignedUploadUrlConnectV1PresignedUrl` operationId).
	PresignedUploadUrlConnectV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlConnectV1PresignedUrlResponse, error)

	// PresignedUploadUrlConnectV1PresignedUrlWithResponse Request a presigned upload URL for a new Custom Connector Plugin.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Custom Connector Plugin archive.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /connect/v1/presigned-upload-url (the `PresignedUploadUrlConnectV1PresignedUrl` operationId).
	PresignedUploadUrlConnectV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlConnectV1PresignedUrlResponse, error)
}

func (r ListConnectV1CustomConnectorPluginsResponse) GetJSON200() *ConnectV1CustomConnectorPluginList {
	return r.JSON200
}
func (r ListConnectV1CustomConnectorPluginsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListConnectV1CustomConnectorPluginsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectV1CustomConnectorPluginsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListConnectV1CustomConnectorPluginsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListConnectV1CustomConnectorPluginsResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectV1CustomConnectorPluginsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectV1CustomConnectorPluginsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectV1CustomConnectorPluginsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateConnectV1CustomConnectorPlugin201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ConnectorClass Java class or alias for connector. You can get connector class from connector documentation provided by developer.
	ConnectorClass string `json:"connector_class"`

	// ConnectorType Custom Connector type.
	ConnectorType string `json:"connector_type"`

	// ContentFormat Archive format of Custom Connector Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of Custom Connector Plugin.
	Description *string `json:"description,omitempty"`

	// DisplayName Display name of Custom Connector Plugin.
	DisplayName string `json:"display_name"`

	// DocumentationLink Document link of Custom Connector Plugin.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateConnectV1CustomConnectorPlugin201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// RuntimeLanguage Runtime language of Custom Connector Plugin.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
	// value when setting up connector.
	SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

	// UploadSource Upload source of Custom Connector Plugin. Only required in `create` request, will be ignored in `read`, `update` or `list`.
	UploadSource CreateConnectV1CustomConnectorPlugin201JSONResponseBody_UploadSource `json:"upload_source"`
} {
	return r.JSON201
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateConnectV1CustomConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r CreateConnectV1CustomConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateConnectV1CustomConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateConnectV1CustomConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteConnectV1CustomConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteConnectV1CustomConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteConnectV1CustomConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteConnectV1CustomConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ConnectorClass Java class or alias for connector. You can get connector class from connector documentation provided by developer.
	ConnectorClass string `json:"connector_class"`

	// ConnectorType Custom Connector type.
	ConnectorType string `json:"connector_type"`

	// ContentFormat Archive format of Custom Connector Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of Custom Connector Plugin.
	Description *string `json:"description,omitempty"`

	// DisplayName Display name of Custom Connector Plugin.
	DisplayName string `json:"display_name"`

	// DocumentationLink Document link of Custom Connector Plugin.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetConnectV1CustomConnectorPlugin200JSONResponseBodyKind `json:"kind"`
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

	// RuntimeLanguage Runtime language of Custom Connector Plugin.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
	// value when setting up connector.
	SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

	// UploadSource Upload source of Custom Connector Plugin. Only required in `create` request, will be ignored in `read`, `update` or `list`.
	UploadSource GetConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource `json:"upload_source"`
} {
	return r.JSON200
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetConnectV1CustomConnectorPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetConnectV1CustomConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r GetConnectV1CustomConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetConnectV1CustomConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetConnectV1CustomConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ConnectorClass Java class or alias for connector. You can get connector class from connector documentation provided by developer.
	ConnectorClass string `json:"connector_class"`

	// ConnectorType Custom Connector type.
	ConnectorType string `json:"connector_type"`

	// ContentFormat Archive format of Custom Connector Plugin.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of Custom Connector Plugin.
	Description *string `json:"description,omitempty"`

	// DisplayName Display name of Custom Connector Plugin.
	DisplayName string `json:"display_name"`

	// DocumentationLink Document link of Custom Connector Plugin.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateConnectV1CustomConnectorPlugin200JSONResponseBodyKind `json:"kind"`
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

	// RuntimeLanguage Runtime language of Custom Connector Plugin.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// SensitiveConfigProperties A sensitive property is a connector configuration property that must be hidden after a user enters property
	// value when setting up connector.
	SensitiveConfigProperties *[]string `json:"sensitive_config_properties,omitempty"`

	// UploadSource Upload source of Custom Connector Plugin. Only required in `create` request, will be ignored in `read`, `update` or `list`.
	UploadSource UpdateConnectV1CustomConnectorPlugin200JSONResponseBody_UploadSource `json:"upload_source"`
} {
	return r.JSON200
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateConnectV1CustomConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateConnectV1CustomConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateConnectV1CustomConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateConnectV1CustomConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetJSON200() *ConnectV1CustomConnectorRuntimeList {
	return r.JSON200
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListConnectV1CustomConnectorRuntimesResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectV1CustomConnectorRuntimesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectV1CustomConnectorRuntimesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectV1CustomConnectorRuntimesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListConnectv1ConnectorPluginsResponse) GetJSON200() *[]struct {
	// Class The connector class name. E.g. BigQuerySink.
	Class string `json:"class"`

	// Type Type of connector, sink or source.
	Type ListConnectv1ConnectorPlugins200JSONResponseBodyType `json:"type"`

	// Version The version string for the connector available.
	Version *string `json:"version,omitempty"`
} {
	return r.JSON200
}
func (r ListConnectv1ConnectorPluginsResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectv1ConnectorPluginsResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r ListConnectv1ConnectorPluginsResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ListConnectv1ConnectorPluginsResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectv1ConnectorPluginsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectv1ConnectorPluginsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectv1ConnectorPluginsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON200() *struct {
	// Config The translated configuration
	Config *map[string]string `json:"config,omitempty"`

	// Errors List of configuration errors
	Errors *[]struct {
		// Field The field name that has an error
		Field string `json:"field"`

		// Message The error message
		Message string `json:"message"`
	} `json:"errors,omitempty"`

	// Warnings List of configuration warnings
	Warnings *[]struct {
		// Field The field name that has a warning
		Field string `json:"field"`

		// Message The warning message
		Message string `json:"message"`
	} `json:"warnings,omitempty"`
} {
	return r.JSON200
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON400() *ConnectV1BadRequestError {
	return r.JSON400
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON403() *ConnectV1ForbiddenError {
	return r.JSON403
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r TranslateConnectv1ConnectorPluginResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r TranslateConnectv1ConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r TranslateConnectv1ConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r TranslateConnectv1ConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r TranslateConnectv1ConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ValidateConnectv1ConnectorPluginResponse) GetJSON200() *struct {
	Configs *[]struct {
		// Definition The definition for a config in the connector plugin, which includes the name, type, importance, etc.
		Definition *struct {
			Alias *string `json:"alias,omitempty"`

			// DefaultValue Default value for this configuration
			DefaultValue *string `json:"default_value,omitempty"`

			// Dependents Other configurations on which this configuration is dependent
			Dependents  *[]string `json:"dependents,omitempty"`
			DisplayName *string   `json:"display_name,omitempty"`

			// Documentation The documentation for the configuration
			Documentation *string `json:"documentation,omitempty"`

			// Group The UI group to which the configuration belongs to
			Group *string `json:"group,omitempty"`

			// Importance The importance level for a configuration
			Importance *ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionImportance `json:"importance,omitempty"`

			// Name The name of the configuration
			Name *string `json:"name,omitempty"`

			// Order The order of configuration in specified group
			Order *int `json:"order,omitempty"`

			// Required Whether this configuration is required
			Required *bool `json:"required,omitempty"`

			// Type The config types
			Type *ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionType `json:"type,omitempty"`

			// Width The width of a configuration value
			Width *ValidateConnectv1ConnectorPlugin200JSONResponseBodyConfigsDefinitionWidth `json:"width,omitempty"`
		} `json:"definition,omitempty"`

		// Metadata Map of metadata details about the connector configuration, such as type of
		// input, etc.
		Metadata *map[string]interface{} `json:"metadata,omitempty"`

		// Value The current value for a config, which includes the name, value, recommended values, etc.
		Value *struct {
			// Errors Errors, if any, in the configuration value
			Errors *[]string `json:"errors,omitempty"`

			// Name The name of the configuration
			Name *string `json:"name,omitempty"`

			// RecommendedValues The list of valid values for the configuration
			RecommendedValues *[]string `json:"recommended_values,omitempty"`

			// Value The value for the configuration
			Value *string `json:"value,omitempty"`

			// Visible The visibility of the configuration. Based on the values of other configuration
			// fields, this visibility boolean value points out if the current field should be
			// visible or not.
			Visible *bool `json:"visible,omitempty"`
		} `json:"value,omitempty"`
	} `json:"configs,omitempty"`

	// ErrorCount The total number of errors encountered during configuration validation.
	ErrorCount *int `json:"error_count,omitempty"`

	// Groups The list of groups used in configuration definitions.
	Groups *[]string `json:"groups,omitempty"`

	// Name The class name of the connector plugin.
	Name *string `json:"name,omitempty"`
} {
	return r.JSON200
}
func (r ValidateConnectv1ConnectorPluginResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ValidateConnectv1ConnectorPluginResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r ValidateConnectv1ConnectorPluginResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ValidateConnectv1ConnectorPluginResponse) GetBody() []byte {
	return r.Body
}
func (r ValidateConnectv1ConnectorPluginResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ValidateConnectv1ConnectorPluginResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ValidateConnectv1ConnectorPluginResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListConnectv1ConnectorsResponse) GetJSON200() *[]string {
	return r.JSON200
}
func (r ListConnectv1ConnectorsResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectv1ConnectorsResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r ListConnectv1ConnectorsResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ListConnectv1ConnectorsResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectv1ConnectorsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectv1ConnectorsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectv1ConnectorsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateConnectv1ConnectorResponse) GetJSON201() *ConnectV1ConnectorWithOffsets {
	return r.JSON201
}
func (r CreateConnectv1ConnectorResponse) GetJSON400() *struct {
	Code    *int    `json:"code,omitempty"`
	Message *string `json:"message,omitempty"`
} {
	return r.JSON400
}
func (r CreateConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r CreateConnectv1ConnectorResponse) GetJSON500() *struct {
	ErrorCode *int    `json:"error_code,omitempty"`
	Message   *string `json:"message,omitempty"`
} {
	return r.JSON500
}
func (r CreateConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r CreateConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteConnectv1ConnectorResponse) GetJSON200() *ConnectV1OK {
	return r.JSON200
}
func (r DeleteConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r DeleteConnectv1ConnectorResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r DeleteConnectv1ConnectorResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r DeleteConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ReadConnectv1ConnectorResponse) GetJSON200() *ConnectV1Connector {
	return r.JSON200
}
func (r ReadConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ReadConnectv1ConnectorResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r ReadConnectv1ConnectorResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ReadConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r ReadConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ReadConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ReadConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetConnectv1ConnectorConfigResponse) GetJSON200() *GetConnectv1ConnectorConfig200JSONResponseBody {
	return r.JSON200
}
func (r GetConnectv1ConnectorConfigResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r GetConnectv1ConnectorConfigResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r GetConnectv1ConnectorConfigResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r GetConnectv1ConnectorConfigResponse) GetBody() []byte {
	return r.Body
}
func (r GetConnectv1ConnectorConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetConnectv1ConnectorConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetConnectv1ConnectorConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetJSON200() *ConnectV1Connector {
	return r.JSON200
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetJSON400() *ConnectV1BadRequestError {
	return r.JSON400
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetJSON500() *struct {
	ErrorCode *int    `json:"error_code,omitempty"`
	Message   *string `json:"message,omitempty"`
} {
	return r.JSON500
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) GetBody() []byte {
	return r.Body
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateOrUpdateConnectv1ConnectorConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON200() *ConnectV1ConnectorOffsets {
	return r.JSON200
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON400() *ConnectV1BadRequestError {
	return r.JSON400
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON403() *ConnectV1ForbiddenError {
	return r.JSON403
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r GetConnectv1ConnectorOffsetsResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r GetConnectv1ConnectorOffsetsResponse) GetBody() []byte {
	return r.Body
}
func (r GetConnectv1ConnectorOffsetsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetConnectv1ConnectorOffsetsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetConnectv1ConnectorOffsetsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON202() *ConnectV1AlterOffsetRequestInfo {
	return r.JSON202
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON400() *ConnectV1BadRequestError {
	return r.JSON400
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON403() *ConnectV1ForbiddenError {
	return r.JSON403
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) GetBody() []byte {
	return r.Body
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r AlterConnectv1ConnectorOffsetsRequestResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON200() *ConnectV1AlterOffsetStatus {
	return r.JSON200
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON400() *ConnectV1BadRequestError {
	return r.JSON400
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON403() *ConnectV1ForbiddenError {
	return r.JSON403
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) GetBody() []byte {
	return r.Body
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetConnectv1ConnectorOffsetsRequestStatusResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r PauseConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r PauseConnectv1ConnectorResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r PauseConnectv1ConnectorResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r PauseConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r PauseConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PauseConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PauseConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r RestartConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r RestartConnectv1ConnectorResponse) GetJSON403() *ConnectV1ForbiddenError {
	return r.JSON403
}
func (r RestartConnectv1ConnectorResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r RestartConnectv1ConnectorResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r RestartConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r RestartConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r RestartConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r RestartConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ResumeConnectv1ConnectorResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ResumeConnectv1ConnectorResponse) GetJSON404() *ConnectV1ResourceNotFoundError {
	return r.JSON404
}
func (r ResumeConnectv1ConnectorResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ResumeConnectv1ConnectorResponse) GetBody() []byte {
	return r.Body
}
func (r ResumeConnectv1ConnectorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ResumeConnectv1ConnectorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ResumeConnectv1ConnectorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ReadConnectv1ConnectorStatusResponse) GetJSON200() *struct {
	// Connector The map containing connector status.
	Connector struct {
		// State The state of the connector.
		State ReadConnectv1ConnectorStatus200JSONResponseBodyConnectorState `json:"state"`

		// Trace The exception name in case of error.
		Trace *string `json:"trace,omitempty"`

		// WorkerId The worker ID of the connector.
		WorkerId string `json:"worker_id"`
	} `json:"connector"`

	// Name The name of the connector.
	Name string `json:"name"`

	// Tasks The map containing the task status.
	Tasks *[]struct {
		// Id The ID of task.
		Id  int     `json:"id"`
		Msg *string `json:"msg,omitempty"`

		// State The state of the task.
		State string `json:"state"`

		// WorkerId The worker ID of the task.
		WorkerId string `json:"worker_id"`
	} `json:"tasks,omitempty"`

	// Type Type of connector, sink or source.
	Type ReadConnectv1ConnectorStatus200JSONResponseBodyType `json:"type"`
} {
	return r.JSON200
}
func (r ReadConnectv1ConnectorStatusResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ReadConnectv1ConnectorStatusResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r ReadConnectv1ConnectorStatusResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ReadConnectv1ConnectorStatusResponse) GetBody() []byte {
	return r.Body
}
func (r ReadConnectv1ConnectorStatusResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ReadConnectv1ConnectorStatusResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ReadConnectv1ConnectorStatusResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListConnectv1ConnectorTasksResponse) GetJSON200() *ConnectV1Connectors {
	return r.JSON200
}
func (r ListConnectv1ConnectorTasksResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectv1ConnectorTasksResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r ListConnectv1ConnectorTasksResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ListConnectv1ConnectorTasksResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectv1ConnectorTasksResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectv1ConnectorTasksResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectv1ConnectorTasksResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) GetJSON200() *ConnectV1ConnectorExpansionMap {
	return r.JSON200
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) GetJSON401() *ConnectV1UnauthenticatedError {
	return r.JSON401
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) GetJSON404() *ConnectV1AccountNotFoundError {
	return r.JSON404
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) GetJSON500() *ConnectV1DefaultSystemError {
	return r.JSON500
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListConnectv1ConnectorsWithExpansionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type PresignedUploadUrlConnectV1PresignedUrlResponse200Headers struct {
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse400Headers struct {
	XRequestId *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse403Headers struct {
	XRequestId *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse404Headers struct {
	XRequestId *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse500Headers struct {
	XRequestId *string
}
type PresignedUploadUrlConnectV1PresignedUrlResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON200 the response for an HTTP 200 `application/json` response
	JSON200 *struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

		// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
		Cloud *string `json:"cloud,omitempty"`

		// ContentFormat Content format of the Custom Connector Plugin archive.
		ContentFormat *string `json:"content_format,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

		// UploadFormData Upload form data of the Custom Connector Plugin. All values should be strings.
		UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

		// UploadId Unique identifier of this upload.
		UploadId *string `json:"upload_id,omitempty"`

		// UploadUrl Upload URL for the Custom Connector Plugin archive.
		UploadUrl *string `json:"upload_url,omitempty"`
	}
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
	// Headers200 the parsed response headers for an HTTP 200 response
	Headers200 *PresignedUploadUrlConnectV1PresignedUrlResponse200Headers
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *PresignedUploadUrlConnectV1PresignedUrlResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *PresignedUploadUrlConnectV1PresignedUrlResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *PresignedUploadUrlConnectV1PresignedUrlResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *PresignedUploadUrlConnectV1PresignedUrlResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *PresignedUploadUrlConnectV1PresignedUrlResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *PresignedUploadUrlConnectV1PresignedUrlResponse500Headers
}

func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Custom Connector Plugin archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

	// UploadFormData Upload form data of the Custom Connector Plugin. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Custom Connector Plugin archive.
	UploadUrl *string `json:"upload_url,omitempty"`
} {
	return r.JSON200
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) GetBody() []byte {
	return r.Body
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PresignedUploadUrlConnectV1PresignedUrlResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) PresignedUploadUrlConnectV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlConnectV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlConnectV1PresignedUrlWithBody(ctx, contentType, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlConnectV1PresignedUrlResponse(rsp)
}
func (c *ClientWithResponses) PresignedUploadUrlConnectV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlConnectV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlConnectV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlConnectV1PresignedUrl(ctx, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlConnectV1PresignedUrlResponse(rsp)
}
func ParsePresignedUploadUrlConnectV1PresignedUrlResponse(rsp *http.Response) (*PresignedUploadUrlConnectV1PresignedUrlResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &PresignedUploadUrlConnectV1PresignedUrlResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 200:
		var dest struct {
			// ApiVersion APIVersion defines the schema version of this representation of a resource.
			ApiVersion PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

			// Cloud Cloud provider where the Custom Connector Plugin archive is uploaded.
			Cloud *string `json:"cloud,omitempty"`

			// ContentFormat Content format of the Custom Connector Plugin archive.
			ContentFormat *string `json:"content_format,omitempty"`

			// Kind Kind defines the object this REST resource represents.
			Kind PresignedUploadUrlConnectV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

			// UploadFormData Upload form data of the Custom Connector Plugin. All values should be strings.
			UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

			// UploadId Unique identifier of this upload.
			UploadId *string `json:"upload_id,omitempty"`

			// UploadUrl Upload URL for the Custom Connector Plugin archive.
			UploadUrl *string `json:"upload_url,omitempty"`
		}
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON200 = &dest

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
	case rsp.StatusCode == 200:
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse200Headers
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
		response.Headers200 = &headers
	case rsp.StatusCode == 400:
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse401Headers
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
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse429Headers
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
		var headers PresignedUploadUrlConnectV1PresignedUrlResponse500Headers
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
