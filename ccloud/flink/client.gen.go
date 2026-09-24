package flink

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
	openapi_types "github.com/oapi-codegen/runtime/types"
)

const (
	ADD     JsonPatchRequestAddReplaceOp = "ADD"
	REPLACE JsonPatchRequestAddReplaceOp = "REPLACE"
	TEST    JsonPatchRequestAddReplaceOp = "TEST"
)

func (e JsonPatchRequestAddReplaceOp) Valid() bool {
	switch e {
	case ADD:
		return true
	case REPLACE:
		return true
	case TEST:
		return true
	default:
		return false
	}
}

const (
	COPY JsonPatchRequestMoveCopyOp = "COPY"
	MOVE JsonPatchRequestMoveCopyOp = "MOVE"
)

func (e JsonPatchRequestMoveCopyOp) Valid() bool {
	switch e {
	case COPY:
		return true
	case MOVE:
		return true
	default:
		return false
	}
}

const (
	REMOVE JsonPatchRequestRemoveOp = "REMOVE"
)

func (e JsonPatchRequestRemoveOp) Valid() bool {
	switch e {
	case REMOVE:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1FlinkArtifactApiVersionArtifactv1 ArtifactV1FlinkArtifactApiVersion = "artifact/v1"
)

func (e ArtifactV1FlinkArtifactApiVersion) Valid() bool {
	switch e {
	case ArtifactV1FlinkArtifactApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1FlinkArtifactKindFlinkArtifact ArtifactV1FlinkArtifactKind = "FlinkArtifact"
)

func (e ArtifactV1FlinkArtifactKind) Valid() bool {
	switch e {
	case ArtifactV1FlinkArtifactKindFlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1FlinkArtifactListApiVersionArtifactv1 ArtifactV1FlinkArtifactListApiVersion = "artifact/v1"
)

func (e ArtifactV1FlinkArtifactListApiVersion) Valid() bool {
	switch e {
	case ArtifactV1FlinkArtifactListApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1FlinkArtifactListDataApiVersionArtifactv1 ArtifactV1FlinkArtifactListDataApiVersion = "artifact/v1"
)

func (e ArtifactV1FlinkArtifactListDataApiVersion) Valid() bool {
	switch e {
	case ArtifactV1FlinkArtifactListDataApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1FlinkArtifactListDataKindFlinkArtifact ArtifactV1FlinkArtifactListDataKind = "FlinkArtifact"
)

func (e ArtifactV1FlinkArtifactListDataKind) Valid() bool {
	switch e {
	case ArtifactV1FlinkArtifactListDataKindFlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	FlinkArtifactList ArtifactV1FlinkArtifactListKind = "FlinkArtifactList"
)

func (e ArtifactV1FlinkArtifactListKind) Valid() bool {
	switch e {
	case FlinkArtifactList:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1PresignedUrlApiVersionArtifactv1 ArtifactV1PresignedUrlApiVersion = "artifact/v1"
)

func (e ArtifactV1PresignedUrlApiVersion) Valid() bool {
	switch e {
	case ArtifactV1PresignedUrlApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1PresignedUrlKindPresignedUrl ArtifactV1PresignedUrlKind = "PresignedUrl"
)

func (e ArtifactV1PresignedUrlKind) Valid() bool {
	switch e {
	case ArtifactV1PresignedUrlKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1PresignedUrlRequestApiVersionArtifactv1 ArtifactV1PresignedUrlRequestApiVersion = "artifact/v1"
)

func (e ArtifactV1PresignedUrlRequestApiVersion) Valid() bool {
	switch e {
	case ArtifactV1PresignedUrlRequestApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1PresignedUrlRequestKindPresignedUrlRequest ArtifactV1PresignedUrlRequestKind = "PresignedUrlRequest"
)

func (e ArtifactV1PresignedUrlRequestKind) Valid() bool {
	switch e {
	case ArtifactV1PresignedUrlRequestKindPresignedUrlRequest:
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
	FcpmV2ComputePoolApiVersionFcpmv2 FcpmV2ComputePoolApiVersion = "fcpm/v2"
)

func (e FcpmV2ComputePoolApiVersion) Valid() bool {
	switch e {
	case FcpmV2ComputePoolApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	FcpmV2ComputePoolKindComputePool FcpmV2ComputePoolKind = "ComputePool"
)

func (e FcpmV2ComputePoolKind) Valid() bool {
	switch e {
	case FcpmV2ComputePoolKindComputePool:
		return true
	default:
		return false
	}
}

const (
	FcpmV2ComputePoolListApiVersionFcpmv2 FcpmV2ComputePoolListApiVersion = "fcpm/v2"
)

func (e FcpmV2ComputePoolListApiVersion) Valid() bool {
	switch e {
	case FcpmV2ComputePoolListApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	FcpmV2ComputePoolListDataApiVersionFcpmv2 FcpmV2ComputePoolListDataApiVersion = "fcpm/v2"
)

func (e FcpmV2ComputePoolListDataApiVersion) Valid() bool {
	switch e {
	case FcpmV2ComputePoolListDataApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	FcpmV2ComputePoolListDataKindComputePool FcpmV2ComputePoolListDataKind = "ComputePool"
)

func (e FcpmV2ComputePoolListDataKind) Valid() bool {
	switch e {
	case FcpmV2ComputePoolListDataKindComputePool:
		return true
	default:
		return false
	}
}

const (
	FcpmV2ComputePoolListKindComputePoolList FcpmV2ComputePoolListKind = "ComputePoolList"
)

func (e FcpmV2ComputePoolListKind) Valid() bool {
	switch e {
	case FcpmV2ComputePoolListKindComputePoolList:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementApiVersionSqlv1 SqlV1StatementApiVersion = "sql/v1"
)

func (e SqlV1StatementApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementKindStatement SqlV1StatementKind = "Statement"
)

func (e SqlV1StatementKind) Valid() bool {
	switch e {
	case SqlV1StatementKindStatement:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementListApiVersionSqlv1 SqlV1StatementListApiVersion = "sql/v1"
)

func (e SqlV1StatementListApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementListApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementListDataApiVersionSqlv1 SqlV1StatementListDataApiVersion = "sql/v1"
)

func (e SqlV1StatementListDataApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementListDataApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementListDataKindStatement SqlV1StatementListDataKind = "Statement"
)

func (e SqlV1StatementListDataKind) Valid() bool {
	switch e {
	case SqlV1StatementListDataKindStatement:
		return true
	default:
		return false
	}
}

const (
	StatementList SqlV1StatementListKind = "StatementList"
)

func (e SqlV1StatementListKind) Valid() bool {
	switch e {
	case StatementList:
		return true
	default:
		return false
	}
}

const (
	SqlV1StatementResultApiVersionSqlv1 SqlV1StatementResultApiVersion = "sql/v1"
)

func (e SqlV1StatementResultApiVersion) Valid() bool {
	switch e {
	case SqlV1StatementResultApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	StatementResult SqlV1StatementResultKind = "StatementResult"
)

func (e SqlV1StatementResultKind) Valid() bool {
	switch e {
	case StatementResult:
		return true
	default:
		return false
	}
}

const (
	CreateArtifactV1FlinkArtifact201JSONResponseBodyApiVersionArtifactv1 CreateArtifactV1FlinkArtifact201JSONResponseBodyApiVersion = "artifact/v1"
)

func (e CreateArtifactV1FlinkArtifact201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateArtifactV1FlinkArtifact201JSONResponseBodyApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	CreateArtifactV1FlinkArtifact201JSONResponseBodyKindFlinkArtifact CreateArtifactV1FlinkArtifact201JSONResponseBodyKind = "FlinkArtifact"
)

func (e CreateArtifactV1FlinkArtifact201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateArtifactV1FlinkArtifact201JSONResponseBodyKindFlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	GetArtifactV1FlinkArtifact200JSONResponseBodyApiVersionArtifactv1 GetArtifactV1FlinkArtifact200JSONResponseBodyApiVersion = "artifact/v1"
)

func (e GetArtifactV1FlinkArtifact200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetArtifactV1FlinkArtifact200JSONResponseBodyApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	GetArtifactV1FlinkArtifact200JSONResponseBodyKindFlinkArtifact GetArtifactV1FlinkArtifact200JSONResponseBodyKind = "FlinkArtifact"
)

func (e GetArtifactV1FlinkArtifact200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetArtifactV1FlinkArtifact200JSONResponseBodyKindFlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	UpdateArtifactV1FlinkArtifact200JSONResponseBodyApiVersionArtifactv1 UpdateArtifactV1FlinkArtifact200JSONResponseBodyApiVersion = "artifact/v1"
)

func (e UpdateArtifactV1FlinkArtifact200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateArtifactV1FlinkArtifact200JSONResponseBodyApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	UpdateArtifactV1FlinkArtifact200JSONResponseBodyKindFlinkArtifact UpdateArtifactV1FlinkArtifact200JSONResponseBodyKind = "FlinkArtifact"
)

func (e UpdateArtifactV1FlinkArtifact200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateArtifactV1FlinkArtifact200JSONResponseBodyKindFlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersionArtifactv1 PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersion = "artifact/v1"
)

func (e PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKindPresignedUrlRequest PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKind = "PresignedUrlRequest"
)

func (e PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKindPresignedUrlRequest:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersionArtifactv1 PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion = "artifact/v1"
)

func (e PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersionArtifactv1:
		return true
	default:
		return false
	}
}

const (
	PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKindPresignedUrl PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind = "PresignedUrl"
)

func (e PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind) Valid() bool {
	switch e {
	case PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKindPresignedUrl:
		return true
	default:
		return false
	}
}

const (
	ListFcpmV2ComputePools200JSONResponseBodyApiVersionFcpmv2 ListFcpmV2ComputePools200JSONResponseBodyApiVersion = "fcpm/v2"
)

func (e ListFcpmV2ComputePools200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListFcpmV2ComputePools200JSONResponseBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	ListFcpmV2ComputePools200JSONResponseBodyKindComputePoolList ListFcpmV2ComputePools200JSONResponseBodyKind = "ComputePoolList"
)

func (e ListFcpmV2ComputePools200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListFcpmV2ComputePools200JSONResponseBodyKindComputePoolList:
		return true
	default:
		return false
	}
}

const (
	CreateFcpmV2ComputePoolJSONBodyApiVersionFcpmv2 CreateFcpmV2ComputePoolJSONBodyApiVersion = "fcpm/v2"
)

func (e CreateFcpmV2ComputePoolJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateFcpmV2ComputePoolJSONBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	CreateFcpmV2ComputePoolJSONBodyKindComputePool CreateFcpmV2ComputePoolJSONBodyKind = "ComputePool"
)

func (e CreateFcpmV2ComputePoolJSONBodyKind) Valid() bool {
	switch e {
	case CreateFcpmV2ComputePoolJSONBodyKindComputePool:
		return true
	default:
		return false
	}
}

const (
	CreateFcpmV2ComputePool202JSONResponseBodyApiVersionFcpmv2 CreateFcpmV2ComputePool202JSONResponseBodyApiVersion = "fcpm/v2"
)

func (e CreateFcpmV2ComputePool202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateFcpmV2ComputePool202JSONResponseBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	CreateFcpmV2ComputePool202JSONResponseBodyKindComputePool CreateFcpmV2ComputePool202JSONResponseBodyKind = "ComputePool"
)

func (e CreateFcpmV2ComputePool202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateFcpmV2ComputePool202JSONResponseBodyKindComputePool:
		return true
	default:
		return false
	}
}

const (
	GetFcpmV2ComputePool200JSONResponseBodyApiVersionFcpmv2 GetFcpmV2ComputePool200JSONResponseBodyApiVersion = "fcpm/v2"
)

func (e GetFcpmV2ComputePool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetFcpmV2ComputePool200JSONResponseBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	GetFcpmV2ComputePool200JSONResponseBodyKindComputePool GetFcpmV2ComputePool200JSONResponseBodyKind = "ComputePool"
)

func (e GetFcpmV2ComputePool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetFcpmV2ComputePool200JSONResponseBodyKindComputePool:
		return true
	default:
		return false
	}
}

const (
	UpdateFcpmV2ComputePoolJSONBodyApiVersionFcpmv2 UpdateFcpmV2ComputePoolJSONBodyApiVersion = "fcpm/v2"
)

func (e UpdateFcpmV2ComputePoolJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateFcpmV2ComputePoolJSONBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	UpdateFcpmV2ComputePoolJSONBodyKindComputePool UpdateFcpmV2ComputePoolJSONBodyKind = "ComputePool"
)

func (e UpdateFcpmV2ComputePoolJSONBodyKind) Valid() bool {
	switch e {
	case UpdateFcpmV2ComputePoolJSONBodyKindComputePool:
		return true
	default:
		return false
	}
}

const (
	UpdateFcpmV2ComputePool200JSONResponseBodyApiVersionFcpmv2 UpdateFcpmV2ComputePool200JSONResponseBodyApiVersion = "fcpm/v2"
)

func (e UpdateFcpmV2ComputePool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateFcpmV2ComputePool200JSONResponseBodyApiVersionFcpmv2:
		return true
	default:
		return false
	}
}

const (
	UpdateFcpmV2ComputePool200JSONResponseBodyKindComputePool UpdateFcpmV2ComputePool200JSONResponseBodyKind = "ComputePool"
)

func (e UpdateFcpmV2ComputePool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateFcpmV2ComputePool200JSONResponseBodyKindComputePool:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1StatementJSONBodyApiVersionSqlv1 CreateSqlv1StatementJSONBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1StatementJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1StatementJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1StatementJSONBodyKindStatement CreateSqlv1StatementJSONBodyKind = "Statement"
)

func (e CreateSqlv1StatementJSONBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1StatementJSONBodyKindStatement:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Statement201JSONResponseBodyApiVersionSqlv1 CreateSqlv1Statement201JSONResponseBodyApiVersion = "sql/v1"
)

func (e CreateSqlv1Statement201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateSqlv1Statement201JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	CreateSqlv1Statement201JSONResponseBodyKindStatement CreateSqlv1Statement201JSONResponseBodyKind = "Statement"
)

func (e CreateSqlv1Statement201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateSqlv1Statement201JSONResponseBodyKindStatement:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Statement200JSONResponseBodyApiVersionSqlv1 GetSqlv1Statement200JSONResponseBodyApiVersion = "sql/v1"
)

func (e GetSqlv1Statement200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetSqlv1Statement200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	GetSqlv1Statement200JSONResponseBodyKindStatement GetSqlv1Statement200JSONResponseBodyKind = "Statement"
)

func (e GetSqlv1Statement200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetSqlv1Statement200JSONResponseBodyKindStatement:
		return true
	default:
		return false
	}
}

const (
	PatchSqlv1Statement200JSONResponseBodyApiVersionSqlv1 PatchSqlv1Statement200JSONResponseBodyApiVersion = "sql/v1"
)

func (e PatchSqlv1Statement200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case PatchSqlv1Statement200JSONResponseBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	PatchSqlv1Statement200JSONResponseBodyKindStatement PatchSqlv1Statement200JSONResponseBodyKind = "Statement"
)

func (e PatchSqlv1Statement200JSONResponseBodyKind) Valid() bool {
	switch e {
	case PatchSqlv1Statement200JSONResponseBodyKindStatement:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1StatementJSONBodyApiVersionSqlv1 UpdateSqlv1StatementJSONBodyApiVersion = "sql/v1"
)

func (e UpdateSqlv1StatementJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateSqlv1StatementJSONBodyApiVersionSqlv1:
		return true
	default:
		return false
	}
}

const (
	UpdateSqlv1StatementJSONBodyKindStatement UpdateSqlv1StatementJSONBodyKind = "Statement"
)

func (e UpdateSqlv1StatementJSONBodyKind) Valid() bool {
	switch e {
	case UpdateSqlv1StatementJSONBodyKindStatement:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type ColumnDetails struct {
	// Name The name of the SQL table column.
	Name string `json:"name"`

	// Type JSON object in TableSchema format; describes the data returned by the results serving API.
	Type DataType `json:"type"`
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
type JsonPatchRequestAddReplace struct {
	// Op The operation to perform.
	Op JsonPatchRequestAddReplaceOp `json:"op"`

	// Path A JSON Pointer path.
	Path string `json:"path"`

	// Value The value to add, replace or test.
	Value interface{} `json:"value"`
}
type JsonPatchRequestAddReplaceOp string
type JsonPatchRequestMoveCopy struct {
	// From A JSON Pointer path.
	From string `json:"from"`

	// Op The operation to perform.
	Op JsonPatchRequestMoveCopyOp `json:"op"`

	// Path A JSON Pointer path.
	Path string `json:"path"`
}
type JsonPatchRequestMoveCopyOp string
type JsonPatchRequestRemove struct {
	// Op The operation to perform.
	Op JsonPatchRequestRemoveOp `json:"op"`

	// Path A JSON Pointer path.
	Path string `json:"path"`
}
type JsonPatchRequestRemoveOp string
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
type PatchRequest = []PatchRequest_Item
type PatchRequest_Item struct {
	union json.RawMessage
}
type ResultListMeta struct {
	// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// Next A URL that can be followed to get the next batch of results.
	Next *string `json:"next,omitempty"`

	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self *string `json:"self,omitempty"`
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
type StatementObjectMeta struct {
	// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// Labels A map of key-value pairs that describe the resource.
	Labels *map[string]string `json:"labels,omitempty"`

	// ResourceVersion A system generated string that uniquely identifies the version of this resource.
	ResourceVersion *string `json:"resource_version,omitempty"`

	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self string `json:"self"`

	// Uid A system generated globally unique identifier for this resource.
	Uid *string `json:"uid,omitempty"`

	// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
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
type ArtifactV1FlinkArtifactList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ArtifactV1FlinkArtifactListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *ArtifactV1FlinkArtifactListDataApiVersion `json:"api_version,omitempty"`

		// Class Java class or alias for the artifact as provided by developer. Deprecated
		// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
		Class *string `json:"class,omitempty"`

		// Cloud Cloud provider where the Flink Artifact archive is uploaded.
		Cloud string `json:"cloud"`

		// ContentFormat Archive format of the Flink Artifact.
		ContentFormat *string `json:"content_format,omitempty"`

		// Description Description of the Flink Artifact.
		Description *string `json:"description,omitempty"`

		// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
		DisplayName string `json:"display_name"`

		// DocumentationLink Documentation link of the Flink Artifact.
		DocumentationLink *string `json:"documentation_link,omitempty"`

		// Environment Environment the Flink Artifact belongs to.
		Environment string `json:"environment"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *ArtifactV1FlinkArtifactListDataKind `json:"kind,omitempty"`
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

		// Region The Cloud provider region the Flink Artifact archive is uploaded.
		Region string `json:"region"`

		// RuntimeLanguage Runtime language of the Flink Artifact.
		RuntimeLanguage *string `json:"runtime_language,omitempty"`

		// Versions Versions associated with this Flink Artifact.
		Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ArtifactV1FlinkArtifactListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type ArtifactV1FlinkArtifactListApiVersion string
type ArtifactV1FlinkArtifactListDataApiVersion string
type ArtifactV1FlinkArtifactListDataKind string
type ArtifactV1FlinkArtifactListKind string
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
type ArtifactV1PresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1PresignedUrlApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Flink Artifact archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Flink Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *ArtifactV1PresignedUrlKind `json:"kind,omitempty"`

	// Region The Cloud provider region the Flink Artifact archive is uploaded.
	Region *string `json:"region,omitempty"`

	// UploadFormData Upload form data of the Flink Artifact. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Flink Artifact archive.
	UploadUrl *string `json:"upload_url,omitempty"`
}
type ArtifactV1PresignedUrlApiVersion string
type ArtifactV1PresignedUrlKind string
type ArtifactV1PresignedUrlRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1PresignedUrlRequestApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Flink Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ArtifactV1PresignedUrlRequestKind `json:"kind,omitempty"`
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
}
type ArtifactV1PresignedUrlRequestApiVersion string
type ArtifactV1PresignedUrlRequestKind string
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
type FcpmV2ComputePool struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *FcpmV2ComputePoolApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *FcpmV2ComputePoolKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Compute Pool
	Spec *FcpmV2ComputePoolSpec `json:"spec,omitempty"`

	// Status The status of the Compute Pool
	Status *FcpmV2ComputePoolStatus `json:"status,omitempty"`
}
type FcpmV2ComputePoolApiVersion string
type FcpmV2ComputePoolKind string
type FcpmV2ComputePoolList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion FcpmV2ComputePoolListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *FcpmV2ComputePoolListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *FcpmV2ComputePoolListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Compute Pool
		Status FcpmV2ComputePoolStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     FcpmV2ComputePoolListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type FcpmV2ComputePoolListApiVersion string
type FcpmV2ComputePoolListDataApiVersion string
type FcpmV2ComputePoolListDataKind string
type FcpmV2ComputePoolListKind string
type FcpmV2ComputePoolSpec struct {
	// Cloud The cloud service provider that runs the compute pool.
	Cloud *string `json:"cloud,omitempty"`

	// DefaultPool The flag to indicate whether the Flink compute pool is a default compute pool or not.
	// Only one default compute pool per environment and region is allowed.
	DefaultPool *bool `json:"default_pool,omitempty"`

	// DisplayName The name of the Flink compute pool.
	DisplayName *string `json:"display_name,omitempty"`

	// EnableAi The flag to enable AI computing using Ray for the Flink compute pool. It's available in the Early Access API
	// lifecycle stage only.
	EnableAi *bool `json:"enable_ai,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// MaxCfu Maximum number of Confluent Flink Units (CFUs) that the Flink compute pool should auto-scale to.
	MaxCfu *int32 `json:"max_cfu,omitempty"`

	// Network The network to which this belongs.
	Network *EnvScopedObjectReference `json:"network,omitempty"`

	// Region Flink compute pools in the region provided will be able to use this identity pool
	Region *string `json:"region,omitempty"`
}
type FcpmV2ComputePoolStatus struct {
	// CurrentCfu The number of Confluent Flink Units (CFUs) currently allocated to this Flink compute pool.
	CurrentCfu int32 `json:"current_cfu"`

	// Phase Status of the Flink compute pool.
	Phase string `json:"phase"`
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
type SqlV1ResultSchema struct {
	// Columns The properties of each SQL column in the schema.
	Columns *[]ColumnDetails `json:"columns,omitempty"`
}
type SqlV1ScalingSpec struct {
	// BaselineCfu The baseline number of Confluent Flink Units (CFUs) targeted for the statement.
	// This is a best-effort target rather than a guarantee, and the statement may auto-scale above it.
	BaselineCfu *int32 `json:"baseline_cfu,omitempty"`
}
type SqlV1ScalingStatus struct {
	// LastUpdated The last time the scaling status was updated.
	LastUpdated *time.Time `json:"last_updated,omitempty"`

	// ScalingState OK: The statement runs at the right scale.
	//
	// PENDING_SCALE_DOWN: The statement requires less resources, and will be scaled down in the near future.
	//
	// PENDING_SCALE_UP: The statement requires more resources, and will be scaled up in the near future.
	//
	// POOL_EXHAUSTED: The statement requires more resources, but not enough resources are available.
	ScalingState *string `json:"scaling_state,omitempty"`
}
type SqlV1StateLimitStatus struct {
	// Detail Details about why state limit status is in its current state.
	Detail *string `json:"detail,omitempty"`

	// LastUpdated The last time the state limit status was updated.
	LastUpdated *time.Time `json:"last_updated,omitempty"`

	// StateLimitState OK: The statement is within state limits.
	//
	// APPROACHING_SOFT_LIMIT: The statement is approaching soft state limits.
	//
	// EXCEEDING_SOFT_LIMIT: The statement is exceeding soft state limits.
	//
	// APPROACHING_HARD_LIMIT: The statement is approaching hard state limits.
	//
	// EXCEEDING_HARD_LIMIT: The statement is exceeding hard state limits.
	StateLimitState *string `json:"state_limit_state,omitempty"`
}
type SqlV1Statement struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *SqlV1StatementApiVersion `json:"api_version,omitempty"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId *string `json:"environment_id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *SqlV1StatementKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt       *time.Time         `json:"created_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string `json:"name,omitempty"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId *openapi_types.UUID `json:"organization_id,omitempty"`

	// Result `Statement Result` represents a resource used to model results of SQL statements.
	// The API allows you to read your SQL statement result.
	Result *SqlV1StatementResult `json:"result,omitempty"`

	// Spec The specs of the Statement
	Spec *SqlV1StatementSpec `json:"spec,omitempty"`

	// Status The status of the Statement
	Status *SqlV1StatementStatus `json:"status,omitempty"`
}
type SqlV1StatementApiVersion string
type SqlV1StatementKind string
type SqlV1StatementList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1StatementListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion SqlV1StatementListDataApiVersion `json:"api_version"`

		// EnvironmentId The unique identifier for the environment.
		EnvironmentId string `json:"environment_id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     SqlV1StatementListDataKind `json:"kind"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt       *time.Time         `json:"created_at,omitempty"`
			Labels          *map[string]string `json:"labels,omitempty"`
			ResourceName    interface{}        `json:"resource_name,omitempty"`
			ResourceVersion interface{}        `json:"resource_version,omitempty"`
			Self            interface{}        `json:"self"`
			Uid             interface{}        `json:"uid,omitempty"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Name The user provided name of the resource, unique within this environment.
		Name string `json:"name"`

		// OrganizationId The unique identifier for the organization.
		OrganizationId openapi_types.UUID `json:"organization_id"`

		// Result `Statement Result` represents a resource used to model results of SQL statements.
		// The API allows you to read your SQL statement result.
		Result *SqlV1StatementResult  `json:"result,omitempty"`
		Spec   map[string]interface{} `json:"spec"`

		// Status The status of the Statement
		Status SqlV1StatementStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1StatementListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`
		Self  interface{} `json:"self,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type SqlV1StatementListApiVersion string
type SqlV1StatementListDataApiVersion string
type SqlV1StatementListDataKind string
type SqlV1StatementListKind string
type SqlV1StatementResult struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion SqlV1StatementResultApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind     SqlV1StatementResultKind     `json:"kind"`
	Metadata ResultListMeta               `json:"metadata"`
	Results  *SqlV1StatementResultResults `json:"results,omitempty"`
}
type SqlV1StatementResultApiVersion string
type SqlV1StatementResultKind string
type SqlV1StatementResultResults struct {
	// Data A data property that contains an array of results. Each entry in the array is a separate result.
	//
	// The value of `op` attribute (if present) represents the kind of change that a row can describe in a changelog:
	//
	// `0`: represents `INSERT` (`+I`), i.e. insertion operation;
	//
	// `1`: represents `UPDATE_BEFORE` (`-U`), i.e. update operation with the previous content of the updated row.
	// This kind should occur together with `UPDATE_AFTER` for modelling an update that needs to retract
	// the previous row first. It is useful in cases of a non-idempotent update, i.e., an update of a row that is not
	// uniquely identifiable by a key;
	//
	// `2`: represents `UPDATE_AFTER` (`+U`), i.e. update operation with new content of the updated row;
	// This kind CAN occur together with `UPDATE_BEFORE` for modelling an update that
	// needs to retract the previous row first or it describes an idempotent update, i.e., an
	// update of a row that is uniquely identifiable by a key;
	//
	// `3`: represents `DELETE` (`-D`), i.e. deletion operation;
	//
	// Defaults to `0`.
	Data *[]interface{} `json:"data,omitempty"`
}
type SqlV1StatementSpec struct {
	// ComputePoolId The id associated with the compute pool in context.
	// If not specified, the statement will use the default compute pool. The default pool is automatically determined by the system.
	ComputePoolId *string `json:"compute_pool_id,omitempty"`

	// ExecutionMode The execution mode of the statement.
	//
	// Note - The attribute is in a [Early Access lifecycle](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	ExecutionMode *string `json:"execution_mode,omitempty"`

	// Principal The id of the principal this statement runs as. Possible values:
	//
	//   * `u-abc123` — user
	//   * `sa-abc123` — service account
	//   * `pool-abc123` — identity pool (OAuth caller authorized
	//     against a single pool, either explicitly supplied or
	//     resolved by the server)
	//   * an identity CRN equal to `status.identity` (OAuth caller
	//     authorized against multiple identity pools)
	//
	// Customers typically supply one of the short prefixed ids and read
	// the same value back. The CRN form is server-set in the multi-pool
	// case; clients should accept it when reading but should not need
	// to construct it.
	Principal *string `json:"principal,omitempty"`

	// Properties A map (key-value pairs) of statement properties.
	Properties *map[string]string `json:"properties,omitempty"`

	// Scaling Mutable scaling configuration that can be updated for a running statement.
	Scaling *SqlV1ScalingSpec `json:"scaling,omitempty"`

	// Statement The raw SQL text statement.
	Statement *string `json:"statement,omitempty"`

	// Stopped Indicates whether the statement should be stopped.
	Stopped *bool `json:"stopped,omitempty"`
}
type SqlV1StatementStatus struct {
	// AffectedResource A reference to the resource created by this statement, if any. This field is set when a statement
	// (e.g., CREATE MATERIALIZED TABLE) results in a new user-facing resource.
	//
	// Note - The attribute is in a [Early Access lifecycle](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	AffectedResource *struct {
		// DatabaseId The unique identifier for the database containing the resource.
		// Only present for resource kinds that are scoped to a database.
		DatabaseId *string `json:"database_id,omitempty"`

		// EnvironmentId The unique identifier for the environment containing the resource.
		EnvironmentId string `json:"environment_id"`

		// Kind The kind of resource that was created.
		Kind string `json:"kind"`

		// ResourceName The name of the created resource, unique within its scope.
		ResourceName string `json:"resource_name"`
	} `json:"affected_resource,omitempty"`

	// Detail Details about the execution status of this statement.
	Detail *string `json:"detail,omitempty"`

	// Duration The total elapsed time (represented as ISO 8601 format) from when the statement transitioned from
	// PENDING to RUNNING until it reached a final terminal state. This field is calculated and set when
	// the Phase is COMPLETED, FAILED, or STOPPED.
	//
	// Note - The attribute is in a [Early Access lifecycle](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	Duration *openapi_types.Duration `json:"duration,omitempty"`

	// EndTime The date and time in UTC (represented as RFC3339 format) at which the statement reached its final terminal state.
	// This field is set when the Phase is COMPLETED, FAILED, or STOPPED.
	//
	// Note - The attribute is in a [Early Access lifecycle](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	EndTime *time.Time `json:"end_time,omitempty"`

	// Identity Optional, read-only. The CRN of the authenticated principal that
	// currently owns this statement, sourced from the upstream
	// authentication decision. Initially populated when the statement
	// is created and refreshed when `spec.principal` is updated, so it
	// reflects the most recent setter rather than the original creator.
	//
	// Treat as a stable, opaque, non-parsable string for storage and
	// mapping; do not implement business logic or branching based on
	// the identity type or string structure. The set of identity
	// shapes may grow over time and is not considered breaking.
	//
	// The `example` above illustrates the OAuth (identity-provider)
	// shape. Set for all supported authentication methods; the CRN
	// shape varies by auth type:
	//
	//   * User: `crn://confluent.cloud/organization=12345/user=u-abc123`
	//   * Service account: `crn://confluent.cloud/organization=12345/service-account=sa-abc123`
	//   * OAuth (identity provider): `crn://confluent.cloud/organization=12345/identity-provider=op-12345/identity=alice@example.com`
	//
	// For the OAuth shape, the value after `identity=` is the JWT
	// claim resolved by the identity provider's configured
	// `identity_claim` (or the identity pool's `identity_claim` when
	// a single pool is in scope) — commonly `claims.sub`, but
	// provider-configurable. The email shown above is just one
	// example; the actual value depends on the identity provider
	// configuration and may be a subject UUID, a username, or any
	// other opaque claim value.
	//
	// When the request was authenticated via OAuth and authorized
	// against multiple identity pools, this CRN also appears as
	// `spec.principal`. May be omitted for statements created before
	// this field was introduced.
	Identity *string `json:"identity,omitempty"`

	// LatestOffsets The last Kafka offsets that a statement has processed. Represented by a mapping from Kafka topic to a
	// string representation of partitions mapped to offsets.
	LatestOffsets *map[string]string `json:"latest_offsets,omitempty"`

	// LatestOffsetsTimestamp The date and time at which the Kafka topic offsets were added to the statement status. It is represented in RFC3339 format and is in UTC.
	LatestOffsetsTimestamp *time.Time `json:"latest_offsets_timestamp,omitempty"`

	// NetworkKind The networking type used by the submitted SQL statement:
	//
	// PUBLIC: SQL statement is using public networking;
	//
	// PRIVATE: SQL statement is using private networking;
	NetworkKind *string `json:"network_kind,omitempty"`

	// Phase The lifecycle phase of the submitted SQL statement:
	//
	// PENDING: SQL statement is pending execution;
	//
	// RUNNING: SQL statement execution is in progress;
	//
	// COMPLETED: SQL statement is completed;
	//
	// DELETING: SQL statement deletion is in progress;
	//
	// FAILING: SQL statement is failing;
	//
	// FAILED: SQL statement execution has failed;
	//
	// STOPPING: SQL statement is being stopped;
	//
	// STOPPED: SQL statement execution has successfully been stopped;
	//
	// DEGRADED: SQL statement is experiencing reduced performance or partial failure;
	Phase string `json:"phase"`

	// ScalingStatus Scaling status for this statement.
	ScalingStatus *SqlV1ScalingStatus `json:"scaling_status,omitempty"`

	// StateLimitStatus State limit status for this statement.
	StateLimitStatus *SqlV1StateLimitStatus `json:"state_limit_status,omitempty"`

	// Traits StatementTraits contains detailed information about the properties of a Statement
	Traits *SqlV1StatementTraits `json:"traits,omitempty"`

	// Warnings List of warnings encountered during statement execution.
	Warnings *[]SqlV1StatementWarning `json:"warnings,omitempty"`
}
type SqlV1StatementTraits struct {
	// ConnectionRefs The names of connections that the SQL statement references (e.g., in FROM clauses).
	ConnectionRefs *[]string `json:"connection_refs,omitempty"`

	// IsAppendOnly Indicates the special case where results of a statement are insert/append only.
	IsAppendOnly *bool `json:"is_append_only,omitempty"`

	// IsBounded Indicates the special case where results of a statement are bounded.
	IsBounded *bool `json:"is_bounded,omitempty"`

	// Schema The table columns of the results schema.
	Schema *SqlV1ResultSchema `json:"schema,omitempty"`

	// SqlKind Categorizes the SQL statement. The result is Confluent-specific but inspired by SQL. It uses underscores for separating concepts e.g. "CREATE_TABLE".
	SqlKind *string `json:"sql_kind,omitempty"`

	// UpsertColumns Defines the column indices clients can use as upsert keys.
	UpsertColumns *[]int `json:"upsert_columns,omitempty"`
}
type SqlV1StatementWarning struct {
	// CreatedAt The timestamp when the warning was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt time.Time `json:"created_at"`

	// Message A human-readable string containing the description of the warning.
	Message string `json:"message"`

	// Reason A machine-readable short, upper case summary delimited by underscore.
	Reason string `json:"reason"`

	// Severity Indicates the severity of the warning.
	//
	// LOW: Indicates a low severity warning and for informing the user.
	//
	// MODERATE: Indicates a moderate severity warning and may require user action. Could cause degraded statements if certain conditions apply.
	//
	// CRITICAL: Indicates a critical severity warning and requires user action. It will cause degraded statements eventually.
	Severity SqlV1WarningSeverity `json:"severity"`
}
type SqlV1WarningSeverity = string
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
type PresignedUploadUrlArtifactV1PresignedUrlJSONBody struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersion `json:"api_version,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat string `json:"content_format"`

	// Environment The Environment the uploaded Flink Artifact belongs to.
	Environment string `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKind `json:"kind,omitempty"`
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
	Region string `json:"region"`
}
type PresignedUploadUrlArtifactV1PresignedUrlJSONBodyApiVersion string
type PresignedUploadUrlArtifactV1PresignedUrlJSONBodyKind string
type PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion string
type PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind string
type PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody PresignedUploadUrlArtifactV1PresignedUrlJSONBody

func (t PatchRequest_Item) AsJsonPatchRequestAddReplace() (JsonPatchRequestAddReplace, error) {
	var body JsonPatchRequestAddReplace
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchRequest_Item) FromJsonPatchRequestAddReplace(v JsonPatchRequestAddReplace) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchRequest_Item) MergeJsonPatchRequestAddReplace(v JsonPatchRequestAddReplace) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchRequest_Item) AsJsonPatchRequestRemove() (JsonPatchRequestRemove, error) {
	var body JsonPatchRequestRemove
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchRequest_Item) FromJsonPatchRequestRemove(v JsonPatchRequestRemove) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchRequest_Item) MergeJsonPatchRequestRemove(v JsonPatchRequestRemove) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchRequest_Item) AsJsonPatchRequestMoveCopy() (JsonPatchRequestMoveCopy, error) {
	var body JsonPatchRequestMoveCopy
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *PatchRequest_Item) FromJsonPatchRequestMoveCopy(v JsonPatchRequestMoveCopy) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *PatchRequest_Item) MergeJsonPatchRequestMoveCopy(v JsonPatchRequestMoveCopy) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t PatchRequest_Item) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *PatchRequest_Item) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
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
func (t CreateArtifactV1FlinkArtifactJSONBody_UploadSource) AsArtifactV1UploadSourcePresignedUrl() (ArtifactV1UploadSourcePresignedUrl, error) {
	var body ArtifactV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *CreateArtifactV1FlinkArtifactJSONBody_UploadSource) FromArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *CreateArtifactV1FlinkArtifactJSONBody_UploadSource) MergeArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
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
func (t CreateArtifactV1FlinkArtifactJSONBody_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t CreateArtifactV1FlinkArtifactJSONBody_UploadSource) ValueByDiscriminator() (interface{}, error) {
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
func (t CreateArtifactV1FlinkArtifactJSONBody_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *CreateArtifactV1FlinkArtifactJSONBody_UploadSource) UnmarshalJSON(b []byte) error {
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

	// ListArtifactV1FlinkArtifacts List of Flink Artifacts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all flink artifacts.
	//
	// Corresponds with GET /artifact/v1/flink-artifacts (the `ListArtifactV1FlinkArtifacts` operationId).
	ListArtifactV1FlinkArtifacts(ctx context.Context, params *ListArtifactV1FlinkArtifactsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateArtifactV1FlinkArtifactWithBody Create a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a flink artifact.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /artifact/v1/flink-artifacts (the `CreateArtifactV1FlinkArtifact` operationId).
	CreateArtifactV1FlinkArtifactWithBody(ctx context.Context, params *CreateArtifactV1FlinkArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateArtifactV1FlinkArtifact Create a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a flink artifact.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /artifact/v1/flink-artifacts (the `CreateArtifactV1FlinkArtifact` operationId).
	CreateArtifactV1FlinkArtifact(ctx context.Context, params *CreateArtifactV1FlinkArtifactParams, body CreateArtifactV1FlinkArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteArtifactV1FlinkArtifact Delete a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a flink artifact.
	//
	// Corresponds with DELETE /artifact/v1/flink-artifacts/{id} (the `DeleteArtifactV1FlinkArtifact` operationId).
	DeleteArtifactV1FlinkArtifact(ctx context.Context, id string, params *DeleteArtifactV1FlinkArtifactParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetArtifactV1FlinkArtifact Read a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a flink artifact.
	//
	// Corresponds with GET /artifact/v1/flink-artifacts/{id} (the `GetArtifactV1FlinkArtifact` operationId).
	GetArtifactV1FlinkArtifact(ctx context.Context, id string, params *GetArtifactV1FlinkArtifactParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateArtifactV1FlinkArtifactWithBody Update a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a flink artifact.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /artifact/v1/flink-artifacts/{id} (the `UpdateArtifactV1FlinkArtifact` operationId).
	UpdateArtifactV1FlinkArtifactWithBody(ctx context.Context, id string, params *UpdateArtifactV1FlinkArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateArtifactV1FlinkArtifact Update a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a flink artifact.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /artifact/v1/flink-artifacts/{id} (the `UpdateArtifactV1FlinkArtifact` operationId).
	UpdateArtifactV1FlinkArtifact(ctx context.Context, id string, params *UpdateArtifactV1FlinkArtifactParams, body UpdateArtifactV1FlinkArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlArtifactV1PresignedUrlWithBody Request a presigned upload URL for a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Flink Artifact archive.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /artifact/v1/presigned-upload-url (the `PresignedUploadUrlArtifactV1PresignedUrl` operationId).
	PresignedUploadUrlArtifactV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PresignedUploadUrlArtifactV1PresignedUrl Request a presigned upload URL for a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Flink Artifact archive.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /artifact/v1/presigned-upload-url (the `PresignedUploadUrlArtifactV1PresignedUrl` operationId).
	PresignedUploadUrlArtifactV1PresignedUrl(ctx context.Context, body PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListFcpmV2ComputePools List of Compute Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all compute pools.
	//
	// Corresponds with GET /fcpm/v2/compute-pools (the `ListFcpmV2ComputePools` operationId).
	ListFcpmV2ComputePools(ctx context.Context, params *ListFcpmV2ComputePoolsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateFcpmV2ComputePoolWithBody Create a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a compute pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /fcpm/v2/compute-pools (the `CreateFcpmV2ComputePool` operationId).
	CreateFcpmV2ComputePoolWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateFcpmV2ComputePool Create a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a compute pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /fcpm/v2/compute-pools (the `CreateFcpmV2ComputePool` operationId).
	CreateFcpmV2ComputePool(ctx context.Context, body CreateFcpmV2ComputePoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteFcpmV2ComputePool Delete a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a compute pool.
	//
	// Corresponds with DELETE /fcpm/v2/compute-pools/{id} (the `DeleteFcpmV2ComputePool` operationId).
	DeleteFcpmV2ComputePool(ctx context.Context, id string, params *DeleteFcpmV2ComputePoolParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetFcpmV2ComputePool Read a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a compute pool.
	//
	// Corresponds with GET /fcpm/v2/compute-pools/{id} (the `GetFcpmV2ComputePool` operationId).
	GetFcpmV2ComputePool(ctx context.Context, id string, params *GetFcpmV2ComputePoolParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateFcpmV2ComputePoolWithBody Update a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a compute pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /fcpm/v2/compute-pools/{id} (the `UpdateFcpmV2ComputePool` operationId).
	UpdateFcpmV2ComputePoolWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateFcpmV2ComputePool Update a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a compute pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /fcpm/v2/compute-pools/{id} (the `UpdateFcpmV2ComputePool` operationId).
	UpdateFcpmV2ComputePool(ctx context.Context, id string, body UpdateFcpmV2ComputePoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListSqlv1Statements List of Statements
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all statements.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `ListSqlv1Statements` operationId).
	ListSqlv1Statements(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1StatementsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1StatementWithBody Create a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a statement.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `CreateSqlv1Statement` operationId).
	CreateSqlv1StatementWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateSqlv1Statement Create a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a statement.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `CreateSqlv1Statement` operationId).
	CreateSqlv1Statement(ctx context.Context, organizationId openapi_types.UUID, environmentId string, body CreateSqlv1StatementJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteSqlv1Statement Delete a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a statement.
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `DeleteSqlv1Statement` operationId).
	DeleteSqlv1Statement(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetSqlv1Statement Read a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a statement.
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `GetSqlv1Statement` operationId).
	GetSqlv1Statement(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PatchSqlv1StatementWithBody Patch a Statement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a statement.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `PatchSqlv1Statement` operationId).
	PatchSqlv1StatementWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// PatchSqlv1StatementWithApplicationJSONPatchPlusJSONBody Patch a Statement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a statement.
	//
	// Takes a body of the `application/json-patch+json` content type.
	//
	// Corresponds with PATCH /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `PatchSqlv1Statement` operationId).
	PatchSqlv1StatementWithApplicationJSONPatchPlusJSONBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, body PatchSqlv1StatementApplicationJSONPatchPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1StatementWithBody Update a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a statement.
	// The request will fail with a 409 Conflict error if the Statement has changed since it was fetched.
	// In this case, do a GET, reapply the modifications, and try the update again.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `UpdateSqlv1Statement` operationId).
	UpdateSqlv1StatementWithBody(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateSqlv1Statement Update a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a statement.
	// The request will fail with a 409 Conflict error if the Statement has changed since it was fetched.
	// In this case, do a GET, reapply the modifications, and try the update again.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `UpdateSqlv1Statement` operationId).
	UpdateSqlv1Statement(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, body UpdateSqlv1StatementJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *Client) PresignedUploadUrlArtifactV1PresignedUrlWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlArtifactV1PresignedUrlRequestWithBody(c.Server, contentType, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *Client) PresignedUploadUrlArtifactV1PresignedUrl(ctx context.Context, body PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewPresignedUploadUrlArtifactV1PresignedUrlRequest(c.Server, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewPresignedUploadUrlArtifactV1PresignedUrlRequest(server string, body PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPresignedUploadUrlArtifactV1PresignedUrlRequestWithBody(server, "application/json", bodyReader)
}
func NewPresignedUploadUrlArtifactV1PresignedUrlRequestWithBody(server string, contentType string, body io.Reader) (*http.Request, error) {
	var err error

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/artifact/v1/presigned-upload-url")
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
func NewPatchSqlv1StatementRequestWithApplicationJSONPatchPlusJSONBody(server string, organizationId openapi_types.UUID, environmentId string, statementName string, body PatchSqlv1StatementApplicationJSONPatchPlusJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewPatchSqlv1StatementRequestWithBody(server, organizationId, environmentId, statementName, "application/json-patch+json", bodyReader)
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

	// ListArtifactV1FlinkArtifactsWithResponse List of Flink Artifacts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all flink artifacts.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /artifact/v1/flink-artifacts (the `ListArtifactV1FlinkArtifacts` operationId).
	ListArtifactV1FlinkArtifactsWithResponse(ctx context.Context, params *ListArtifactV1FlinkArtifactsParams, reqEditors ...RequestEditorFn) (*ListArtifactV1FlinkArtifactsResponse, error)

	// CreateArtifactV1FlinkArtifactWithBodyWithResponse Create a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a flink artifact.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /artifact/v1/flink-artifacts (the `CreateArtifactV1FlinkArtifact` operationId).
	CreateArtifactV1FlinkArtifactWithBodyWithResponse(ctx context.Context, params *CreateArtifactV1FlinkArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateArtifactV1FlinkArtifactResponse, error)

	// CreateArtifactV1FlinkArtifactWithResponse Create a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a flink artifact.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /artifact/v1/flink-artifacts (the `CreateArtifactV1FlinkArtifact` operationId).
	CreateArtifactV1FlinkArtifactWithResponse(ctx context.Context, params *CreateArtifactV1FlinkArtifactParams, body CreateArtifactV1FlinkArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateArtifactV1FlinkArtifactResponse, error)

	// DeleteArtifactV1FlinkArtifactWithResponse Delete a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a flink artifact.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /artifact/v1/flink-artifacts/{id} (the `DeleteArtifactV1FlinkArtifact` operationId).
	DeleteArtifactV1FlinkArtifactWithResponse(ctx context.Context, id string, params *DeleteArtifactV1FlinkArtifactParams, reqEditors ...RequestEditorFn) (*DeleteArtifactV1FlinkArtifactResponse, error)

	// GetArtifactV1FlinkArtifactWithResponse Read a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a flink artifact.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /artifact/v1/flink-artifacts/{id} (the `GetArtifactV1FlinkArtifact` operationId).
	GetArtifactV1FlinkArtifactWithResponse(ctx context.Context, id string, params *GetArtifactV1FlinkArtifactParams, reqEditors ...RequestEditorFn) (*GetArtifactV1FlinkArtifactResponse, error)

	// UpdateArtifactV1FlinkArtifactWithBodyWithResponse Update a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a flink artifact.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /artifact/v1/flink-artifacts/{id} (the `UpdateArtifactV1FlinkArtifact` operationId).
	UpdateArtifactV1FlinkArtifactWithBodyWithResponse(ctx context.Context, id string, params *UpdateArtifactV1FlinkArtifactParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateArtifactV1FlinkArtifactResponse, error)

	// UpdateArtifactV1FlinkArtifactWithResponse Update a Flink Artifact
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a flink artifact.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /artifact/v1/flink-artifacts/{id} (the `UpdateArtifactV1FlinkArtifact` operationId).
	UpdateArtifactV1FlinkArtifactWithResponse(ctx context.Context, id string, params *UpdateArtifactV1FlinkArtifactParams, body UpdateArtifactV1FlinkArtifactJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateArtifactV1FlinkArtifactResponse, error)

	// PresignedUploadUrlArtifactV1PresignedUrlWithBodyWithResponse Request a presigned upload URL for a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Flink Artifact archive.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /artifact/v1/presigned-upload-url (the `PresignedUploadUrlArtifactV1PresignedUrl` operationId).
	PresignedUploadUrlArtifactV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlArtifactV1PresignedUrlResponse, error)

	// PresignedUploadUrlArtifactV1PresignedUrlWithResponse Request a presigned upload URL for a new Flink Artifact.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Request a presigned upload URL to upload a Flink Artifact archive.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /artifact/v1/presigned-upload-url (the `PresignedUploadUrlArtifactV1PresignedUrl` operationId).
	PresignedUploadUrlArtifactV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlArtifactV1PresignedUrlResponse, error)

	// ListFcpmV2ComputePoolsWithResponse List of Compute Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all compute pools.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /fcpm/v2/compute-pools (the `ListFcpmV2ComputePools` operationId).
	ListFcpmV2ComputePoolsWithResponse(ctx context.Context, params *ListFcpmV2ComputePoolsParams, reqEditors ...RequestEditorFn) (*ListFcpmV2ComputePoolsResponse, error)

	// CreateFcpmV2ComputePoolWithBodyWithResponse Create a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a compute pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /fcpm/v2/compute-pools (the `CreateFcpmV2ComputePool` operationId).
	CreateFcpmV2ComputePoolWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateFcpmV2ComputePoolResponse, error)

	// CreateFcpmV2ComputePoolWithResponse Create a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a compute pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /fcpm/v2/compute-pools (the `CreateFcpmV2ComputePool` operationId).
	CreateFcpmV2ComputePoolWithResponse(ctx context.Context, body CreateFcpmV2ComputePoolJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateFcpmV2ComputePoolResponse, error)

	// DeleteFcpmV2ComputePoolWithResponse Delete a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a compute pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /fcpm/v2/compute-pools/{id} (the `DeleteFcpmV2ComputePool` operationId).
	DeleteFcpmV2ComputePoolWithResponse(ctx context.Context, id string, params *DeleteFcpmV2ComputePoolParams, reqEditors ...RequestEditorFn) (*DeleteFcpmV2ComputePoolResponse, error)

	// GetFcpmV2ComputePoolWithResponse Read a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a compute pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /fcpm/v2/compute-pools/{id} (the `GetFcpmV2ComputePool` operationId).
	GetFcpmV2ComputePoolWithResponse(ctx context.Context, id string, params *GetFcpmV2ComputePoolParams, reqEditors ...RequestEditorFn) (*GetFcpmV2ComputePoolResponse, error)

	// UpdateFcpmV2ComputePoolWithBodyWithResponse Update a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a compute pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /fcpm/v2/compute-pools/{id} (the `UpdateFcpmV2ComputePool` operationId).
	UpdateFcpmV2ComputePoolWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateFcpmV2ComputePoolResponse, error)

	// UpdateFcpmV2ComputePoolWithResponse Update a Compute Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a compute pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /fcpm/v2/compute-pools/{id} (the `UpdateFcpmV2ComputePool` operationId).
	UpdateFcpmV2ComputePoolWithResponse(ctx context.Context, id string, body UpdateFcpmV2ComputePoolJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateFcpmV2ComputePoolResponse, error)

	// ListSqlv1StatementsWithResponse List of Statements
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all statements.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `ListSqlv1Statements` operationId).
	ListSqlv1StatementsWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, params *ListSqlv1StatementsParams, reqEditors ...RequestEditorFn) (*ListSqlv1StatementsResponse, error)

	// CreateSqlv1StatementWithBodyWithResponse Create a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a statement.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `CreateSqlv1Statement` operationId).
	CreateSqlv1StatementWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateSqlv1StatementResponse, error)

	// CreateSqlv1StatementWithResponse Create a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a statement.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements (the `CreateSqlv1Statement` operationId).
	CreateSqlv1StatementWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, body CreateSqlv1StatementJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateSqlv1StatementResponse, error)

	// DeleteSqlv1StatementWithResponse Delete a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a statement.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `DeleteSqlv1Statement` operationId).
	DeleteSqlv1StatementWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*DeleteSqlv1StatementResponse, error)

	// GetSqlv1StatementWithResponse Read a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a statement.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `GetSqlv1Statement` operationId).
	GetSqlv1StatementWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, reqEditors ...RequestEditorFn) (*GetSqlv1StatementResponse, error)

	// PatchSqlv1StatementWithBodyWithResponse Patch a Statement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a statement.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `PatchSqlv1Statement` operationId).
	PatchSqlv1StatementWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PatchSqlv1StatementResponse, error)

	// PatchSqlv1StatementWithApplicationJSONPatchPlusJSONBodyWithResponse Patch a Statement
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to patch a statement.
	//
	// Takes a body of the `application/json-patch+json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `PatchSqlv1Statement` operationId).
	PatchSqlv1StatementWithApplicationJSONPatchPlusJSONBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, body PatchSqlv1StatementApplicationJSONPatchPlusJSONRequestBody, reqEditors ...RequestEditorFn) (*PatchSqlv1StatementResponse, error)

	// UpdateSqlv1StatementWithBodyWithResponse Update a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a statement.
	// The request will fail with a 409 Conflict error if the Statement has changed since it was fetched.
	// In this case, do a GET, reapply the modifications, and try the update again.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `UpdateSqlv1Statement` operationId).
	UpdateSqlv1StatementWithBodyWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateSqlv1StatementResponse, error)

	// UpdateSqlv1StatementWithResponse Update a Statement
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a statement.
	// The request will fail with a 409 Conflict error if the Statement has changed since it was fetched.
	// In this case, do a GET, reapply the modifications, and try the update again.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /sql/v1/organizations/{organization_id}/environments/{environment_id}/statements/{statement_name} (the `UpdateSqlv1Statement` operationId).
	UpdateSqlv1StatementWithResponse(ctx context.Context, organizationId openapi_types.UUID, environmentId string, statementName string, body UpdateSqlv1StatementJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateSqlv1StatementResponse, error)
}

func (r ListArtifactV1FlinkArtifactsResponse) GetJSON200() *ArtifactV1FlinkArtifactList {
	return r.JSON200
}
func (r ListArtifactV1FlinkArtifactsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListArtifactV1FlinkArtifactsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListArtifactV1FlinkArtifactsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListArtifactV1FlinkArtifactsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListArtifactV1FlinkArtifactsResponse) GetBody() []byte {
	return r.Body
}
func (r ListArtifactV1FlinkArtifactsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListArtifactV1FlinkArtifactsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListArtifactV1FlinkArtifactsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateArtifactV1FlinkArtifact201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Class Java class or alias for the artifact as provided by developer. Deprecated
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	Class *string `json:"class,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Flink Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
	DisplayName string `json:"display_name"`

	// DocumentationLink Documentation link of the Flink Artifact.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment Environment the Flink Artifact belongs to.
	Environment string `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateArtifactV1FlinkArtifact201JSONResponseBodyKind `json:"kind,omitempty"`
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
	Region string `json:"region"`

	// RuntimeLanguage Runtime language of the Flink Artifact.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// Versions Versions associated with this Flink Artifact.
	Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
} {
	return r.JSON201
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateArtifactV1FlinkArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateArtifactV1FlinkArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r CreateArtifactV1FlinkArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateArtifactV1FlinkArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateArtifactV1FlinkArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteArtifactV1FlinkArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteArtifactV1FlinkArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteArtifactV1FlinkArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteArtifactV1FlinkArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetArtifactV1FlinkArtifact200JSONResponseBodyApiVersion `json:"api_version"`

	// Class Java class or alias for the artifact as provided by developer. Deprecated
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	Class *string `json:"class,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Flink Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
	DisplayName string `json:"display_name"`

	// DocumentationLink Documentation link of the Flink Artifact.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment Environment the Flink Artifact belongs to.
	Environment string `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetArtifactV1FlinkArtifact200JSONResponseBodyKind `json:"kind"`
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
	Region string `json:"region"`

	// RuntimeLanguage Runtime language of the Flink Artifact.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// Versions Versions associated with this Flink Artifact.
	Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
} {
	return r.JSON200
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetArtifactV1FlinkArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetArtifactV1FlinkArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r GetArtifactV1FlinkArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetArtifactV1FlinkArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetArtifactV1FlinkArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateArtifactV1FlinkArtifact200JSONResponseBodyApiVersion `json:"api_version"`

	// Class Java class or alias for the artifact as provided by developer. Deprecated
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	Class *string `json:"class,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud string `json:"cloud"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Flink Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
	DisplayName string `json:"display_name"`

	// DocumentationLink Documentation link of the Flink Artifact.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment Environment the Flink Artifact belongs to.
	Environment string `json:"environment"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateArtifactV1FlinkArtifact200JSONResponseBodyKind `json:"kind"`
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
	Region string `json:"region"`

	// RuntimeLanguage Runtime language of the Flink Artifact.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// Versions Versions associated with this Flink Artifact.
	Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
} {
	return r.JSON200
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateArtifactV1FlinkArtifactResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateArtifactV1FlinkArtifactResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateArtifactV1FlinkArtifactResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateArtifactV1FlinkArtifactResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type PresignedUploadUrlArtifactV1PresignedUrlResponse200Headers struct {
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse400Headers struct {
	XRequestId *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse403Headers struct {
	XRequestId *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse404Headers struct {
	XRequestId *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse500Headers struct {
	XRequestId *string
}
type PresignedUploadUrlArtifactV1PresignedUrlResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON200 the response for an HTTP 200 `application/json` response
	JSON200 *struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

		// Cloud Cloud provider where the Flink Artifact archive is uploaded.
		Cloud *string `json:"cloud,omitempty"`

		// ContentFormat Content format of the Flink Artifact archive.
		ContentFormat *string `json:"content_format,omitempty"`

		// Environment The Environment the uploaded Flink Artifact belongs to.
		Environment *string `json:"environment,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

		// Region The Cloud provider region the Flink Artifact archive is uploaded.
		Region *string `json:"region,omitempty"`

		// UploadFormData Upload form data of the Flink Artifact. All values should be strings.
		UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

		// UploadId Unique identifier of this upload.
		UploadId *string `json:"upload_id,omitempty"`

		// UploadUrl Upload URL for the Flink Artifact archive.
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
	Headers200 *PresignedUploadUrlArtifactV1PresignedUrlResponse200Headers
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *PresignedUploadUrlArtifactV1PresignedUrlResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *PresignedUploadUrlArtifactV1PresignedUrlResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *PresignedUploadUrlArtifactV1PresignedUrlResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *PresignedUploadUrlArtifactV1PresignedUrlResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *PresignedUploadUrlArtifactV1PresignedUrlResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *PresignedUploadUrlArtifactV1PresignedUrlResponse500Headers
}

func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Content format of the Flink Artifact archive.
	ContentFormat *string `json:"content_format,omitempty"`

	// Environment The Environment the uploaded Flink Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

	// Region The Cloud provider region the Flink Artifact archive is uploaded.
	Region *string `json:"region,omitempty"`

	// UploadFormData Upload form data of the Flink Artifact. All values should be strings.
	UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

	// UploadId Unique identifier of this upload.
	UploadId *string `json:"upload_id,omitempty"`

	// UploadUrl Upload URL for the Flink Artifact archive.
	UploadUrl *string `json:"upload_url,omitempty"`
} {
	return r.JSON200
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) GetBody() []byte {
	return r.Body
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PresignedUploadUrlArtifactV1PresignedUrlResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListFcpmV2ComputePoolsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListFcpmV2ComputePools200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListFcpmV2ComputePools200JSONResponseBodyKind `json:"kind"`
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
func (r ListFcpmV2ComputePoolsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListFcpmV2ComputePoolsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListFcpmV2ComputePoolsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListFcpmV2ComputePoolsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListFcpmV2ComputePoolsResponse) GetBody() []byte {
	return r.Body
}
func (r ListFcpmV2ComputePoolsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListFcpmV2ComputePoolsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListFcpmV2ComputePoolsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateFcpmV2ComputePool202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateFcpmV2ComputePool202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Compute Pool
	Status FcpmV2ComputePoolStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateFcpmV2ComputePoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateFcpmV2ComputePoolResponse) GetBody() []byte {
	return r.Body
}
func (r CreateFcpmV2ComputePoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateFcpmV2ComputePoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateFcpmV2ComputePoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteFcpmV2ComputePoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteFcpmV2ComputePoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteFcpmV2ComputePoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteFcpmV2ComputePoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteFcpmV2ComputePoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteFcpmV2ComputePoolResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteFcpmV2ComputePoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteFcpmV2ComputePoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteFcpmV2ComputePoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetFcpmV2ComputePoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetFcpmV2ComputePool200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetFcpmV2ComputePool200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Compute Pool
	Status FcpmV2ComputePoolStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetFcpmV2ComputePoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetFcpmV2ComputePoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetFcpmV2ComputePoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetFcpmV2ComputePoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetFcpmV2ComputePoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetFcpmV2ComputePoolResponse) GetBody() []byte {
	return r.Body
}
func (r GetFcpmV2ComputePoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetFcpmV2ComputePoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetFcpmV2ComputePoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateFcpmV2ComputePool200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateFcpmV2ComputePool200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Compute Pool
	Status FcpmV2ComputePoolStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateFcpmV2ComputePoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateFcpmV2ComputePoolResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateFcpmV2ComputePoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateFcpmV2ComputePoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateFcpmV2ComputePoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListSqlv1StatementsResponse) GetJSON200() *SqlV1StatementList {
	return r.JSON200
}
func (r ListSqlv1StatementsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListSqlv1StatementsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListSqlv1StatementsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListSqlv1StatementsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListSqlv1StatementsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListSqlv1StatementsResponse) GetBody() []byte {
	return r.Body
}
func (r ListSqlv1StatementsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListSqlv1StatementsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListSqlv1StatementsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateSqlv1StatementResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateSqlv1Statement201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId *string `json:"environment_id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateSqlv1Statement201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt       *time.Time         `json:"created_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string `json:"name,omitempty"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId *openapi_types.UUID `json:"organization_id,omitempty"`

	// Result `Statement Result` represents a resource used to model results of SQL statements.
	// The API allows you to read your SQL statement result.
	Result *SqlV1StatementResult  `json:"result,omitempty"`
	Spec   map[string]interface{} `json:"spec"`

	// Status The status of the Statement
	Status SqlV1StatementStatus `json:"status"`
} {
	return r.JSON201
}
func (r CreateSqlv1StatementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateSqlv1StatementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateSqlv1StatementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateSqlv1StatementResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateSqlv1StatementResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateSqlv1StatementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateSqlv1StatementResponse) GetBody() []byte {
	return r.Body
}
func (r CreateSqlv1StatementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateSqlv1StatementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateSqlv1StatementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteSqlv1StatementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteSqlv1StatementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteSqlv1StatementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteSqlv1StatementResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteSqlv1StatementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteSqlv1StatementResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteSqlv1StatementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteSqlv1StatementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteSqlv1StatementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetSqlv1StatementResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetSqlv1Statement200JSONResponseBodyApiVersion `json:"api_version"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId *string `json:"environment_id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetSqlv1Statement200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt       *time.Time         `json:"created_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string `json:"name,omitempty"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId *openapi_types.UUID `json:"organization_id,omitempty"`

	// Result `Statement Result` represents a resource used to model results of SQL statements.
	// The API allows you to read your SQL statement result.
	Result *SqlV1StatementResult  `json:"result,omitempty"`
	Spec   map[string]interface{} `json:"spec"`

	// Status The status of the Statement
	Status SqlV1StatementStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetSqlv1StatementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetSqlv1StatementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetSqlv1StatementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetSqlv1StatementResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetSqlv1StatementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetSqlv1StatementResponse) GetBody() []byte {
	return r.Body
}
func (r GetSqlv1StatementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetSqlv1StatementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetSqlv1StatementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r PatchSqlv1StatementResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *PatchSqlv1Statement200JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// EnvironmentId The unique identifier for the environment.
	EnvironmentId *string `json:"environment_id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *PatchSqlv1Statement200JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt       *time.Time         `json:"created_at,omitempty"`
		Labels          *map[string]string `json:"labels,omitempty"`
		ResourceName    interface{}        `json:"resource_name,omitempty"`
		ResourceVersion interface{}        `json:"resource_version,omitempty"`
		Self            interface{}        `json:"self"`
		Uid             interface{}        `json:"uid,omitempty"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Name The user provided name of the resource, unique within this environment.
	Name *string `json:"name,omitempty"`

	// OrganizationId The unique identifier for the organization.
	OrganizationId *openapi_types.UUID `json:"organization_id,omitempty"`

	// Result `Statement Result` represents a resource used to model results of SQL statements.
	// The API allows you to read your SQL statement result.
	Result *SqlV1StatementResult  `json:"result,omitempty"`
	Spec   map[string]interface{} `json:"spec"`

	// Status The status of the Statement
	Status SqlV1StatementStatus `json:"status"`
} {
	return r.JSON200
}
func (r PatchSqlv1StatementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r PatchSqlv1StatementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r PatchSqlv1StatementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r PatchSqlv1StatementResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r PatchSqlv1StatementResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r PatchSqlv1StatementResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r PatchSqlv1StatementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r PatchSqlv1StatementResponse) GetBody() []byte {
	return r.Body
}
func (r PatchSqlv1StatementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r PatchSqlv1StatementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r PatchSqlv1StatementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateSqlv1StatementResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateSqlv1StatementResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateSqlv1StatementResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateSqlv1StatementResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateSqlv1StatementResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateSqlv1StatementResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateSqlv1StatementResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateSqlv1StatementResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateSqlv1StatementResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) PresignedUploadUrlArtifactV1PresignedUrlWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PresignedUploadUrlArtifactV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlArtifactV1PresignedUrlWithBody(ctx, contentType, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlArtifactV1PresignedUrlResponse(rsp)
}
func (c *ClientWithResponses) PresignedUploadUrlArtifactV1PresignedUrlWithResponse(ctx context.Context, body PresignedUploadUrlArtifactV1PresignedUrlJSONRequestBody, reqEditors ...RequestEditorFn) (*PresignedUploadUrlArtifactV1PresignedUrlResponse, error) {
	rsp, err := c.PresignedUploadUrlArtifactV1PresignedUrl(ctx, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParsePresignedUploadUrlArtifactV1PresignedUrlResponse(rsp)
}
func ParsePresignedUploadUrlArtifactV1PresignedUrlResponse(rsp *http.Response) (*PresignedUploadUrlArtifactV1PresignedUrlResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &PresignedUploadUrlArtifactV1PresignedUrlResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 200:
		var dest struct {
			// ApiVersion APIVersion defines the schema version of this representation of a resource.
			ApiVersion PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyApiVersion `json:"api_version"`

			// Cloud Cloud provider where the Flink Artifact archive is uploaded.
			Cloud *string `json:"cloud,omitempty"`

			// ContentFormat Content format of the Flink Artifact archive.
			ContentFormat *string `json:"content_format,omitempty"`

			// Environment The Environment the uploaded Flink Artifact belongs to.
			Environment *string `json:"environment,omitempty"`

			// Kind Kind defines the object this REST resource represents.
			Kind PresignedUploadUrlArtifactV1PresignedUrl200JSONResponseBodyKind `json:"kind"`

			// Region The Cloud provider region the Flink Artifact archive is uploaded.
			Region *string `json:"region,omitempty"`

			// UploadFormData Upload form data of the Flink Artifact. All values should be strings.
			UploadFormData *map[string]interface{} `json:"upload_form_data,omitempty"`

			// UploadId Unique identifier of this upload.
			UploadId *string `json:"upload_id,omitempty"`

			// UploadUrl Upload URL for the Flink Artifact archive.
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
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse200Headers
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
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse401Headers
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
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse429Headers
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
		var headers PresignedUploadUrlArtifactV1PresignedUrlResponse500Headers
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
