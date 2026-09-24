package kafka

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
	ANY             AclResourceType = "ANY"
	CLUSTER         AclResourceType = "CLUSTER"
	DELEGATIONTOKEN AclResourceType = "DELEGATION_TOKEN"
	GROUP           AclResourceType = "GROUP"
	TOPIC           AclResourceType = "TOPIC"
	TRANSACTIONALID AclResourceType = "TRANSACTIONAL_ID"
	UNKNOWN         AclResourceType = "UNKNOWN"
)

func (e AclResourceType) Valid() bool {
	switch e {
	case ANY:
		return true
	case CLUSTER:
		return true
	case DELEGATIONTOKEN:
		return true
	case GROUP:
		return true
	case TOPIC:
		return true
	case TRANSACTIONALID:
		return true
	case UNKNOWN:
		return true
	default:
		return false
	}
}

const (
	ACTIVE                 MirrorTopicStatus = "ACTIVE"
	FAILED                 MirrorTopicStatus = "FAILED"
	LINKFAILED             MirrorTopicStatus = "LINK_FAILED"
	LINKPAUSED             MirrorTopicStatus = "LINK_PAUSED"
	PAUSED                 MirrorTopicStatus = "PAUSED"
	PENDINGMIRROR          MirrorTopicStatus = "PENDING_MIRROR"
	PENDINGRESTORE         MirrorTopicStatus = "PENDING_RESTORE"
	PENDINGSETUPFORRESTORE MirrorTopicStatus = "PENDING_SETUP_FOR_RESTORE"
	PENDINGSTOPPED         MirrorTopicStatus = "PENDING_STOPPED"
	PENDINGSYNCHRONIZE     MirrorTopicStatus = "PENDING_SYNCHRONIZE"
	SOURCEUNAVAILABLE      MirrorTopicStatus = "SOURCE_UNAVAILABLE"
	STOPPED                MirrorTopicStatus = "STOPPED"
)

func (e MirrorTopicStatus) Valid() bool {
	switch e {
	case ACTIVE:
		return true
	case FAILED:
		return true
	case LINKFAILED:
		return true
	case LINKPAUSED:
		return true
	case PAUSED:
		return true
	case PENDINGMIRROR:
		return true
	case PENDINGRESTORE:
		return true
	case PENDINGSETUPFORRESTORE:
		return true
	case PENDINGSTOPPED:
		return true
	case PENDINGSYNCHRONIZE:
		return true
	case SOURCEUNAVAILABLE:
		return true
	case STOPPED:
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

type AbstractConfigData struct {
	ClusterId   string              `json:"cluster_id"`
	IsDefault   bool                `json:"is_default"`
	IsReadOnly  bool                `json:"is_read_only"`
	IsSensitive bool                `json:"is_sensitive"`
	Kind        string              `json:"kind"`
	Metadata    ResourceMetadata    `json:"metadata"`
	Name        string              `json:"name"`
	Source      ConfigSource        `json:"source"`
	Synonyms    []ConfigSynonymData `json:"synonyms"`
	Value       *string             `json:"value,omitempty"`
}
type AclData struct {
	ClusterId    string           `json:"cluster_id"`
	Host         string           `json:"host"`
	Kind         string           `json:"kind"`
	Metadata     ResourceMetadata `json:"metadata"`
	Operation    AclOperation     `json:"operation"`
	PatternType  AclPatternType   `json:"pattern_type"`
	Permission   AclPermission    `json:"permission"`
	Principal    string           `json:"principal"`
	ResourceName string           `json:"resource_name"`
	ResourceType AclResourceType  `json:"resource_type"`
}
type AclDataList struct {
	Data     []AclData                  `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type AclOperation = string
type AclPatternType = string
type AclPermission = string
type AclResourceType string
type AlterConfigBatchRequestData struct {
	Data []struct {
		Name      string  `json:"name"`
		Operation *string `json:"operation,omitempty"`
		Value     *string `json:"value,omitempty"`
	} `json:"data"`
	ValidateOnly *bool `json:"validate_only,omitempty"`
}
type AlterMirrorStatusResponseData struct {
	ErrorCode                    *int                             `json:"error_code"`
	ErrorMessage                 *string                          `json:"error_message"`
	Kind                         string                           `json:"kind"`
	MessagesTruncated            *int64                           `json:"messages_truncated"`
	Metadata                     ResourceMetadata                 `json:"metadata"`
	MirrorLags                   MirrorLags                       `json:"mirror_lags"`
	MirrorTopicName              string                           `json:"mirror_topic_name"`
	PartitionLevelTruncationData PartitionLevelTruncationDataList `json:"partition_level_truncation_data"`
}
type AlterMirrorStatusResponseDataList struct {
	Data     []AlterMirrorStatusResponseData `json:"data"`
	Kind     string                          `json:"kind"`
	Metadata ResourceCollectionMetadata      `json:"metadata"`
}
type AlterMirrorsRequestData struct {
	// MirrorTopicNamePattern The mirror topics specified as a pattern.
	MirrorTopicNamePattern *string `json:"mirror_topic_name_pattern,omitempty"`

	// MirrorTopicNames The mirror topics specified as a list of topic names.
	MirrorTopicNames *[]string `json:"mirror_topic_names,omitempty"`
}
type AnyValue = interface{}
type AssignmentsType = string
type AuthorizedOperations = []string
type ClusterConfigData struct {
	ClusterId   string              `json:"cluster_id"`
	ConfigType  ClusterConfigType   `json:"config_type"`
	IsDefault   bool                `json:"is_default"`
	IsReadOnly  bool                `json:"is_read_only"`
	IsSensitive bool                `json:"is_sensitive"`
	Kind        string              `json:"kind"`
	Metadata    ResourceMetadata    `json:"metadata"`
	Name        string              `json:"name"`
	Source      ConfigSource        `json:"source"`
	Synonyms    []ConfigSynonymData `json:"synonyms"`
	Value       *string             `json:"value,omitempty"`
}
type ClusterConfigDataList struct {
	Data     []ClusterConfigData        `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ClusterConfigType = string
type ClusterData struct {
	Acls                   Relationship     `json:"acls"`
	BrokerConfigs          Relationship     `json:"broker_configs"`
	Brokers                Relationship     `json:"brokers"`
	ClusterId              string           `json:"cluster_id"`
	ConsumerGroups         Relationship     `json:"consumer_groups"`
	Controller             *Relationship    `json:"controller,omitempty"`
	Kind                   string           `json:"kind"`
	Metadata               ResourceMetadata `json:"metadata"`
	PartitionReassignments Relationship     `json:"partition_reassignments"`
	Topics                 Relationship     `json:"topics"`
}
type ConfigData struct {
	Name  string  `json:"name"`
	Value *string `json:"value"`
}
type ConfigSource = string
type ConfigSynonymData struct {
	Name   string       `json:"name"`
	Source ConfigSource `json:"source"`
	Value  *string      `json:"value,omitempty"`
}
type ConsumerData struct {
	Assignments     Relationship     `json:"assignments"`
	ClientId        string           `json:"client_id"`
	ClusterId       string           `json:"cluster_id"`
	ConsumerGroupId string           `json:"consumer_group_id"`
	ConsumerId      string           `json:"consumer_id"`
	InstanceId      *string          `json:"instance_id,omitempty"`
	Kind            string           `json:"kind"`
	Metadata        ResourceMetadata `json:"metadata"`
}
type ConsumerDataList struct {
	Data     []ConsumerData             `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ConsumerGroupData struct {
	ClusterId            string             `json:"cluster_id"`
	ConsumerGroupId      string             `json:"consumer_group_id"`
	Consumers            Relationship       `json:"consumers"`
	Coordinator          Relationship       `json:"coordinator"`
	IsMixedConsumerGroup bool               `json:"is_mixed_consumer_group"`
	IsSimple             bool               `json:"is_simple"`
	Kind                 string             `json:"kind"`
	LagSummary           Relationship       `json:"lag_summary"`
	Metadata             ResourceMetadata   `json:"metadata"`
	PartitionAssignor    string             `json:"partition_assignor"`
	State                ConsumerGroupState `json:"state"`
	Type                 ConsumerGroupType  `json:"type"`
}
type ConsumerGroupDataList struct {
	Data     []ConsumerGroupData        `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ConsumerGroupLagSummaryData struct {
	ClusterId         string           `json:"cluster_id"`
	ConsumerGroupId   string           `json:"consumer_group_id"`
	Kind              string           `json:"kind"`
	MaxLag            int64            `json:"max_lag"`
	MaxLagClientId    string           `json:"max_lag_client_id"`
	MaxLagConsumer    Relationship     `json:"max_lag_consumer"`
	MaxLagConsumerId  string           `json:"max_lag_consumer_id"`
	MaxLagInstanceId  *string          `json:"max_lag_instance_id,omitempty"`
	MaxLagPartition   Relationship     `json:"max_lag_partition"`
	MaxLagPartitionId int              `json:"max_lag_partition_id"`
	MaxLagTopicName   string           `json:"max_lag_topic_name"`
	Metadata          ResourceMetadata `json:"metadata"`
	TotalLag          int64            `json:"total_lag"`
}
type ConsumerGroupState = string
type ConsumerGroupType = string
type ConsumerLagData struct {
	ClientId        string           `json:"client_id"`
	ClusterId       string           `json:"cluster_id"`
	ConsumerGroupId string           `json:"consumer_group_id"`
	ConsumerId      string           `json:"consumer_id"`
	CurrentOffset   int64            `json:"current_offset"`
	InstanceId      *string          `json:"instance_id,omitempty"`
	Kind            string           `json:"kind"`
	Lag             int64            `json:"lag"`
	LogEndOffset    int64            `json:"log_end_offset"`
	Metadata        ResourceMetadata `json:"metadata"`
	PartitionId     int              `json:"partition_id"`
	TopicName       string           `json:"topic_name"`
}
type ConsumerLagDataList struct {
	Data     []ConsumerLagData          `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type CreateAclRequestData struct {
	Host         string          `json:"host"`
	Operation    AclOperation    `json:"operation"`
	PatternType  AclPatternType  `json:"pattern_type"`
	Permission   AclPermission   `json:"permission"`
	Principal    string          `json:"principal"`
	ResourceName string          `json:"resource_name"`
	ResourceType AclResourceType `json:"resource_type"`
}
type CreateAclRequestDataList struct {
	Data []CreateAclRequestData `json:"data"`
}
type CreateLinkRequestData struct {
	// ClusterLinkId The expected cluster link ID. Can be provided when creating the second side of a bidirectional link for validating the link ID is as expected. If it's not provided, it's inferred from the remote cluster.
	ClusterLinkId        *string       `json:"cluster_link_id,omitempty"`
	Configs              *[]ConfigData `json:"configs,omitempty"`
	DestinationClusterId *string       `json:"destination_cluster_id,omitempty"`

	// RemoteClusterId The expected remote cluster ID.
	RemoteClusterId *string `json:"remote_cluster_id,omitempty"`
	SourceClusterId *string `json:"source_cluster_id,omitempty"`
}
type CreateMirrorTopicRequestData struct {
	Configs           *[]ConfigData `json:"configs,omitempty"`
	MirrorTopicName   *string       `json:"mirror_topic_name,omitempty"`
	ReplicationFactor *int          `json:"replication_factor,omitempty"`
	SourceTopicName   string        `json:"source_topic_name"`
}
type CreateTopicRequestData struct {
	Configs *[]struct {
		Name  string  `json:"name"`
		Value *string `json:"value,omitempty"`
	} `json:"configs,omitempty"`
	PartitionsCount   *int   `json:"partitions_count,omitempty"`
	ReplicationFactor *int   `json:"replication_factor,omitempty"`
	TopicName         string `json:"topic_name"`
	ValidateOnly      *bool  `json:"validate_only,omitempty"`
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
type GroupConfigData struct {
	ClusterId   string              `json:"cluster_id"`
	GroupId     string              `json:"group_id"`
	IsDefault   bool                `json:"is_default"`
	IsReadOnly  bool                `json:"is_read_only"`
	IsSensitive bool                `json:"is_sensitive"`
	Kind        string              `json:"kind"`
	Metadata    ResourceMetadata    `json:"metadata"`
	Name        string              `json:"name"`
	Source      ConfigSource        `json:"source"`
	Synonyms    []ConfigSynonymData `json:"synonyms"`
	Value       *string             `json:"value,omitempty"`
}
type GroupConfigDataList struct {
	Data     []GroupConfigData          `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type LinkCategory struct {
	Count         int32         `json:"count"`
	StateCategory StateCategory `json:"state_category"`
}
type LinkError = string
type LinkState = string
type LinkTask struct {
	Errors   []LinkTaskError `json:"errors"`
	State    LinkTaskState   `json:"state"`
	TaskName string          `json:"task_name"`
}
type LinkTaskError struct {
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}
type LinkTaskState = string
type ListLinkConfigsResponseData struct {
	ClusterId   string           `json:"cluster_id"`
	IsDefault   bool             `json:"is_default"`
	IsReadOnly  bool             `json:"is_read_only"`
	IsSensitive bool             `json:"is_sensitive"`
	Kind        string           `json:"kind"`
	LinkName    string           `json:"link_name"`
	Metadata    ResourceMetadata `json:"metadata"`
	Name        string           `json:"name"`
	Source      string           `json:"source"`
	Synonyms    []string         `json:"synonyms"`
	Value       string           `json:"value"`
}
type ListLinkConfigsResponseDataList struct {
	Data     []ListLinkConfigsResponseData `json:"data"`
	Kind     string                        `json:"kind"`
	Metadata ResourceCollectionMetadata    `json:"metadata"`
}
type ListLinksResponseData struct {
	CategoryCounts       *[]LinkCategory `json:"category_counts,omitempty"`
	ClusterLinkId        string          `json:"cluster_link_id"`
	DestinationClusterId *string         `json:"destination_cluster_id,omitempty"`
	Kind                 string          `json:"kind"`
	LinkError            *LinkError      `json:"link_error,omitempty"`
	LinkErrorMessage     *string         `json:"link_error_message,omitempty"`
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	LinkId          *string          `json:"link_id,omitempty"`
	LinkName        string           `json:"link_name"`
	LinkState       *LinkState       `json:"link_state,omitempty"`
	Metadata        ResourceMetadata `json:"metadata"`
	RemoteClusterId *string          `json:"remote_cluster_id,omitempty"`
	SourceClusterId *string          `json:"source_cluster_id,omitempty"`
	Tasks           *[]LinkTask      `json:"tasks,omitempty"`
	TopicNames      []string         `json:"topic_names"`
}
type ListLinksResponseDataList struct {
	Data     []ListLinksResponseData    `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ListMirrorTopicsResponseData struct {
	Kind                        string            `json:"kind"`
	LinkName                    string            `json:"link_name"`
	Metadata                    ResourceMetadata  `json:"metadata"`
	MirrorLags                  MirrorLags        `json:"mirror_lags"`
	MirrorStateTransitionErrors *[]LinkTaskError  `json:"mirror_state_transition_errors,omitempty"`
	MirrorStatus                MirrorTopicStatus `json:"mirror_status"`
	MirrorTopicError            *MirrorTopicError `json:"mirror_topic_error,omitempty"`
	MirrorTopicName             string            `json:"mirror_topic_name"`
	NumPartitions               int               `json:"num_partitions"`
	SourceTopicName             string            `json:"source_topic_name"`
	StateTimeMs                 int64             `json:"state_time_ms"`
}
type ListMirrorTopicsResponseDataList struct {
	Data     []ListMirrorTopicsResponseData `json:"data"`
	Kind     string                         `json:"kind"`
	Metadata ResourceCollectionMetadata     `json:"metadata"`
}
type MirrorLag struct {
	Lag                   int64 `json:"lag"`
	LastSourceFetchOffset int64 `json:"last_source_fetch_offset"`
	Partition             int   `json:"partition"`
}
type MirrorLags = []MirrorLag
type MirrorTopicError = string
type MirrorTopicStatus string
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
type PartitionData struct {
	ClusterId    string           `json:"cluster_id"`
	Kind         string           `json:"kind"`
	Leader       *Relationship    `json:"leader,omitempty"`
	Metadata     ResourceMetadata `json:"metadata"`
	PartitionId  int              `json:"partition_id"`
	Reassignment Relationship     `json:"reassignment"`
	Replicas     Relationship     `json:"replicas"`
	TopicName    string           `json:"topic_name"`
}
type PartitionDataList struct {
	Data     []PartitionData            `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type PartitionLevelTruncationData struct {
	MessagesTruncated int64 `json:"messages_truncated"`
	OffsetTruncatedTo int64 `json:"offset_truncated_to"`
	PartitionId       int   `json:"partition_id"`
}
type PartitionLevelTruncationDataList = []PartitionLevelTruncationData
type ProduceRequest struct {
	Headers     *[]ProduceRequestHeader `json:"headers,omitempty"`
	Key         *ProduceRequestData     `json:"key,omitempty"`
	PartitionId *int32                  `json:"partition_id,omitempty"`
	Timestamp   *time.Time              `json:"timestamp,omitempty"`
	Value       *ProduceRequestData     `json:"value,omitempty"`
}
type ProduceRequestData struct {
	Data *AnyValue `json:"data,omitempty"`
	Type *string   `json:"type,omitempty"`
}
type ProduceRequestHeader struct {
	Name  string  `json:"name"`
	Value *[]byte `json:"value,omitempty"`
}
type ProduceResponse struct {
	ClusterId   *string              `json:"cluster_id,omitempty"`
	ErrorCode   int32                `json:"error_code"`
	Key         *ProduceResponseData `json:"key,omitempty"`
	Message     *string              `json:"message,omitempty"`
	Offset      *int64               `json:"offset,omitempty"`
	PartitionId *int32               `json:"partition_id,omitempty"`
	Timestamp   *time.Time           `json:"timestamp,omitempty"`
	TopicName   *string              `json:"topic_name,omitempty"`
	Value       *ProduceResponseData `json:"value,omitempty"`
}
type ProduceResponseData struct {
	Size int    `json:"size"`
	Type string `json:"type"`
}
type Relationship struct {
	Related string `json:"related"`
}
type Resource struct {
	Kind     string           `json:"kind"`
	Metadata ResourceMetadata `json:"metadata"`
}
type ResourceCollection struct {
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ResourceCollectionMetadata struct {
	Next *string `json:"next,omitempty"`
	Self string  `json:"self"`
}
type ResourceMetadata struct {
	ResourceName *string `json:"resource_name,omitempty"`
	Self         string  `json:"self"`
}
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type ShareGroupConsumerAssignmentData struct {
	ClusterId   string           `json:"cluster_id"`
	ConsumerId  string           `json:"consumer_id"`
	GroupId     string           `json:"group_id"`
	Kind        string           `json:"kind"`
	Metadata    ResourceMetadata `json:"metadata"`
	Partition   Relationship     `json:"partition"`
	PartitionId int              `json:"partition_id"`
	TopicName   string           `json:"topic_name"`
}
type ShareGroupConsumerAssignmentDataList struct {
	Data     []ShareGroupConsumerAssignmentData `json:"data"`
	Kind     string                             `json:"kind"`
	Metadata ResourceCollectionMetadata         `json:"metadata"`
}
type ShareGroupConsumerData struct {
	Assignments Relationship     `json:"assignments"`
	ClientId    string           `json:"client_id"`
	ClusterId   string           `json:"cluster_id"`
	ConsumerId  string           `json:"consumer_id"`
	GroupId     string           `json:"group_id"`
	Kind        string           `json:"kind"`
	Metadata    ResourceMetadata `json:"metadata"`
}
type ShareGroupConsumerDataList struct {
	Data     []ShareGroupConsumerData   `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ShareGroupData struct {
	// AssignedTopicPartitions List of topic-partitions assigned to this share group, including those from empty groups
	AssignedTopicPartitions *[]ShareGroupTopicPartitionData `json:"assigned_topic_partitions,omitempty"`
	ClusterId               string                          `json:"cluster_id"`

	// ConsumerCount Number of consumers in this share group
	ConsumerCount int32            `json:"consumer_count"`
	Consumers     Relationship     `json:"consumers"`
	Coordinator   Relationship     `json:"coordinator"`
	Kind          string           `json:"kind"`
	Metadata      ResourceMetadata `json:"metadata"`

	// PartitionCount Total number of partitions assigned to this share group across all consumers
	PartitionCount int32           `json:"partition_count"`
	ShareGroupId   string          `json:"share_group_id"`
	State          ShareGroupState `json:"state"`
}
type ShareGroupDataList struct {
	Data     []ShareGroupData           `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type ShareGroupState = string
type ShareGroupTopicPartitionData struct {
	Kind      string           `json:"kind"`
	Metadata  ResourceMetadata `json:"metadata"`
	Partition Relationship     `json:"partition"`

	// PartitionId The partition ID
	PartitionId int32 `json:"partition_id"`

	// TopicName The name of the topic
	TopicName string `json:"topic_name"`
}
type StateCategory = string
type StreamsGroupData struct {
	// ClusterId The unique identifier of the Kafka cluster.
	ClusterId string `json:"cluster_id"`

	// GroupEpoch The epoch of the Streams group.
	GroupEpoch int `json:"group_epoch"`

	// GroupId The unique identifier of the Streams group.
	GroupId string `json:"group_id"`
	Kind    string `json:"kind"`

	// MemberCount The number of members in the Streams group.
	MemberCount int              `json:"member_count"`
	Members     Relationship     `json:"members"`
	Metadata    ResourceMetadata `json:"metadata"`

	// State The state of the Streams group.
	State         StreamsGroupState `json:"state"`
	Subtopologies Relationship      `json:"subtopologies"`

	// SubtopologyCount The number of subtopologies in the Streams group.
	SubtopologyCount int `json:"subtopology_count"`

	// TargetAssignmentEpoch The epoch of the target assignment.
	TargetAssignmentEpoch int `json:"target_assignment_epoch"`

	// TopologyEpoch The epoch of the Streams topology.
	TopologyEpoch int `json:"topology_epoch"`
}
type StreamsGroupDataList struct {
	// Data The array of Streams group details.
	Data     []StreamsGroupData         `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type StreamsGroupMemberAssignmentData struct {
	ActiveTasks Relationship `json:"active_tasks"`

	// ClusterId The unique identifier of the Kafka cluster.
	ClusterId string `json:"cluster_id"`

	// GroupId The unique identifier of the Streams group.
	GroupId string `json:"group_id"`
	Kind    string `json:"kind"`

	// MemberId The unique identifier of the Streams group member.
	MemberId     string           `json:"member_id"`
	Metadata     ResourceMetadata `json:"metadata"`
	StandbyTasks Relationship     `json:"standby_tasks"`
	WarmupTasks  Relationship     `json:"warmup_tasks"`
}
type StreamsGroupMemberData struct {
	Assignments Relationship `json:"assignments"`

	// ClientId The client identifier of the Streams group member.
	ClientId string `json:"client_id"`

	// ClusterId The unique identifier of the Kafka cluster.
	ClusterId string `json:"cluster_id"`

	// GroupId The unique identifier of the Streams group.
	GroupId string `json:"group_id"`

	// InstanceId The instance identifier of the Streams group member.
	InstanceId string `json:"instance_id"`

	// IsClassic The flag indicating if the member is a classic consumer.
	IsClassic bool   `json:"is_classic"`
	Kind      string `json:"kind"`

	// MemberEpoch The epoch of the Streams group member.
	MemberEpoch int `json:"member_epoch"`

	// MemberId The unique identifier of the Streams group member.
	MemberId string           `json:"member_id"`
	Metadata ResourceMetadata `json:"metadata"`

	// ProcessId The process identifier of the Streams group member.
	ProcessId        string       `json:"process_id"`
	TargetAssignment Relationship `json:"target_assignment"`

	// TopologyEpoch The epoch of the Streams topology for the member.
	TopologyEpoch int `json:"topology_epoch"`
}
type StreamsGroupMemberDataList struct {
	// Data The array of Streams group member details.
	Data     []StreamsGroupMemberData   `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type StreamsGroupState = string
type StreamsGroupSubtopologyData struct {
	// ClusterId The unique identifier of the Kafka cluster.
	ClusterId string `json:"cluster_id"`

	// GroupId The unique identifier of the Streams group.
	GroupId  string           `json:"group_id"`
	Kind     string           `json:"kind"`
	Metadata ResourceMetadata `json:"metadata"`

	// SourceTopics The list of source topics for the subtopology.
	SourceTopics []string `json:"source_topics"`

	// SubtopologyId The unique identifier of the Streams subtopology.
	SubtopologyId string `json:"subtopology_id"`
}
type StreamsGroupSubtopologyDataList struct {
	// Data The array of Streams group subtopology details.
	Data     []StreamsGroupSubtopologyData `json:"data"`
	Kind     string                        `json:"kind"`
	Metadata ResourceCollectionMetadata    `json:"metadata"`
}
type StreamsTaskData struct {
	Kind     string           `json:"kind"`
	Metadata ResourceMetadata `json:"metadata"`

	// PartitionIds The list of partition IDs assigned to the Streams task.
	PartitionIds []int `json:"partition_ids"`

	// SubtopologyId The unique identifier of the Streams subtopology.
	SubtopologyId string `json:"subtopology_id"`
}
type StreamsTaskDataList struct {
	// Data The array of Streams task details.
	Data     []StreamsTaskData          `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type TopicConfigData struct {
	ClusterId   string              `json:"cluster_id"`
	IsDefault   bool                `json:"is_default"`
	IsReadOnly  bool                `json:"is_read_only"`
	IsSensitive bool                `json:"is_sensitive"`
	Kind        string              `json:"kind"`
	Metadata    ResourceMetadata    `json:"metadata"`
	Name        string              `json:"name"`
	Source      ConfigSource        `json:"source"`
	Synonyms    []ConfigSynonymData `json:"synonyms"`
	TopicName   string              `json:"topic_name"`
	Value       *string             `json:"value,omitempty"`
}
type TopicConfigDataList struct {
	Data     []TopicConfigData          `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type TopicData struct {
	AuthorizedOperations   *AuthorizedOperations `json:"authorized_operations,omitempty"`
	ClusterId              string                `json:"cluster_id"`
	Configs                Relationship          `json:"configs"`
	IsInternal             bool                  `json:"is_internal"`
	Kind                   string                `json:"kind"`
	Metadata               ResourceMetadata      `json:"metadata"`
	PartitionReassignments Relationship          `json:"partition_reassignments"`
	Partitions             Relationship          `json:"partitions"`
	PartitionsCount        int                   `json:"partitions_count"`
	ReplicationFactor      int                   `json:"replication_factor"`
	TopicName              string                `json:"topic_name"`
}
type TopicDataList struct {
	Data     []TopicData                `json:"data"`
	Kind     string                     `json:"kind"`
	Metadata ResourceCollectionMetadata `json:"metadata"`
}
type UpdateConfigRequestData struct {
	Value *string `json:"value,omitempty"`
}
type UpdateGroupConfigRequestData struct {
	Value string `json:"value"`
}
type UpdateLinkConfigRequestData struct {
	Value string `json:"value"`
}
type UpdatePartitionCountRequestData struct {
	PartitionsCount int32 `json:"partitions_count"`
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
type AclHost = string
type AclOperationRequired = AclOperation
type AclPatternTypeRequired = AclPatternType
type AclPermissionRequired = AclPermission
type AclPrincipal = string
type AclResourceName = string
type AclResourceTypeRequired = AclResourceType
type ClusterId = string
type ConfigName = string
type ConsumerGroupId = string
type ConsumerId = string
type Force = bool
type GroupId = string
type IncludeAuthorizedOperations = bool
type IncludePartitionLevelTruncationData = bool
type IncludeStateTransitionErrors = bool
type IncludeTasks = bool
type LinkConfigName = string
type LinkName = string
type MemberId = string
type MirrorTopicName = string
type PartitionId = int
type QueryParamLinkName = string
type SubtopologyId = string
type TopicName = string
type ValidateLink = bool
type ValidateOnly = bool
type AlterMirrorStatusResponse = AlterMirrorStatusResponseDataList
type BadRequestErrorResponse = Error
type BadRequestErrorResponseCreateAcls = Error
type BadRequestErrorResponseCreateTopic = Error
type BadRequestErrorResponseDeleteAcls = Error
type BadRequestErrorResponseProduceRecords = Error
type BadRequestErrorResponseUpdatePartitionCountTopic = Error
type CreateTopicResponse = TopicData
type DeleteAclsResponse struct {
	Data []AclData `json:"data"`
}
type DescribeMirrorTopicResponse = ListMirrorTopicsResponseData
type ForbiddenErrorResponse = Error
type GetClusterConfigResponse = ClusterConfigData
type GetClusterResponse = ClusterData
type GetConsumerGroupLagSummaryResponse = ConsumerGroupLagSummaryData
type GetConsumerGroupResponse = ConsumerGroupData
type GetConsumerLagResponse = ConsumerLagData
type GetConsumerResponse = ConsumerData
type GetGroupConfigResponse = GroupConfigData
type GetLinkConfigsResponse = ListLinkConfigsResponseData
type GetLinkResponse = ListLinksResponseData
type GetPartitionResponse = PartitionData
type GetShareGroupConsumerResponse = ShareGroupConsumerData
type GetShareGroupResponse = ShareGroupData
type GetStreamsGroupMemberAssignmentsResponse = StreamsGroupMemberAssignmentData
type GetStreamsGroupMemberResponse = StreamsGroupMemberData
type GetStreamsGroupResponse = StreamsGroupData
type GetStreamsGroupSubtopologyResponse = StreamsGroupSubtopologyData
type GetStreamsTaskResponse = StreamsTaskData
type GetTopicConfigResponse = TopicConfigData
type GetTopicResponse = TopicData
type ListClusterConfigsResponse = ClusterConfigDataList
type ListConsumerGroupsResponse = ConsumerGroupDataList
type ListConsumerLagsResponse = ConsumerLagDataList
type ListConsumersResponse = ConsumerDataList
type ListGroupConfigsResponse = GroupConfigDataList
type ListLinkConfigsResponse = ListLinkConfigsResponseDataList
type ListLinksResponse = ListLinksResponseDataList
type ListMirrorTopicsResponse = ListMirrorTopicsResponseDataList
type ListPartitionsResponse = PartitionDataList
type ListShareGroupConsumerAssignmentsResponse = ShareGroupConsumerAssignmentDataList
type ListShareGroupConsumersResponse = ShareGroupConsumerDataList
type ListShareGroupsResponse = ShareGroupDataList
type ListStreamsGroupMembersResponse = StreamsGroupMemberDataList
type ListStreamsGroupSubtopologiesResponse = StreamsGroupSubtopologyDataList
type ListStreamsGroupsResponse = StreamsGroupDataList
type ListStreamsTasksResponse = StreamsTaskDataList
type ListTopicConfigsResponse = TopicConfigDataList
type ListTopicsResponse = TopicDataList
type NotFoundErrorResponse = Error
type RequestEntityTooLargeErrorResponse = Error
type SearchAclsResponse = AclDataList
type ServerErrorResponse = Error
type UnauthorizedErrorResponse = Error
type UnprocessableEntityProduceRecord = Error
type UnsupportedMediaTypeErrorResponse = Error
type AlterClusterConfigBatchRequest = AlterConfigBatchRequestData
type AlterGroupConfigBatchRequest = AlterConfigBatchRequestData
type AlterLinkConfigBatchRequest = AlterConfigBatchRequestData
type AlterMirrorsRequest = AlterMirrorsRequestData
type AlterTopicConfigBatchRequest = AlterConfigBatchRequestData
type BatchCreateAclRequest = CreateAclRequestDataList
type CreateAclRequest = CreateAclRequestData
type CreateLinkRequest = CreateLinkRequestData
type CreateMirrorTopicRequest = CreateMirrorTopicRequestData
type CreateTopicRequest = CreateTopicRequestData
type UpdateClusterConfigRequest = UpdateConfigRequestData
type UpdateGroupConfigRequest = UpdateGroupConfigRequestData
type UpdateLinkConfigRequest = UpdateLinkConfigRequestData
type UpdateTopicConfigRequest = UpdateConfigRequestData

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

	// GetKafkaCluster Get Cluster
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the Kafka cluster with the specified ``cluster_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id} (the `GetKafkaCluster` operationId).
	GetKafkaCluster(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaAcls Delete ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the ACLs that match the search criteria.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/acls (the `DeleteKafkaAcls` operationId).
	DeleteKafkaAcls(ctx context.Context, clusterId ClusterId, params *DeleteKafkaAclsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaAcls List ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// - When calling `/acls` without the `principal` parameter, service
	//   accounts are returned in numeric ID format (e.g., `User:12345`).
	// - To retrieve service accounts in the `sa-xxx` format, use
	//   `/acls?principal=UserV2:*`.
	// - The `principal` parameter supports both legacy `User:` format and
	//   new `UserV2:` format for service accounts.
	// Return a list of ACLs that match the search criteria.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/acls (the `GetKafkaAcls` operationId).
	GetKafkaAcls(ctx context.Context, clusterId ClusterId, params *GetKafkaAclsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaAclsWithBody Create an ACL
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create an ACL.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls (the `CreateKafkaAcls` operationId).
	CreateKafkaAclsWithBody(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaAcls Create an ACL
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create an ACL.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls (the `CreateKafkaAcls` operationId).
	CreateKafkaAcls(ctx context.Context, clusterId ClusterId, body CreateKafkaAclsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// BatchCreateKafkaAclsWithBody Batch Create ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create ACLs.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls:batch (the `BatchCreateKafkaAcls` operationId).
	BatchCreateKafkaAclsWithBody(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// BatchCreateKafkaAcls Batch Create ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create ACLs.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls:batch (the `BatchCreateKafkaAcls` operationId).
	BatchCreateKafkaAcls(ctx context.Context, clusterId ClusterId, body BatchCreateKafkaAclsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaClusterConfigs List Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of dynamic cluster-wide broker configuration parameters for the specified Kafka
	// cluster. Returns an empty list if there are no dynamic cluster-wide broker configuration parameters.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/broker-configs (the `ListKafkaClusterConfigs` operationId).
	ListKafkaClusterConfigs(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaClusterConfig Reset Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Reset the configuration parameter specified by ``name`` to its
	// default value by deleting a dynamic cluster-wide configuration.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `DeleteKafkaClusterConfig` operationId).
	DeleteKafkaClusterConfig(ctx context.Context, clusterId ClusterId, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaClusterConfig Get Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `GetKafkaClusterConfig` operationId).
	GetKafkaClusterConfig(ctx context.Context, clusterId ClusterId, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaClusterConfigWithBody Update Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `UpdateKafkaClusterConfig` operationId).
	UpdateKafkaClusterConfigWithBody(ctx context.Context, clusterId ClusterId, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaClusterConfig Update Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `UpdateKafkaClusterConfig` operationId).
	UpdateKafkaClusterConfig(ctx context.Context, clusterId ClusterId, name ConfigName, body UpdateKafkaClusterConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaClusterConfigsWithBody Batch Alter Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of dynamic cluster-wide broker configuration parameters.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/broker-configs:alter (the `UpdateKafkaClusterConfigs` operationId).
	UpdateKafkaClusterConfigsWithBody(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaClusterConfigs Batch Alter Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of dynamic cluster-wide broker configuration parameters.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/broker-configs:alter (the `UpdateKafkaClusterConfigs` operationId).
	UpdateKafkaClusterConfigs(ctx context.Context, clusterId ClusterId, body UpdateKafkaClusterConfigsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaConsumerGroups List Consumer Groups
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of consumer groups that belong to the specified
	// Kafka cluster.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups (the `ListKafkaConsumerGroups` operationId).
	ListKafkaConsumerGroups(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaConsumerGroup Get Consumer Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the consumer group specified by the ``consumer_group_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id} (the `GetKafkaConsumerGroup` operationId).
	GetKafkaConsumerGroup(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaConsumers List Consumers
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of consumers that belong to the specified consumer
	// group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/consumers (the `ListKafkaConsumers` operationId).
	ListKafkaConsumers(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaConsumer Get Consumer
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the consumer specified by the ``consumer_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/consumers/{consumer_id} (the `GetKafkaConsumer` operationId).
	GetKafkaConsumer(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaConsumerGroupLagSummary Get Consumer Group Lag Summary
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return the maximum and total lag of the consumers belonging to the
	// specified consumer group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lag-summary (the `GetKafkaConsumerGroupLagSummary` operationId).
	GetKafkaConsumerGroupLagSummary(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaConsumerLags List Consumer Lags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return a list of consumer lags of the consumers belonging to the
	// specified consumer group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lags (the `ListKafkaConsumerLags` operationId).
	ListKafkaConsumerLags(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaConsumerLag Get Consumer Lag
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return the consumer lag on a partition with the given `partition_id`.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lags/{topic_name}/partitions/{partition_id} (the `GetKafkaConsumerLag` operationId).
	GetKafkaConsumerLag(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, topicName TopicName, partitionId PartitionId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaGroupConfigs List all configs of the group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs (the `ListKafkaGroupConfigs` operationId).
	ListKafkaGroupConfigs(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaGroupConfig Delete group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the dynamic configuration override with the specified name for the specified group. After deletion, the default group configuration will be applied. This API supports consumer groups, share groups, and streams groups.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `DeleteKafkaGroupConfig` operationId).
	DeleteKafkaGroupConfig(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaGroupConfig Get group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `GetKafkaGroupConfig` operationId).
	GetKafkaGroupConfig(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaGroupConfigWithBody Update group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `UpdateKafkaGroupConfig` operationId).
	UpdateKafkaGroupConfigWithBody(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaGroupConfig Update group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `UpdateKafkaGroupConfig` operationId).
	UpdateKafkaGroupConfig(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, body UpdateKafkaGroupConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaGroupConfigBatchWithBody Batch Alter Group Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch alter configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs:alter (the `UpdateKafkaGroupConfigBatch` operationId).
	UpdateKafkaGroupConfigBatchWithBody(ctx context.Context, clusterId ClusterId, groupId GroupId, params *UpdateKafkaGroupConfigBatchParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaGroupConfigBatch Batch Alter Group Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch alter configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs:alter (the `UpdateKafkaGroupConfigBatch` operationId).
	UpdateKafkaGroupConfigBatch(ctx context.Context, clusterId ClusterId, groupId GroupId, params *UpdateKafkaGroupConfigBatchParams, body UpdateKafkaGroupConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaLinks List all cluster links in the dest cluster
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// ``link_id`` in ``ListLinksResponseData`` is deprecated and may be removed in a future release. Use the new ``cluster_link_id`` instead.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links (the `ListKafkaLinks` operationId).
	ListKafkaLinks(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaLinkWithBody Create a cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Cluster link creation requires source cluster security configurations in
	// the configs JSON section of the data request payload.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links (the `CreateKafkaLink` operationId).
	CreateKafkaLinkWithBody(ctx context.Context, clusterId ClusterId, params *CreateKafkaLinkParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaLink Create a cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Cluster link creation requires source cluster security configurations in
	// the configs JSON section of the data request payload.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links (the `CreateKafkaLink` operationId).
	CreateKafkaLink(ctx context.Context, clusterId ClusterId, params *CreateKafkaLinkParams, body CreateKafkaLinkJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaMirrorTopics List mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all mirror topics in the cluster
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/-/mirrors (the `ListKafkaMirrorTopics` operationId).
	ListKafkaMirrorTopics(ctx context.Context, clusterId ClusterId, params *ListKafkaMirrorTopicsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaLink Delete the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/links/{link_name} (the `DeleteKafkaLink` operationId).
	DeleteKafkaLink(ctx context.Context, clusterId ClusterId, linkName LinkName, params *DeleteKafkaLinkParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaLink Describe the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// ``link_id`` in ``ListLinksResponseData`` is deprecated and may be removed in a future release. Use the new ``cluster_link_id`` instead.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name} (the `GetKafkaLink` operationId).
	GetKafkaLink(ctx context.Context, clusterId ClusterId, linkName LinkName, params *GetKafkaLinkParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaLinkConfigs List all configs of the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs (the `ListKafkaLinkConfigs` operationId).
	ListKafkaLinkConfigs(ctx context.Context, clusterId ClusterId, linkName LinkName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaLinkConfig Reset the given config to default value
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `DeleteKafkaLinkConfig` operationId).
	DeleteKafkaLinkConfig(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaLinkConfigs Describe the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `GetKafkaLinkConfigs` operationId).
	GetKafkaLinkConfigs(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaLinkConfigWithBody Alter the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `UpdateKafkaLinkConfig` operationId).
	UpdateKafkaLinkConfigWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaLinkConfig Alter the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `UpdateKafkaLinkConfig` operationId).
	UpdateKafkaLinkConfig(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, body UpdateKafkaLinkConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaLinkConfigBatchWithBody Batch Alter Cluster Link Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch Alter Cluster Link Configs
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs:alter (the `UpdateKafkaLinkConfigBatch` operationId).
	UpdateKafkaLinkConfigBatchWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaLinkConfigBatchParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaLinkConfigBatch Batch Alter Cluster Link Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch Alter Cluster Link Configs
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs:alter (the `UpdateKafkaLinkConfigBatch` operationId).
	UpdateKafkaLinkConfigBatch(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaLinkConfigBatchParams, body UpdateKafkaLinkConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaMirrorTopicsUnderLink List mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all mirror topics under the link
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `ListKafkaMirrorTopicsUnderLink` operationId).
	ListKafkaMirrorTopicsUnderLink(ctx context.Context, clusterId ClusterId, linkName LinkName, params *ListKafkaMirrorTopicsUnderLinkParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaMirrorTopicWithBody Create a mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic in the destination cluster mirroring a topic in
	// the source cluster
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `CreateKafkaMirrorTopic` operationId).
	CreateKafkaMirrorTopicWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaMirrorTopic Create a mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic in the destination cluster mirroring a topic in
	// the source cluster
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `CreateKafkaMirrorTopic` operationId).
	CreateKafkaMirrorTopic(ctx context.Context, clusterId ClusterId, linkName LinkName, body CreateKafkaMirrorTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ReadKafkaMirrorTopic Describe the mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors/{mirror_topic_name} (the `ReadKafkaMirrorTopic` operationId).
	ReadKafkaMirrorTopic(ctx context.Context, clusterId ClusterId, linkName LinkName, mirrorTopicName MirrorTopicName, params *ReadKafkaMirrorTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsFailoverWithBody Failover the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:failover (the `UpdateKafkaMirrorTopicsFailover` operationId).
	UpdateKafkaMirrorTopicsFailoverWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsFailoverParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsFailover Failover the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:failover (the `UpdateKafkaMirrorTopicsFailover` operationId).
	UpdateKafkaMirrorTopicsFailover(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsFailoverParams, body UpdateKafkaMirrorTopicsFailoverJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsPauseWithBody Pause the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:pause (the `UpdateKafkaMirrorTopicsPause` operationId).
	UpdateKafkaMirrorTopicsPauseWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPauseParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsPause Pause the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:pause (the `UpdateKafkaMirrorTopicsPause` operationId).
	UpdateKafkaMirrorTopicsPause(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPauseParams, body UpdateKafkaMirrorTopicsPauseJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsPromoteWithBody Promote the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:promote (the `UpdateKafkaMirrorTopicsPromote` operationId).
	UpdateKafkaMirrorTopicsPromoteWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPromoteParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsPromote Promote the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:promote (the `UpdateKafkaMirrorTopicsPromote` operationId).
	UpdateKafkaMirrorTopicsPromote(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPromoteParams, body UpdateKafkaMirrorTopicsPromoteJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsResumeWithBody Resume the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:resume (the `UpdateKafkaMirrorTopicsResume` operationId).
	UpdateKafkaMirrorTopicsResumeWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsResumeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsResume Resume the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:resume (the `UpdateKafkaMirrorTopicsResume` operationId).
	UpdateKafkaMirrorTopicsResume(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsResumeParams, body UpdateKafkaMirrorTopicsResumeJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithBody Reverse the local mirror topic and Pause the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-pause-mirror (the `UpdateKafkaMirrorTopicsReverseAndPauseMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndPauseMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsReverseAndPauseMirror Reverse the local mirror topic and Pause the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-pause-mirror (the `UpdateKafkaMirrorTopicsReverseAndPauseMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndPauseMirror(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndPauseMirrorParams, body UpdateKafkaMirrorTopicsReverseAndPauseMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsReverseAndStartMirrorWithBody Reverse the local mirror topic and start the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-start-mirror (the `UpdateKafkaMirrorTopicsReverseAndStartMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndStartMirrorWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndStartMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsReverseAndStartMirror Reverse the local mirror topic and start the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-start-mirror (the `UpdateKafkaMirrorTopicsReverseAndStartMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndStartMirror(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndStartMirrorParams, body UpdateKafkaMirrorTopicsReverseAndStartMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithBody Truncates the local topic to the remote stopped mirror log end offsets and restores mirroring to the local topic to mirror from the remote topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:truncate-and-restore (the `UpdateKafkaMirrorTopicsTruncateAndRestoreMirror` operationId).
	UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithBody(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaMirrorTopicsTruncateAndRestoreMirror Truncates the local topic to the remote stopped mirror log end offsets and restores mirroring to the local topic to mirror from the remote topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:truncate-and-restore (the `UpdateKafkaMirrorTopicsTruncateAndRestoreMirror` operationId).
	UpdateKafkaMirrorTopicsTruncateAndRestoreMirror(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorParams, body UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaShareGroups List Share Groups
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the list of share groups that belong to the specified
	// Kafka cluster.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups (the `ListKafkaShareGroups` operationId).
	ListKafkaShareGroups(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaShareGroup Delete Share Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the share group specified by the ``group_id``.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/share-groups/{group_id} (the `DeleteKafkaShareGroup` operationId).
	DeleteKafkaShareGroup(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaShareGroup Get Share Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the share group specified by the ``group_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id} (the `GetKafkaShareGroup` operationId).
	GetKafkaShareGroup(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaShareGroupConsumers List Share Group Consumers
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of consumers that belong to the specified share
	// group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers (the `ListKafkaShareGroupConsumers` operationId).
	ListKafkaShareGroupConsumers(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaShareGroupConsumer Get Share Group Consumer
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the consumer specified by the ``consumer_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers/{consumer_id} (the `GetKafkaShareGroupConsumer` operationId).
	GetKafkaShareGroupConsumer(ctx context.Context, clusterId ClusterId, groupId GroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaShareGroupConsumerAssignments List Share Group Consumer Assignments
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the consumer assignments specified by the ``consumer_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers/{consumer_id}/assignments (the `ListKafkaShareGroupConsumerAssignments` operationId).
	ListKafkaShareGroupConsumerAssignments(ctx context.Context, clusterId ClusterId, groupId GroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaStreamsGroups List Streams Groups
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the list of streams groups that belong to the specified Kafka cluster
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups (the `ListKafkaStreamsGroups` operationId).
	ListKafkaStreamsGroups(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroup Get Streams Group
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the streams group specified by the ``group_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id} (the `GetKafkaStreamsGroup` operationId).
	GetKafkaStreamsGroup(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaStreamsGroupMembers List Streams Group Members
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of members that belong to the specified streams group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members (the `ListKafkaStreamsGroupMembers` operationId).
	ListKafkaStreamsGroupMembers(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupMember Get Streams Group Member
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the members specified by the ``member_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id} (the `GetKafkaStreamsGroupMember` operationId).
	GetKafkaStreamsGroupMember(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupMemberAssignments Get Streams Group Member Assignments
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the assignments of the member specified by the ``member_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments (the `GetKafkaStreamsGroupMemberAssignments` operationId).
	GetKafkaStreamsGroupMemberAssignments(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaStreamsGroupMemberAssignmentTasks List Streams Group Assignments of a Specific Type
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments/{assignments_type} (the `ListKafkaStreamsGroupMemberAssignmentTasks` operationId).
	ListKafkaStreamsGroupMemberAssignmentTasks(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupMemberAssignmentTaskPartitions List Streams Group Assignments Task Partitions of a Specific Type and Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments/{assignments_type}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupMemberAssignmentTaskPartitions` operationId).
	GetKafkaStreamsGroupMemberAssignmentTaskPartitions(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupMemberTargetAssignments Get Streams Group Member Target Assignments
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the target assignments of the member specified by the ``member_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments (the `GetKafkaStreamsGroupMemberTargetAssignments` operationId).
	GetKafkaStreamsGroupMemberTargetAssignments(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaStreamsGroupMemberTargetAssignmentTasks List Streams Group Target Assignments of a Specific Type
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the target tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments/{assignments_type} (the `ListKafkaStreamsGroupMemberTargetAssignmentTasks` operationId).
	ListKafkaStreamsGroupMemberTargetAssignmentTasks(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitions List Streams Group Target Assignments Task Partitions of a Specific Type and Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments/{assignments_type}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitions` operationId).
	GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitions(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaStreamsGroupSubtopologies List Streams Group Subtopologies
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of subtopologies that belong to the specified streams group.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/subtopologies (the `ListKafkaStreamsGroupSubtopologies` operationId).
	ListKafkaStreamsGroupSubtopologies(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaStreamsGroupSubtopology Get Streams Group Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the subtopology specified by the ``subtopology_id``.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupSubtopology` operationId).
	GetKafkaStreamsGroupSubtopology(ctx context.Context, clusterId ClusterId, groupId GroupId, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaTopics List Topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of topics that belong to the specified Kafka cluster.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics (the `ListKafkaTopics` operationId).
	ListKafkaTopics(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaTopicWithBody Create Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic.
	// Also supports a dry-run mode that only validates whether the topic creation would succeed
	// if the ``validate_only`` request property is explicitly specified and set to true. Note that
	// when dry-run mode is being used the response status would be 200 OK instead of 201 Created.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics (the `CreateKafkaTopic` operationId).
	CreateKafkaTopicWithBody(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateKafkaTopic Create Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic.
	// Also supports a dry-run mode that only validates whether the topic creation would succeed
	// if the ``validate_only`` request property is explicitly specified and set to true. Note that
	// when dry-run mode is being used the response status would be 200 OK instead of 201 Created.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics (the `CreateKafkaTopic` operationId).
	CreateKafkaTopic(ctx context.Context, clusterId ClusterId, body CreateKafkaTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaAllTopicConfigs List All Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of configuration parameters for all topics hosted by the specified
	// cluster.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/-/configs (the `ListKafkaAllTopicConfigs` operationId).
	ListKafkaAllTopicConfigs(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaTopic Delete Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the topic with the given `topic_name`.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `DeleteKafkaTopic` operationId).
	DeleteKafkaTopic(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaTopic Get Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the topic with the given `topic_name`.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `GetKafkaTopic` operationId).
	GetKafkaTopic(ctx context.Context, clusterId ClusterId, topicName TopicName, params *GetKafkaTopicParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdatePartitionCountKafkaTopicWithBody Update Partition Count
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Increase the number of partitions for a topic. To update other topic
	// configurations, see https://docs.confluent.io/cloud/current/api.html#tag/Configs-(v3)/operation/updateKafkaTopicConfig.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `UpdatePartitionCountKafkaTopic` operationId).
	UpdatePartitionCountKafkaTopicWithBody(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdatePartitionCountKafkaTopic Update Partition Count
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Increase the number of partitions for a topic. To update other topic
	// configurations, see https://docs.confluent.io/cloud/current/api.html#tag/Configs-(v3)/operation/updateKafkaTopicConfig.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `UpdatePartitionCountKafkaTopic` operationId).
	UpdatePartitionCountKafkaTopic(ctx context.Context, clusterId ClusterId, topicName TopicName, body UpdatePartitionCountKafkaTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaTopicConfigs List Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of configuration parameters that belong to the specified topic.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs (the `ListKafkaTopicConfigs` operationId).
	ListKafkaTopicConfigs(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteKafkaTopicConfig Reset Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Reset the configuration parameter with given `name` to its default value.
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `DeleteKafkaTopicConfig` operationId).
	DeleteKafkaTopicConfig(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaTopicConfig Get Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the configuration parameter with the given `name`.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `GetKafkaTopicConfig` operationId).
	GetKafkaTopicConfig(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaTopicConfigWithBody Update Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration parameter with given `name`. To update the
	// number of partitions, see
	// https://docs.confluent.io/cloud/current/api.html#tag/Topic-(v3)/operation/updatePartitionCountKafkaTopic.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `UpdateKafkaTopicConfig` operationId).
	UpdateKafkaTopicConfigWithBody(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaTopicConfig Update Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration parameter with given `name`. To update the
	// number of partitions, see
	// https://docs.confluent.io/cloud/current/api.html#tag/Topic-(v3)/operation/updatePartitionCountKafkaTopic.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `UpdateKafkaTopicConfig` operationId).
	UpdateKafkaTopicConfig(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, body UpdateKafkaTopicConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaTopicConfigBatchWithBody Batch Alter Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of topic configuration parameters.
	// Also supports a dry-run mode that only validates whether the operation would succeed if the
	// ``validate_only`` request property is explicitly specified and set to true.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter (the `UpdateKafkaTopicConfigBatch` operationId).
	UpdateKafkaTopicConfigBatchWithBody(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateKafkaTopicConfigBatch Batch Alter Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of topic configuration parameters.
	// Also supports a dry-run mode that only validates whether the operation would succeed if the
	// ``validate_only`` request property is explicitly specified and set to true.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter (the `UpdateKafkaTopicConfigBatch` operationId).
	UpdateKafkaTopicConfigBatch(ctx context.Context, clusterId ClusterId, topicName TopicName, body UpdateKafkaTopicConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaDefaultTopicConfigs List New Topic Default Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List the default configuration parameters used if the topic were to be newly created.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/default-configs (the `ListKafkaDefaultTopicConfigs` operationId).
	ListKafkaDefaultTopicConfigs(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListKafkaPartitions List Partitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of partitions that belong to the specified topic.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions (the `ListKafkaPartitions` operationId).
	ListKafkaPartitions(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetKafkaPartition Get Partition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the partition with the given `partition_id`.
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions/{partition_id} (the `GetKafkaPartition` operationId).
	GetKafkaPartition(ctx context.Context, clusterId ClusterId, topicName TopicName, partitionId PartitionId, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ProduceRecordWithBody Produce Records
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Produce records to the given topic, returning delivery reports for each
	// record produced. This API can be used in streaming mode by setting
	// "Transfer-Encoding: chunked" header. For as long as the connection is
	// kept open, the server will keep accepting records. Records are streamed
	// to and from the server as Concatenated JSON. For each record sent to the
	// server, the server will asynchronously send back a delivery report, in
	// the same order, each with its own error_code. An error_code of 200
	// indicates success. The HTTP status code will be HTTP 200 OK as long as
	// the connection is successfully established. To identify records that
	// have encountered an error, check the error_code of each delivery report.
	//
	// Note that the cluster_id is validated only when running in Confluent Cloud.
	//
	// This API currently does not support Schema Registry integration. Sending
	// schemas is not supported. Only BINARY, JSON, and STRING formats are
	// supported.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records (the `ProduceRecord` operationId).
	ProduceRecordWithBody(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ProduceRecord Produce Records
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Produce records to the given topic, returning delivery reports for each
	// record produced. This API can be used in streaming mode by setting
	// "Transfer-Encoding: chunked" header. For as long as the connection is
	// kept open, the server will keep accepting records. Records are streamed
	// to and from the server as Concatenated JSON. For each record sent to the
	// server, the server will asynchronously send back a delivery report, in
	// the same order, each with its own error_code. An error_code of 200
	// indicates success. The HTTP status code will be HTTP 200 OK as long as
	// the connection is successfully established. To identify records that
	// have encountered an error, check the error_code of each delivery report.
	//
	// Note that the cluster_id is validated only when running in Confluent Cloud.
	//
	// This API currently does not support Schema Registry integration. Sending
	// schemas is not supported. Only BINARY, JSON, and STRING formats are
	// supported.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records (the `ProduceRecord` operationId).
	ProduceRecord(ctx context.Context, clusterId ClusterId, topicName TopicName, body ProduceRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// GetKafkaClusterWithResponse Get Cluster
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the Kafka cluster with the specified ``cluster_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id} (the `GetKafkaCluster` operationId).
	GetKafkaClusterWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*GetKafkaClusterResponse, error)

	// DeleteKafkaAclsWithResponse Delete ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the ACLs that match the search criteria.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/acls (the `DeleteKafkaAcls` operationId).
	DeleteKafkaAclsWithResponse(ctx context.Context, clusterId ClusterId, params *DeleteKafkaAclsParams, reqEditors ...RequestEditorFn) (*DeleteKafkaAclsResponse, error)

	// GetKafkaAclsWithResponse List ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// - When calling `/acls` without the `principal` parameter, service
	//   accounts are returned in numeric ID format (e.g., `User:12345`).
	// - To retrieve service accounts in the `sa-xxx` format, use
	//   `/acls?principal=UserV2:*`.
	// - The `principal` parameter supports both legacy `User:` format and
	//   new `UserV2:` format for service accounts.
	// Return a list of ACLs that match the search criteria.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/acls (the `GetKafkaAcls` operationId).
	GetKafkaAclsWithResponse(ctx context.Context, clusterId ClusterId, params *GetKafkaAclsParams, reqEditors ...RequestEditorFn) (*GetKafkaAclsResponse, error)

	// CreateKafkaAclsWithBodyWithResponse Create an ACL
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create an ACL.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls (the `CreateKafkaAcls` operationId).
	CreateKafkaAclsWithBodyWithResponse(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKafkaAclsResponse, error)

	// CreateKafkaAclsWithResponse Create an ACL
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create an ACL.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls (the `CreateKafkaAcls` operationId).
	CreateKafkaAclsWithResponse(ctx context.Context, clusterId ClusterId, body CreateKafkaAclsJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKafkaAclsResponse, error)

	// BatchCreateKafkaAclsWithBodyWithResponse Batch Create ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create ACLs.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls:batch (the `BatchCreateKafkaAcls` operationId).
	BatchCreateKafkaAclsWithBodyWithResponse(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*BatchCreateKafkaAclsResponse, error)

	// BatchCreateKafkaAclsWithResponse Batch Create ACLs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create ACLs.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/acls:batch (the `BatchCreateKafkaAcls` operationId).
	BatchCreateKafkaAclsWithResponse(ctx context.Context, clusterId ClusterId, body BatchCreateKafkaAclsJSONRequestBody, reqEditors ...RequestEditorFn) (*BatchCreateKafkaAclsResponse, error)

	// ListKafkaClusterConfigsWithResponse List Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of dynamic cluster-wide broker configuration parameters for the specified Kafka
	// cluster. Returns an empty list if there are no dynamic cluster-wide broker configuration parameters.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/broker-configs (the `ListKafkaClusterConfigs` operationId).
	ListKafkaClusterConfigsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaClusterConfigsResponse, error)

	// DeleteKafkaClusterConfigWithResponse Reset Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Reset the configuration parameter specified by ``name`` to its
	// default value by deleting a dynamic cluster-wide configuration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `DeleteKafkaClusterConfig` operationId).
	DeleteKafkaClusterConfigWithResponse(ctx context.Context, clusterId ClusterId, name ConfigName, reqEditors ...RequestEditorFn) (*DeleteKafkaClusterConfigResponse, error)

	// GetKafkaClusterConfigWithResponse Get Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `GetKafkaClusterConfig` operationId).
	GetKafkaClusterConfigWithResponse(ctx context.Context, clusterId ClusterId, name ConfigName, reqEditors ...RequestEditorFn) (*GetKafkaClusterConfigResponse, error)

	// UpdateKafkaClusterConfigWithBodyWithResponse Update Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `UpdateKafkaClusterConfig` operationId).
	UpdateKafkaClusterConfigWithBodyWithResponse(ctx context.Context, clusterId ClusterId, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaClusterConfigResponse, error)

	// UpdateKafkaClusterConfigWithResponse Update Dynamic Broker Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the dynamic cluster-wide broker configuration parameter specified by ``name``.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/broker-configs/{name} (the `UpdateKafkaClusterConfig` operationId).
	UpdateKafkaClusterConfigWithResponse(ctx context.Context, clusterId ClusterId, name ConfigName, body UpdateKafkaClusterConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaClusterConfigResponse, error)

	// UpdateKafkaClusterConfigsWithBodyWithResponse Batch Alter Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of dynamic cluster-wide broker configuration parameters.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/broker-configs:alter (the `UpdateKafkaClusterConfigs` operationId).
	UpdateKafkaClusterConfigsWithBodyWithResponse(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaClusterConfigsResponse, error)

	// UpdateKafkaClusterConfigsWithResponse Batch Alter Dynamic Broker Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of dynamic cluster-wide broker configuration parameters.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/broker-configs:alter (the `UpdateKafkaClusterConfigs` operationId).
	UpdateKafkaClusterConfigsWithResponse(ctx context.Context, clusterId ClusterId, body UpdateKafkaClusterConfigsJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaClusterConfigsResponse, error)

	// ListKafkaConsumerGroupsWithResponse List Consumer Groups
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of consumer groups that belong to the specified
	// Kafka cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups (the `ListKafkaConsumerGroups` operationId).
	ListKafkaConsumerGroupsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaConsumerGroupsResponse, error)

	// GetKafkaConsumerGroupWithResponse Get Consumer Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the consumer group specified by the ``consumer_group_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id} (the `GetKafkaConsumerGroup` operationId).
	GetKafkaConsumerGroupWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*GetKafkaConsumerGroupResponse, error)

	// ListKafkaConsumersWithResponse List Consumers
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return a list of consumers that belong to the specified consumer
	// group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/consumers (the `ListKafkaConsumers` operationId).
	ListKafkaConsumersWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*ListKafkaConsumersResponse, error)

	// GetKafkaConsumerWithResponse Get Consumer
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the consumer specified by the ``consumer_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/consumers/{consumer_id} (the `GetKafkaConsumer` operationId).
	GetKafkaConsumerWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*GetKafkaConsumerResponse, error)

	// GetKafkaConsumerGroupLagSummaryWithResponse Get Consumer Group Lag Summary
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return the maximum and total lag of the consumers belonging to the
	// specified consumer group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lag-summary (the `GetKafkaConsumerGroupLagSummary` operationId).
	GetKafkaConsumerGroupLagSummaryWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*GetKafkaConsumerGroupLagSummaryResponse, error)

	// ListKafkaConsumerLagsWithResponse List Consumer Lags
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return a list of consumer lags of the consumers belonging to the
	// specified consumer group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lags (the `ListKafkaConsumerLags` operationId).
	ListKafkaConsumerLagsWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, reqEditors ...RequestEditorFn) (*ListKafkaConsumerLagsResponse, error)

	// GetKafkaConsumerLagWithResponse Get Consumer Lag
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Available in dedicated clusters only](https://img.shields.io/badge/-Available%20in%20dedicated%20clusters%20only-%23bc8540)](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#dedicated-cluster)
	//
	// Return the consumer lag on a partition with the given `partition_id`.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/consumer-groups/{consumer_group_id}/lags/{topic_name}/partitions/{partition_id} (the `GetKafkaConsumerLag` operationId).
	GetKafkaConsumerLagWithResponse(ctx context.Context, clusterId ClusterId, consumerGroupId ConsumerGroupId, topicName TopicName, partitionId PartitionId, reqEditors ...RequestEditorFn) (*GetKafkaConsumerLagResponse, error)

	// ListKafkaGroupConfigsWithResponse List all configs of the group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs (the `ListKafkaGroupConfigs` operationId).
	ListKafkaGroupConfigsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*ListKafkaGroupConfigsResponse, error)

	// DeleteKafkaGroupConfigWithResponse Delete group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the dynamic configuration override with the specified name for the specified group. After deletion, the default group configuration will be applied. This API supports consumer groups, share groups, and streams groups.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `DeleteKafkaGroupConfig` operationId).
	DeleteKafkaGroupConfigWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, reqEditors ...RequestEditorFn) (*DeleteKafkaGroupConfigResponse, error)

	// GetKafkaGroupConfigWithResponse Get group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Get the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `GetKafkaGroupConfig` operationId).
	GetKafkaGroupConfigWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, reqEditors ...RequestEditorFn) (*GetKafkaGroupConfigResponse, error)

	// UpdateKafkaGroupConfigWithBodyWithResponse Update group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `UpdateKafkaGroupConfig` operationId).
	UpdateKafkaGroupConfigWithBodyWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaGroupConfigResponse, error)

	// UpdateKafkaGroupConfigWithResponse Update group config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration with the specified name for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs/{name} (the `UpdateKafkaGroupConfig` operationId).
	UpdateKafkaGroupConfigWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, name ConfigName, body UpdateKafkaGroupConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaGroupConfigResponse, error)

	// UpdateKafkaGroupConfigBatchWithBodyWithResponse Batch Alter Group Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch alter configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs:alter (the `UpdateKafkaGroupConfigBatch` operationId).
	UpdateKafkaGroupConfigBatchWithBodyWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, params *UpdateKafkaGroupConfigBatchParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaGroupConfigBatchResponse, error)

	// UpdateKafkaGroupConfigBatchWithResponse Batch Alter Group Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch alter configurations for the specified group. This API supports consumer groups, share groups, and streams groups.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/groups/{group_id}/configs:alter (the `UpdateKafkaGroupConfigBatch` operationId).
	UpdateKafkaGroupConfigBatchWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, params *UpdateKafkaGroupConfigBatchParams, body UpdateKafkaGroupConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaGroupConfigBatchResponse, error)

	// ListKafkaLinksWithResponse List all cluster links in the dest cluster
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// ``link_id`` in ``ListLinksResponseData`` is deprecated and may be removed in a future release. Use the new ``cluster_link_id`` instead.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links (the `ListKafkaLinks` operationId).
	ListKafkaLinksWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaLinksResponse, error)

	// CreateKafkaLinkWithBodyWithResponse Create a cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Cluster link creation requires source cluster security configurations in
	// the configs JSON section of the data request payload.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links (the `CreateKafkaLink` operationId).
	CreateKafkaLinkWithBodyWithResponse(ctx context.Context, clusterId ClusterId, params *CreateKafkaLinkParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKafkaLinkResponse, error)

	// CreateKafkaLinkWithResponse Create a cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Cluster link creation requires source cluster security configurations in
	// the configs JSON section of the data request payload.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links (the `CreateKafkaLink` operationId).
	CreateKafkaLinkWithResponse(ctx context.Context, clusterId ClusterId, params *CreateKafkaLinkParams, body CreateKafkaLinkJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKafkaLinkResponse, error)

	// ListKafkaMirrorTopicsWithResponse List mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all mirror topics in the cluster
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/-/mirrors (the `ListKafkaMirrorTopics` operationId).
	ListKafkaMirrorTopicsWithResponse(ctx context.Context, clusterId ClusterId, params *ListKafkaMirrorTopicsParams, reqEditors ...RequestEditorFn) (*ListKafkaMirrorTopicsResponse, error)

	// DeleteKafkaLinkWithResponse Delete the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/links/{link_name} (the `DeleteKafkaLink` operationId).
	DeleteKafkaLinkWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *DeleteKafkaLinkParams, reqEditors ...RequestEditorFn) (*DeleteKafkaLinkResponse, error)

	// GetKafkaLinkWithResponse Describe the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// ``link_id`` in ``ListLinksResponseData`` is deprecated and may be removed in a future release. Use the new ``cluster_link_id`` instead.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name} (the `GetKafkaLink` operationId).
	GetKafkaLinkWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *GetKafkaLinkParams, reqEditors ...RequestEditorFn) (*GetKafkaLinkResponse, error)

	// ListKafkaLinkConfigsWithResponse List all configs of the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs (the `ListKafkaLinkConfigs` operationId).
	ListKafkaLinkConfigsWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, reqEditors ...RequestEditorFn) (*ListKafkaLinkConfigsResponse, error)

	// DeleteKafkaLinkConfigWithResponse Reset the given config to default value
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `DeleteKafkaLinkConfig` operationId).
	DeleteKafkaLinkConfigWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, reqEditors ...RequestEditorFn) (*DeleteKafkaLinkConfigResponse, error)

	// GetKafkaLinkConfigsWithResponse Describe the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `GetKafkaLinkConfigs` operationId).
	GetKafkaLinkConfigsWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, reqEditors ...RequestEditorFn) (*GetKafkaLinkConfigsResponse, error)

	// UpdateKafkaLinkConfigWithBodyWithResponse Alter the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `UpdateKafkaLinkConfig` operationId).
	UpdateKafkaLinkConfigWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaLinkConfigResponse, error)

	// UpdateKafkaLinkConfigWithResponse Alter the config under the cluster link
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs/{config_name} (the `UpdateKafkaLinkConfig` operationId).
	UpdateKafkaLinkConfigWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, configName LinkConfigName, body UpdateKafkaLinkConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaLinkConfigResponse, error)

	// UpdateKafkaLinkConfigBatchWithBodyWithResponse Batch Alter Cluster Link Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch Alter Cluster Link Configs
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs:alter (the `UpdateKafkaLinkConfigBatch` operationId).
	UpdateKafkaLinkConfigBatchWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaLinkConfigBatchParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaLinkConfigBatchResponse, error)

	// UpdateKafkaLinkConfigBatchWithResponse Batch Alter Cluster Link Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Batch Alter Cluster Link Configs
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/links/{link_name}/configs:alter (the `UpdateKafkaLinkConfigBatch` operationId).
	UpdateKafkaLinkConfigBatchWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaLinkConfigBatchParams, body UpdateKafkaLinkConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaLinkConfigBatchResponse, error)

	// ListKafkaMirrorTopicsUnderLinkWithResponse List mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List all mirror topics under the link
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `ListKafkaMirrorTopicsUnderLink` operationId).
	ListKafkaMirrorTopicsUnderLinkWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *ListKafkaMirrorTopicsUnderLinkParams, reqEditors ...RequestEditorFn) (*ListKafkaMirrorTopicsUnderLinkResponse, error)

	// CreateKafkaMirrorTopicWithBodyWithResponse Create a mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic in the destination cluster mirroring a topic in
	// the source cluster
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `CreateKafkaMirrorTopic` operationId).
	CreateKafkaMirrorTopicWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKafkaMirrorTopicResponse, error)

	// CreateKafkaMirrorTopicWithResponse Create a mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic in the destination cluster mirroring a topic in
	// the source cluster
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors (the `CreateKafkaMirrorTopic` operationId).
	CreateKafkaMirrorTopicWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, body CreateKafkaMirrorTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKafkaMirrorTopicResponse, error)

	// ReadKafkaMirrorTopicWithResponse Describe the mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors/{mirror_topic_name} (the `ReadKafkaMirrorTopic` operationId).
	ReadKafkaMirrorTopicWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, mirrorTopicName MirrorTopicName, params *ReadKafkaMirrorTopicParams, reqEditors ...RequestEditorFn) (*ReadKafkaMirrorTopicResponse, error)

	// UpdateKafkaMirrorTopicsFailoverWithBodyWithResponse Failover the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:failover (the `UpdateKafkaMirrorTopicsFailover` operationId).
	UpdateKafkaMirrorTopicsFailoverWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsFailoverParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsFailoverResponse, error)

	// UpdateKafkaMirrorTopicsFailoverWithResponse Failover the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:failover (the `UpdateKafkaMirrorTopicsFailover` operationId).
	UpdateKafkaMirrorTopicsFailoverWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsFailoverParams, body UpdateKafkaMirrorTopicsFailoverJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsFailoverResponse, error)

	// UpdateKafkaMirrorTopicsPauseWithBodyWithResponse Pause the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:pause (the `UpdateKafkaMirrorTopicsPause` operationId).
	UpdateKafkaMirrorTopicsPauseWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPauseParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsPauseResponse, error)

	// UpdateKafkaMirrorTopicsPauseWithResponse Pause the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:pause (the `UpdateKafkaMirrorTopicsPause` operationId).
	UpdateKafkaMirrorTopicsPauseWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPauseParams, body UpdateKafkaMirrorTopicsPauseJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsPauseResponse, error)

	// UpdateKafkaMirrorTopicsPromoteWithBodyWithResponse Promote the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:promote (the `UpdateKafkaMirrorTopicsPromote` operationId).
	UpdateKafkaMirrorTopicsPromoteWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPromoteParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsPromoteResponse, error)

	// UpdateKafkaMirrorTopicsPromoteWithResponse Promote the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:promote (the `UpdateKafkaMirrorTopicsPromote` operationId).
	UpdateKafkaMirrorTopicsPromoteWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsPromoteParams, body UpdateKafkaMirrorTopicsPromoteJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsPromoteResponse, error)

	// UpdateKafkaMirrorTopicsResumeWithBodyWithResponse Resume the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:resume (the `UpdateKafkaMirrorTopicsResume` operationId).
	UpdateKafkaMirrorTopicsResumeWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsResumeParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsResumeResponse, error)

	// UpdateKafkaMirrorTopicsResumeWithResponse Resume the mirror topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:resume (the `UpdateKafkaMirrorTopicsResume` operationId).
	UpdateKafkaMirrorTopicsResumeWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsResumeParams, body UpdateKafkaMirrorTopicsResumeJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsResumeResponse, error)

	// UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithBodyWithResponse Reverse the local mirror topic and Pause the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-pause-mirror (the `UpdateKafkaMirrorTopicsReverseAndPauseMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndPauseMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse, error)

	// UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithResponse Reverse the local mirror topic and Pause the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-pause-mirror (the `UpdateKafkaMirrorTopicsReverseAndPauseMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndPauseMirrorWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndPauseMirrorParams, body UpdateKafkaMirrorTopicsReverseAndPauseMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse, error)

	// UpdateKafkaMirrorTopicsReverseAndStartMirrorWithBodyWithResponse Reverse the local mirror topic and start the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-start-mirror (the `UpdateKafkaMirrorTopicsReverseAndStartMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndStartMirrorWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndStartMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse, error)

	// UpdateKafkaMirrorTopicsReverseAndStartMirrorWithResponse Reverse the local mirror topic and start the remote mirror topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:reverse-and-start-mirror (the `UpdateKafkaMirrorTopicsReverseAndStartMirror` operationId).
	UpdateKafkaMirrorTopicsReverseAndStartMirrorWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsReverseAndStartMirrorParams, body UpdateKafkaMirrorTopicsReverseAndStartMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse, error)

	// UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithBodyWithResponse Truncates the local topic to the remote stopped mirror log end offsets and restores mirroring to the local topic to mirror from the remote topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:truncate-and-restore (the `UpdateKafkaMirrorTopicsTruncateAndRestoreMirror` operationId).
	UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithBodyWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse, error)

	// UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithResponse Truncates the local topic to the remote stopped mirror log end offsets and restores mirroring to the local topic to mirror from the remote topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/links/{link_name}/mirrors:truncate-and-restore (the `UpdateKafkaMirrorTopicsTruncateAndRestoreMirror` operationId).
	UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorWithResponse(ctx context.Context, clusterId ClusterId, linkName LinkName, params *UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorParams, body UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse, error)

	// ListKafkaShareGroupsWithResponse List Share Groups
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the list of share groups that belong to the specified
	// Kafka cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups (the `ListKafkaShareGroups` operationId).
	ListKafkaShareGroupsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaShareGroupsResponse, error)

	// DeleteKafkaShareGroupWithResponse Delete Share Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the share group specified by the ``group_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/share-groups/{group_id} (the `DeleteKafkaShareGroup` operationId).
	DeleteKafkaShareGroupWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*DeleteKafkaShareGroupResponse, error)

	// GetKafkaShareGroupWithResponse Get Share Group
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the share group specified by the ``group_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id} (the `GetKafkaShareGroup` operationId).
	GetKafkaShareGroupWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*GetKafkaShareGroupResponse, error)

	// ListKafkaShareGroupConsumersWithResponse List Share Group Consumers
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of consumers that belong to the specified share
	// group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers (the `ListKafkaShareGroupConsumers` operationId).
	ListKafkaShareGroupConsumersWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*ListKafkaShareGroupConsumersResponse, error)

	// GetKafkaShareGroupConsumerWithResponse Get Share Group Consumer
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the consumer specified by the ``consumer_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers/{consumer_id} (the `GetKafkaShareGroupConsumer` operationId).
	GetKafkaShareGroupConsumerWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*GetKafkaShareGroupConsumerResponse, error)

	// ListKafkaShareGroupConsumerAssignmentsWithResponse List Share Group Consumer Assignments
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the consumer assignments specified by the ``consumer_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/share-groups/{group_id}/consumers/{consumer_id}/assignments (the `ListKafkaShareGroupConsumerAssignments` operationId).
	ListKafkaShareGroupConsumerAssignmentsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, consumerId ConsumerId, reqEditors ...RequestEditorFn) (*ListKafkaShareGroupConsumerAssignmentsResponse, error)

	// ListKafkaStreamsGroupsWithResponse List Streams Groups
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the list of streams groups that belong to the specified Kafka cluster
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups (the `ListKafkaStreamsGroups` operationId).
	ListKafkaStreamsGroupsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaStreamsGroupsResponse, error)

	// GetKafkaStreamsGroupWithResponse Get Streams Group
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the streams group specified by the ``group_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id} (the `GetKafkaStreamsGroup` operationId).
	GetKafkaStreamsGroupWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupResponse, error)

	// ListKafkaStreamsGroupMembersWithResponse List Streams Group Members
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of members that belong to the specified streams group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members (the `ListKafkaStreamsGroupMembers` operationId).
	ListKafkaStreamsGroupMembersWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*ListKafkaStreamsGroupMembersResponse, error)

	// GetKafkaStreamsGroupMemberWithResponse Get Streams Group Member
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the members specified by the ``member_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id} (the `GetKafkaStreamsGroupMember` operationId).
	GetKafkaStreamsGroupMemberWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupMemberResponse, error)

	// GetKafkaStreamsGroupMemberAssignmentsWithResponse Get Streams Group Member Assignments
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the assignments of the member specified by the ``member_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments (the `GetKafkaStreamsGroupMemberAssignments` operationId).
	GetKafkaStreamsGroupMemberAssignmentsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupMemberAssignmentsResponse, error)

	// ListKafkaStreamsGroupMemberAssignmentTasksWithResponse List Streams Group Assignments of a Specific Type
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments/{assignments_type} (the `ListKafkaStreamsGroupMemberAssignmentTasks` operationId).
	ListKafkaStreamsGroupMemberAssignmentTasksWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, reqEditors ...RequestEditorFn) (*ListKafkaStreamsGroupMemberAssignmentTasksResponse, error)

	// GetKafkaStreamsGroupMemberAssignmentTaskPartitionsWithResponse List Streams Group Assignments Task Partitions of a Specific Type and Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/assignments/{assignments_type}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupMemberAssignmentTaskPartitions` operationId).
	GetKafkaStreamsGroupMemberAssignmentTaskPartitionsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse, error)

	// GetKafkaStreamsGroupMemberTargetAssignmentsWithResponse Get Streams Group Member Target Assignments
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the target assignments of the member specified by the ``member_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments (the `GetKafkaStreamsGroupMemberTargetAssignments` operationId).
	GetKafkaStreamsGroupMemberTargetAssignmentsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupMemberTargetAssignmentsResponse, error)

	// ListKafkaStreamsGroupMemberTargetAssignmentTasksWithResponse List Streams Group Target Assignments of a Specific Type
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the target tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments/{assignments_type} (the `ListKafkaStreamsGroupMemberTargetAssignmentTasks` operationId).
	ListKafkaStreamsGroupMemberTargetAssignmentTasksWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, reqEditors ...RequestEditorFn) (*ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse, error)

	// GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsWithResponse List Streams Group Target Assignments Task Partitions of a Specific Type and Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the tasks of the member specified by the ``member_id``, and the type ``assignments_type``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/members/{member_id}/target-assignments/{assignments_type}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitions` operationId).
	GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, memberId MemberId, assignmentsType AssignmentsType, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse, error)

	// ListKafkaStreamsGroupSubtopologiesWithResponse List Streams Group Subtopologies
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return a list of subtopologies that belong to the specified streams group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/subtopologies (the `ListKafkaStreamsGroupSubtopologies` operationId).
	ListKafkaStreamsGroupSubtopologiesWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, reqEditors ...RequestEditorFn) (*ListKafkaStreamsGroupSubtopologiesResponse, error)

	// GetKafkaStreamsGroupSubtopologyWithResponse Get Streams Group Subtopology
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	// Return the subtopology specified by the ``subtopology_id``.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/streams-groups/{group_id}/subtopologies/{subtopology_id} (the `GetKafkaStreamsGroupSubtopology` operationId).
	GetKafkaStreamsGroupSubtopologyWithResponse(ctx context.Context, clusterId ClusterId, groupId GroupId, subtopologyId SubtopologyId, reqEditors ...RequestEditorFn) (*GetKafkaStreamsGroupSubtopologyResponse, error)

	// ListKafkaTopicsWithResponse List Topics
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of topics that belong to the specified Kafka cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics (the `ListKafkaTopics` operationId).
	ListKafkaTopicsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaTopicsResponse, error)

	// CreateKafkaTopicWithBodyWithResponse Create Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic.
	// Also supports a dry-run mode that only validates whether the topic creation would succeed
	// if the ``validate_only`` request property is explicitly specified and set to true. Note that
	// when dry-run mode is being used the response status would be 200 OK instead of 201 Created.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics (the `CreateKafkaTopic` operationId).
	CreateKafkaTopicWithBodyWithResponse(ctx context.Context, clusterId ClusterId, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateKafkaTopicResponse, error)

	// CreateKafkaTopicWithResponse Create Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Create a topic.
	// Also supports a dry-run mode that only validates whether the topic creation would succeed
	// if the ``validate_only`` request property is explicitly specified and set to true. Note that
	// when dry-run mode is being used the response status would be 200 OK instead of 201 Created.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics (the `CreateKafkaTopic` operationId).
	CreateKafkaTopicWithResponse(ctx context.Context, clusterId ClusterId, body CreateKafkaTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateKafkaTopicResponse, error)

	// ListKafkaAllTopicConfigsWithResponse List All Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of configuration parameters for all topics hosted by the specified
	// cluster.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/-/configs (the `ListKafkaAllTopicConfigs` operationId).
	ListKafkaAllTopicConfigsWithResponse(ctx context.Context, clusterId ClusterId, reqEditors ...RequestEditorFn) (*ListKafkaAllTopicConfigsResponse, error)

	// DeleteKafkaTopicWithResponse Delete Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Delete the topic with the given `topic_name`.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `DeleteKafkaTopic` operationId).
	DeleteKafkaTopicWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*DeleteKafkaTopicResponse, error)

	// GetKafkaTopicWithResponse Get Topic
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the topic with the given `topic_name`.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `GetKafkaTopic` operationId).
	GetKafkaTopicWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, params *GetKafkaTopicParams, reqEditors ...RequestEditorFn) (*GetKafkaTopicResponse, error)

	// UpdatePartitionCountKafkaTopicWithBodyWithResponse Update Partition Count
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Increase the number of partitions for a topic. To update other topic
	// configurations, see https://docs.confluent.io/cloud/current/api.html#tag/Configs-(v3)/operation/updateKafkaTopicConfig.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `UpdatePartitionCountKafkaTopic` operationId).
	UpdatePartitionCountKafkaTopicWithBodyWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdatePartitionCountKafkaTopicResponse, error)

	// UpdatePartitionCountKafkaTopicWithResponse Update Partition Count
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Increase the number of partitions for a topic. To update other topic
	// configurations, see https://docs.confluent.io/cloud/current/api.html#tag/Configs-(v3)/operation/updateKafkaTopicConfig.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /kafka/v3/clusters/{cluster_id}/topics/{topic_name} (the `UpdatePartitionCountKafkaTopic` operationId).
	UpdatePartitionCountKafkaTopicWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, body UpdatePartitionCountKafkaTopicJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdatePartitionCountKafkaTopicResponse, error)

	// ListKafkaTopicConfigsWithResponse List Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of configuration parameters that belong to the specified topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs (the `ListKafkaTopicConfigs` operationId).
	ListKafkaTopicConfigsWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*ListKafkaTopicConfigsResponse, error)

	// DeleteKafkaTopicConfigWithResponse Reset Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Reset the configuration parameter with given `name` to its default value.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `DeleteKafkaTopicConfig` operationId).
	DeleteKafkaTopicConfigWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, reqEditors ...RequestEditorFn) (*DeleteKafkaTopicConfigResponse, error)

	// GetKafkaTopicConfigWithResponse Get Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the configuration parameter with the given `name`.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `GetKafkaTopicConfig` operationId).
	GetKafkaTopicConfigWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, reqEditors ...RequestEditorFn) (*GetKafkaTopicConfigResponse, error)

	// UpdateKafkaTopicConfigWithBodyWithResponse Update Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration parameter with given `name`. To update the
	// number of partitions, see
	// https://docs.confluent.io/cloud/current/api.html#tag/Topic-(v3)/operation/updatePartitionCountKafkaTopic.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `UpdateKafkaTopicConfig` operationId).
	UpdateKafkaTopicConfigWithBodyWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaTopicConfigResponse, error)

	// UpdateKafkaTopicConfigWithResponse Update Topic Config
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the configuration parameter with given `name`. To update the
	// number of partitions, see
	// https://docs.confluent.io/cloud/current/api.html#tag/Topic-(v3)/operation/updatePartitionCountKafkaTopic.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name} (the `UpdateKafkaTopicConfig` operationId).
	UpdateKafkaTopicConfigWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, name ConfigName, body UpdateKafkaTopicConfigJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaTopicConfigResponse, error)

	// UpdateKafkaTopicConfigBatchWithBodyWithResponse Batch Alter Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of topic configuration parameters.
	// Also supports a dry-run mode that only validates whether the operation would succeed if the
	// ``validate_only`` request property is explicitly specified and set to true.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter (the `UpdateKafkaTopicConfigBatch` operationId).
	UpdateKafkaTopicConfigBatchWithBodyWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateKafkaTopicConfigBatchResponse, error)

	// UpdateKafkaTopicConfigBatchWithResponse Batch Alter Topic Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update or delete a set of topic configuration parameters.
	// Also supports a dry-run mode that only validates whether the operation would succeed if the
	// ``validate_only`` request property is explicitly specified and set to true.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter (the `UpdateKafkaTopicConfigBatch` operationId).
	UpdateKafkaTopicConfigBatchWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, body UpdateKafkaTopicConfigBatchJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateKafkaTopicConfigBatchResponse, error)

	// ListKafkaDefaultTopicConfigsWithResponse List New Topic Default Configs
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// List the default configuration parameters used if the topic were to be newly created.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/default-configs (the `ListKafkaDefaultTopicConfigs` operationId).
	ListKafkaDefaultTopicConfigsWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*ListKafkaDefaultTopicConfigsResponse, error)

	// ListKafkaPartitionsWithResponse List Partitions
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the list of partitions that belong to the specified topic.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions (the `ListKafkaPartitions` operationId).
	ListKafkaPartitionsWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, reqEditors ...RequestEditorFn) (*ListKafkaPartitionsResponse, error)

	// GetKafkaPartitionWithResponse Get Partition
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Return the partition with the given `partition_id`.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions/{partition_id} (the `GetKafkaPartition` operationId).
	GetKafkaPartitionWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, partitionId PartitionId, reqEditors ...RequestEditorFn) (*GetKafkaPartitionResponse, error)

	// ProduceRecordWithBodyWithResponse Produce Records
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Produce records to the given topic, returning delivery reports for each
	// record produced. This API can be used in streaming mode by setting
	// "Transfer-Encoding: chunked" header. For as long as the connection is
	// kept open, the server will keep accepting records. Records are streamed
	// to and from the server as Concatenated JSON. For each record sent to the
	// server, the server will asynchronously send back a delivery report, in
	// the same order, each with its own error_code. An error_code of 200
	// indicates success. The HTTP status code will be HTTP 200 OK as long as
	// the connection is successfully established. To identify records that
	// have encountered an error, check the error_code of each delivery report.
	//
	// Note that the cluster_id is validated only when running in Confluent Cloud.
	//
	// This API currently does not support Schema Registry integration. Sending
	// schemas is not supported. Only BINARY, JSON, and STRING formats are
	// supported.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records (the `ProduceRecord` operationId).
	ProduceRecordWithBodyWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*ProduceRecordResponse, error)

	// ProduceRecordWithResponse Produce Records
	//
	// [![Generally Available](https://img.shields.io/badge/Lifecycle%20Stage-Generally%20Available-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Produce records to the given topic, returning delivery reports for each
	// record produced. This API can be used in streaming mode by setting
	// "Transfer-Encoding: chunked" header. For as long as the connection is
	// kept open, the server will keep accepting records. Records are streamed
	// to and from the server as Concatenated JSON. For each record sent to the
	// server, the server will asynchronously send back a delivery report, in
	// the same order, each with its own error_code. An error_code of 200
	// indicates success. The HTTP status code will be HTTP 200 OK as long as
	// the connection is successfully established. To identify records that
	// have encountered an error, check the error_code of each delivery report.
	//
	// Note that the cluster_id is validated only when running in Confluent Cloud.
	//
	// This API currently does not support Schema Registry integration. Sending
	// schemas is not supported. Only BINARY, JSON, and STRING formats are
	// supported.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records (the `ProduceRecord` operationId).
	ProduceRecordWithResponse(ctx context.Context, clusterId ClusterId, topicName TopicName, body ProduceRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*ProduceRecordResponse, error)
}

func (r GetKafkaClusterResponse) GetJSON200() *GetClusterResponse {
	return r.JSON200
}
func (r GetKafkaClusterResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaClusterResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaClusterResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaClusterResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaClusterResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaClusterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaClusterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaClusterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaAclsResponse) GetJSON200() *DeleteAclsResponse {
	return r.JSON200
}
func (r DeleteKafkaAclsResponse) GetJSON400() *BadRequestErrorResponseDeleteAcls {
	return r.JSON400
}
func (r DeleteKafkaAclsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaAclsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r DeleteKafkaAclsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaAclsResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaAclsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaAclsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaAclsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaAclsResponse) GetJSON200() *SearchAclsResponse {
	return r.JSON200
}
func (r GetKafkaAclsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaAclsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaAclsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaAclsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaAclsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaAclsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaAclsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaAclsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKafkaAclsResponse) GetJSON400() *BadRequestErrorResponseCreateAcls {
	return r.JSON400
}
func (r CreateKafkaAclsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r CreateKafkaAclsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r CreateKafkaAclsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r CreateKafkaAclsResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKafkaAclsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKafkaAclsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKafkaAclsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r BatchCreateKafkaAclsResponse) GetJSON400() *BadRequestErrorResponseCreateAcls {
	return r.JSON400
}
func (r BatchCreateKafkaAclsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r BatchCreateKafkaAclsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r BatchCreateKafkaAclsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r BatchCreateKafkaAclsResponse) GetBody() []byte {
	return r.Body
}
func (r BatchCreateKafkaAclsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r BatchCreateKafkaAclsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r BatchCreateKafkaAclsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaClusterConfigsResponse) GetJSON200() *ListClusterConfigsResponse {
	return r.JSON200
}
func (r ListKafkaClusterConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaClusterConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaClusterConfigsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaClusterConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaClusterConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaClusterConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaClusterConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaClusterConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaClusterConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaClusterConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaClusterConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r DeleteKafkaClusterConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaClusterConfigResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaClusterConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaClusterConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaClusterConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaClusterConfigResponse) GetJSON200() *GetClusterConfigResponse {
	return r.JSON200
}
func (r GetKafkaClusterConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaClusterConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaClusterConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaClusterConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaClusterConfigResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaClusterConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaClusterConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaClusterConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaClusterConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaClusterConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaClusterConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r UpdateKafkaClusterConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaClusterConfigResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaClusterConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaClusterConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaClusterConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaClusterConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaClusterConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaClusterConfigsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r UpdateKafkaClusterConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaClusterConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaClusterConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaClusterConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaClusterConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaConsumerGroupsResponse) GetJSON200() *ListConsumerGroupsResponse {
	return r.JSON200
}
func (r ListKafkaConsumerGroupsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaConsumerGroupsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaConsumerGroupsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaConsumerGroupsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaConsumerGroupsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaConsumerGroupsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaConsumerGroupsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaConsumerGroupsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaConsumerGroupResponse) GetJSON200() *GetConsumerGroupResponse {
	return r.JSON200
}
func (r GetKafkaConsumerGroupResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaConsumerGroupResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaConsumerGroupResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaConsumerGroupResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaConsumerGroupResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaConsumerGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaConsumerGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaConsumerGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaConsumersResponse) GetJSON200() *ListConsumersResponse {
	return r.JSON200
}
func (r ListKafkaConsumersResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaConsumersResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaConsumersResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaConsumersResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaConsumersResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaConsumersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaConsumersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaConsumersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaConsumerResponse) GetJSON200() *GetConsumerResponse {
	return r.JSON200
}
func (r GetKafkaConsumerResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaConsumerResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaConsumerResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaConsumerResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaConsumerResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaConsumerResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaConsumerResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaConsumerResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetJSON200() *GetConsumerGroupLagSummaryResponse {
	return r.JSON200
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaConsumerGroupLagSummaryResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaConsumerGroupLagSummaryResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaConsumerGroupLagSummaryResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaConsumerGroupLagSummaryResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaConsumerLagsResponse) GetJSON200() *ListConsumerLagsResponse {
	return r.JSON200
}
func (r ListKafkaConsumerLagsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaConsumerLagsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaConsumerLagsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaConsumerLagsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaConsumerLagsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaConsumerLagsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaConsumerLagsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaConsumerLagsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaConsumerLagResponse) GetJSON200() *GetConsumerLagResponse {
	return r.JSON200
}
func (r GetKafkaConsumerLagResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaConsumerLagResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaConsumerLagResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaConsumerLagResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaConsumerLagResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaConsumerLagResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaConsumerLagResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaConsumerLagResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaGroupConfigsResponse) GetJSON200() *ListGroupConfigsResponse {
	return r.JSON200
}
func (r ListKafkaGroupConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaGroupConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaGroupConfigsResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r ListKafkaGroupConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaGroupConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaGroupConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaGroupConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaGroupConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaGroupConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaGroupConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaGroupConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r DeleteKafkaGroupConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaGroupConfigResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaGroupConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaGroupConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaGroupConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaGroupConfigResponse) GetJSON200() *GetGroupConfigResponse {
	return r.JSON200
}
func (r GetKafkaGroupConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaGroupConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaGroupConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r GetKafkaGroupConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaGroupConfigResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaGroupConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaGroupConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaGroupConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaGroupConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaGroupConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaGroupConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r UpdateKafkaGroupConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaGroupConfigResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaGroupConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaGroupConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaGroupConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaGroupConfigBatchResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaGroupConfigBatchResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaGroupConfigBatchResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r UpdateKafkaGroupConfigBatchResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaGroupConfigBatchResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaGroupConfigBatchResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaGroupConfigBatchResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaGroupConfigBatchResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaLinksResponse) GetJSON200() *ListLinksResponse {
	return r.JSON200
}
func (r ListKafkaLinksResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaLinksResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaLinksResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaLinksResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaLinksResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaLinksResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaLinksResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKafkaLinkResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r CreateKafkaLinkResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r CreateKafkaLinkResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r CreateKafkaLinkResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKafkaLinkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKafkaLinkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKafkaLinkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaMirrorTopicsResponse) GetJSON200() *ListMirrorTopicsResponse {
	return r.JSON200
}
func (r ListKafkaMirrorTopicsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaMirrorTopicsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaMirrorTopicsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaMirrorTopicsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaMirrorTopicsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaMirrorTopicsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaMirrorTopicsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaLinkResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaLinkResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaLinkResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaLinkResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaLinkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaLinkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaLinkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaLinkResponse) GetJSON200() *GetLinkResponse {
	return r.JSON200
}
func (r GetKafkaLinkResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaLinkResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaLinkResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaLinkResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaLinkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaLinkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaLinkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaLinkConfigsResponse) GetJSON200() *ListLinkConfigsResponse {
	return r.JSON200
}
func (r ListKafkaLinkConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaLinkConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaLinkConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaLinkConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaLinkConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaLinkConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaLinkConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaLinkConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaLinkConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaLinkConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaLinkConfigResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaLinkConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaLinkConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaLinkConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaLinkConfigsResponse) GetJSON200() *GetLinkConfigsResponse {
	return r.JSON200
}
func (r GetKafkaLinkConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaLinkConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaLinkConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaLinkConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaLinkConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaLinkConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaLinkConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaLinkConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaLinkConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaLinkConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaLinkConfigResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaLinkConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaLinkConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaLinkConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaLinkConfigBatchResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaLinkConfigBatchResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaLinkConfigBatchResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaLinkConfigBatchResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaLinkConfigBatchResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaLinkConfigBatchResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaLinkConfigBatchResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) GetJSON200() *ListMirrorTopicsResponse {
	return r.JSON200
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaMirrorTopicsUnderLinkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKafkaMirrorTopicResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r CreateKafkaMirrorTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r CreateKafkaMirrorTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r CreateKafkaMirrorTopicResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKafkaMirrorTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKafkaMirrorTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKafkaMirrorTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ReadKafkaMirrorTopicResponse) GetJSON200() *DescribeMirrorTopicResponse {
	return r.JSON200
}
func (r ReadKafkaMirrorTopicResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ReadKafkaMirrorTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ReadKafkaMirrorTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ReadKafkaMirrorTopicResponse) GetBody() []byte {
	return r.Body
}
func (r ReadKafkaMirrorTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ReadKafkaMirrorTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ReadKafkaMirrorTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsFailoverResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsPauseResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsPauseResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsPauseResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsPauseResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsPauseResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsPauseResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsPauseResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsPauseResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsPromoteResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsResumeResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsResumeResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsResumeResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsResumeResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsResumeResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsResumeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsResumeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsResumeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsReverseAndPauseMirrorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsReverseAndStartMirrorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) GetJSON200() *AlterMirrorStatusResponse {
	return r.JSON200
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaMirrorTopicsTruncateAndRestoreMirrorResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaShareGroupsResponse) GetJSON200() *ListShareGroupsResponse {
	return r.JSON200
}
func (r ListKafkaShareGroupsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaShareGroupsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaShareGroupsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaShareGroupsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaShareGroupsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaShareGroupsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaShareGroupsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaShareGroupsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaShareGroupResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaShareGroupResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaShareGroupResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r DeleteKafkaShareGroupResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r DeleteKafkaShareGroupResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaShareGroupResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaShareGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaShareGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaShareGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaShareGroupResponse) GetJSON200() *GetShareGroupResponse {
	return r.JSON200
}
func (r GetKafkaShareGroupResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaShareGroupResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaShareGroupResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaShareGroupResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaShareGroupResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaShareGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaShareGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaShareGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaShareGroupConsumersResponse) GetJSON200() *ListShareGroupConsumersResponse {
	return r.JSON200
}
func (r ListKafkaShareGroupConsumersResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaShareGroupConsumersResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaShareGroupConsumersResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaShareGroupConsumersResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaShareGroupConsumersResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaShareGroupConsumersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaShareGroupConsumersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaShareGroupConsumersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaShareGroupConsumerResponse) GetJSON200() *GetShareGroupConsumerResponse {
	return r.JSON200
}
func (r GetKafkaShareGroupConsumerResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaShareGroupConsumerResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaShareGroupConsumerResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaShareGroupConsumerResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaShareGroupConsumerResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaShareGroupConsumerResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaShareGroupConsumerResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaShareGroupConsumerResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetJSON200() *ListShareGroupConsumerAssignmentsResponse {
	return r.JSON200
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaShareGroupConsumerAssignmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaStreamsGroupsResponse) GetJSON200() *ListStreamsGroupsResponse {
	return r.JSON200
}
func (r ListKafkaStreamsGroupsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaStreamsGroupsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaStreamsGroupsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaStreamsGroupsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaStreamsGroupsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaStreamsGroupsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaStreamsGroupsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaStreamsGroupsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupResponse) GetJSON200() *GetStreamsGroupResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaStreamsGroupMembersResponse) GetJSON200() *ListStreamsGroupMembersResponse {
	return r.JSON200
}
func (r ListKafkaStreamsGroupMembersResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaStreamsGroupMembersResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaStreamsGroupMembersResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaStreamsGroupMembersResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaStreamsGroupMembersResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaStreamsGroupMembersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaStreamsGroupMembersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaStreamsGroupMembersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupMemberResponse) GetJSON200() *GetStreamsGroupMemberResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupMemberResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupMemberResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupMemberResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupMemberResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupMemberResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupMemberResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupMemberResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupMemberResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetJSON200() *GetStreamsGroupMemberAssignmentsResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupMemberAssignmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetJSON200() *ListStreamsTasksResponse {
	return r.JSON200
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaStreamsGroupMemberAssignmentTasksResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetJSON200() *GetStreamsTaskResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupMemberAssignmentTaskPartitionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetJSON200() *GetStreamsGroupMemberAssignmentsResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetJSON200() *ListStreamsTasksResponse {
	return r.JSON200
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaStreamsGroupMemberTargetAssignmentTasksResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetJSON200() *GetStreamsTaskResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupMemberTargetAssignmentTaskPartitionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetJSON200() *ListStreamsGroupSubtopologiesResponse {
	return r.JSON200
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaStreamsGroupSubtopologiesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetJSON200() *GetStreamsGroupSubtopologyResponse {
	return r.JSON200
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaStreamsGroupSubtopologyResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaStreamsGroupSubtopologyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaStreamsGroupSubtopologyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaStreamsGroupSubtopologyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaTopicsResponse) GetJSON200() *ListTopicsResponse {
	return r.JSON200
}
func (r ListKafkaTopicsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaTopicsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaTopicsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaTopicsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaTopicsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaTopicsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaTopicsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaTopicsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateKafkaTopicResponse) GetJSON200() *CreateTopicResponse {
	return r.JSON200
}
func (r CreateKafkaTopicResponse) GetJSON201() *CreateTopicResponse {
	return r.JSON201
}
func (r CreateKafkaTopicResponse) GetJSON400() *BadRequestErrorResponseCreateTopic {
	return r.JSON400
}
func (r CreateKafkaTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r CreateKafkaTopicResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r CreateKafkaTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r CreateKafkaTopicResponse) GetBody() []byte {
	return r.Body
}
func (r CreateKafkaTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateKafkaTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateKafkaTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaAllTopicConfigsResponse) GetJSON200() *ListTopicConfigsResponse {
	return r.JSON200
}
func (r ListKafkaAllTopicConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaAllTopicConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaAllTopicConfigsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaAllTopicConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaAllTopicConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaAllTopicConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaAllTopicConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaAllTopicConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaTopicResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaTopicResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r DeleteKafkaTopicResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r DeleteKafkaTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaTopicResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaTopicResponse) GetJSON200() *GetTopicResponse {
	return r.JSON200
}
func (r GetKafkaTopicResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaTopicResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaTopicResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r GetKafkaTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaTopicResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdatePartitionCountKafkaTopicResponse) GetJSON200() *GetTopicResponse {
	return r.JSON200
}
func (r UpdatePartitionCountKafkaTopicResponse) GetJSON400() *BadRequestErrorResponseUpdatePartitionCountTopic {
	return r.JSON400
}
func (r UpdatePartitionCountKafkaTopicResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdatePartitionCountKafkaTopicResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r UpdatePartitionCountKafkaTopicResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdatePartitionCountKafkaTopicResponse) GetBody() []byte {
	return r.Body
}
func (r UpdatePartitionCountKafkaTopicResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdatePartitionCountKafkaTopicResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdatePartitionCountKafkaTopicResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaTopicConfigsResponse) GetJSON200() *ListTopicConfigsResponse {
	return r.JSON200
}
func (r ListKafkaTopicConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaTopicConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaTopicConfigsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaTopicConfigsResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r ListKafkaTopicConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaTopicConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaTopicConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaTopicConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaTopicConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteKafkaTopicConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r DeleteKafkaTopicConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r DeleteKafkaTopicConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r DeleteKafkaTopicConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r DeleteKafkaTopicConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r DeleteKafkaTopicConfigResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteKafkaTopicConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteKafkaTopicConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteKafkaTopicConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaTopicConfigResponse) GetJSON200() *GetTopicConfigResponse {
	return r.JSON200
}
func (r GetKafkaTopicConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaTopicConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaTopicConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaTopicConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r GetKafkaTopicConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaTopicConfigResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaTopicConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaTopicConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaTopicConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaTopicConfigResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaTopicConfigResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaTopicConfigResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r UpdateKafkaTopicConfigResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r UpdateKafkaTopicConfigResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaTopicConfigResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaTopicConfigResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaTopicConfigResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaTopicConfigResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateKafkaTopicConfigBatchResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r UpdateKafkaTopicConfigBatchResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r UpdateKafkaTopicConfigBatchResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r UpdateKafkaTopicConfigBatchResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r UpdateKafkaTopicConfigBatchResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r UpdateKafkaTopicConfigBatchResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateKafkaTopicConfigBatchResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateKafkaTopicConfigBatchResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateKafkaTopicConfigBatchResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaDefaultTopicConfigsResponse) GetJSON200() *ListTopicConfigsResponse {
	return r.JSON200
}
func (r ListKafkaDefaultTopicConfigsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaDefaultTopicConfigsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaDefaultTopicConfigsResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r ListKafkaDefaultTopicConfigsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaDefaultTopicConfigsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaDefaultTopicConfigsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaDefaultTopicConfigsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaDefaultTopicConfigsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListKafkaPartitionsResponse) GetJSON200() *ListPartitionsResponse {
	return r.JSON200
}
func (r ListKafkaPartitionsResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r ListKafkaPartitionsResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ListKafkaPartitionsResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ListKafkaPartitionsResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r ListKafkaPartitionsResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ListKafkaPartitionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListKafkaPartitionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListKafkaPartitionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListKafkaPartitionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetKafkaPartitionResponse) GetJSON200() *GetPartitionResponse {
	return r.JSON200
}
func (r GetKafkaPartitionResponse) GetJSON400() *BadRequestErrorResponse {
	return r.JSON400
}
func (r GetKafkaPartitionResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r GetKafkaPartitionResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r GetKafkaPartitionResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r GetKafkaPartitionResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r GetKafkaPartitionResponse) GetBody() []byte {
	return r.Body
}
func (r GetKafkaPartitionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetKafkaPartitionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetKafkaPartitionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ProduceRecordResponse) GetJSON200() *ProduceResponse {
	return r.JSON200
}
func (r ProduceRecordResponse) GetJSON400() *BadRequestErrorResponseProduceRecords {
	return r.JSON400
}
func (r ProduceRecordResponse) GetJSON401() *UnauthorizedErrorResponse {
	return r.JSON401
}
func (r ProduceRecordResponse) GetJSON403() *ForbiddenErrorResponse {
	return r.JSON403
}
func (r ProduceRecordResponse) GetJSON404() *NotFoundErrorResponse {
	return r.JSON404
}
func (r ProduceRecordResponse) GetJSON413() *RequestEntityTooLargeErrorResponse {
	return r.JSON413
}
func (r ProduceRecordResponse) GetJSON415() *UnsupportedMediaTypeErrorResponse {
	return r.JSON415
}
func (r ProduceRecordResponse) GetJSON422() *UnprocessableEntityProduceRecord {
	return r.JSON422
}
func (r ProduceRecordResponse) GetJSON5XX() *ServerErrorResponse {
	return r.JSON5XX
}
func (r ProduceRecordResponse) GetBody() []byte {
	return r.Body
}
func (r ProduceRecordResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ProduceRecordResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ProduceRecordResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
