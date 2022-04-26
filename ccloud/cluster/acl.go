package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type (
	AclResourceType   string
	AclOperationType  string
	AclPermissionType string
	AclPatternType    string
)

const (
	AclResourceTypeUnknown         AclResourceType = "UNKNOWN"
	AclResourceTypeAny             AclResourceType = "ANY"
	AclResourceTypeTopic           AclResourceType = "TOPIC"
	AclResourceTypeGroup           AclResourceType = "GROUP"
	AclResourceTypeCluster         AclResourceType = "CLUSTER"
	AclResourceTypeTransactionalId AclResourceType = "TRANSACTIONAL_ID"
	AclResourceTypeDelegationToken AclResourceType = "DELEGATION_TOKEN"

	AclOperationTypeUnknown         AclOperationType = "UNKNOWN"
	AclOperationTypeAny             AclOperationType = "ANY"
	AclOperationTypeAll             AclOperationType = "ALL"
	AclOperationTypeRead            AclOperationType = "READ"
	AclOperationTypeWrite           AclOperationType = "WRITE"
	AclOperationTypeCreate          AclOperationType = "CREATE"
	AclOperationTypeDelete          AclOperationType = "DELETE"
	AclOperationTypeAlter           AclOperationType = "ALTER"
	AclOperationTypeDescribe        AclOperationType = "DESCRIBE"
	AclOperationTypeClusterAction   AclOperationType = "CLUSTER_ACTION"
	AclOperationTypeDescribeConfigs AclOperationType = "DESCRIBE_CONFIGS"
	AclOperationTypeAlterConfigs    AclOperationType = "ALTER_CONFIGS"
	AclOperationTypeIdempotentWrite AclOperationType = "IDEMPOTENT_WRITE"

	AclPermissionTypeUnknown AclPermissionType = "UNKNOWN"
	AclPermissionTypeAllow   AclPermissionType = "ALLOW"
	AclPermissionTypeDeny    AclPermissionType = "DENY"
	AclPermissionTypeAny     AclPermissionType = "ANY"

	AclPatternTypeUnknown  AclPatternType = "UNKNOWN"
	AclPatternTypeLiteral  AclPatternType = "LITERAL"
	AclPatternTypePrefixed AclPatternType = "PREFIXED"
	AclPatternTypeMatch    AclPatternType = "MATCH"
	AclPatternTypeAny      AclPatternType = "ANY"
)

type KafkaAcl struct {
	common.BaseModel
	ClusterId    string            `json:"cluster_id"`
	ResourceType AclResourceType   `json:"resource_type"`
	ResourceName string            `json:"resource_name"`
	PatternType  AclPatternType    `json:"pattern_type"`
	Principal    string            `json:"principal"`
	Host         string            `json:"host"`
	Operation    AclOperationType  `json:"operation"`
	Permission   AclPermissionType `json:"permission"`
}

type KafkaAclList struct {
	common.BaseModel
	Data []KafkaAcl `json:"data"`
}

type KafkaAclSearchQry struct {
	common.PaginationOptions
	ResourceType AclResourceType   `url:"resource_type,omitempty"`
	ResourceName string            `url:"resource_name,omitempty"`
	Principal    string            `url:"principal,omitempty"`
	Host         string            `url:"host,omitempty"`
	Operation    AclOperationType  `url:"operation,omitempty"`
	Permission   AclPermissionType `url:"permission,omitempty"`
	PatternType  AclPatternType    `url:"pattern_type,omitempty"`
}

func (c *ConfluentClusterClient) SearchAcls(qry *KafkaAclSearchQry) (*KafkaAclList, error) {
	res, err := c.doRequest(c.clusterInfo.Acls.Related, "", "GET", nil, qry)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to search acls: %s", res.Status)
	}

	defer res.Body.Close()

	var list KafkaAclList
	err = json.NewDecoder(res.Body).Decode(&list)
	if err != nil {
		return nil, err
	}

	return &list, nil
}

func (c *ConfluentClusterClient) CreateAcl(acl *KafkaAcl) (*KafkaAcl, error) {
	res, err := c.doRequest(c.clusterInfo.Acls.Related, "", "POST", nil, acl)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != res.StatusCode {
		return nil, fmt.Errorf("failed to create acl: %s", res.Status)
	}

	defer res.Body.Close()

	var createdAcl KafkaAcl
	err = json.NewDecoder(res.Body).Decode(&createdAcl)
	if err != nil {
		return nil, err
	}

	return &createdAcl, nil
}

func (c *ConfluentClusterClient) DeleteAcl(acl *KafkaAcl) error {
	res, err := c.doRequest(c.clusterInfo.Acls.Related, acl.Id, "DELETE", nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to delete acl: %s", res.Status)
	}

	return nil
}
