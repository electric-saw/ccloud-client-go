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
	ClusterId    string            `json:"cluster_id,omitempty" url:"-"`
	ResourceType AclResourceType   `json:"resource_type,omitempty" url:"resource_type,omitempty"`
	ResourceName string            `json:"resource_name,omitempty" url:"resource_name,omitempty"`
	PatternType  AclPatternType    `json:"pattern_type,omitempty" url:"pattern_type,omitempty"`
	Principal    string            `json:"principal,omitempty" url:"principal,omitempty"`
	Host         string            `json:"host,omitempty" url:"host,omitempty"`
	Operation    AclOperationType  `json:"operation,omitempty" url:"operation,omitempty"`
	Permission   AclPermissionType `json:"permission,omitempty" url:"permission,omitempty"`
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

type KafkaAclCreateReq struct {
	ResourceType AclResourceType   `json:"resource_type,omitempty"`
	ResourceName string            `json:"resource_name,omitempty"`
	PatternType  AclPatternType    `json:"pattern_type,omitempty"`
	Principal    string            `json:"principal,omitempty"`
	Host         string            `json:"host,omitempty"`
	Operation    AclOperationType  `json:"operation,omitempty"`
	Permission   AclPermissionType `json:"permission,omitempty"`
}

func (c *ConfluentClusterClient) CreateAcl(acl *KafkaAclCreateReq) error {
	res, err := c.doRequest(c.clusterInfo.Acls.Related, "", "POST", acl, nil)
	if err != nil {
		return err
	}

	if http.StatusCreated != res.StatusCode {
		errorRes, err := common.NewErrorResponse(res.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("failed to create acl (%s):  %s", res.Status, errorRes.Error())
	}

	return nil
}

func (c *ConfluentClusterClient) DeleteAcl(acl *KafkaAcl) error {
	var url string
	if acl.Metadata.Self != nil && *acl.Metadata.Self != "" {
		url = *acl.Metadata.Self
	} else {
		url = c.clusterInfo.Acls.Related
	}

	res, err := c.doRequest(url, "", "DELETE", nil, acl)
	if err != nil {
		return err
	}

	if http.StatusOK != res.StatusCode && http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to delete acl: %s", res.Status)
	}

	return nil
}
