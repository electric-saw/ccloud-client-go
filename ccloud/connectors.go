package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type Connector struct {
	common.BaseModel
	Name   string                 `json:"name,omitempty"`
	Config map[string]interface{} `json:"config,omitempty"`
}

type ConnectorCreateReq struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

func (c *ConfluentClient) CreateConnector(environmentId, clusterId, name string, config map[string]interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)
	create := &ConnectorCreateReq{
		Name:   name,
		Config: config,
	}
	resp, err := c.doRequest(urlPath, http.MethodPost, create, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		errRes, _ := common.NewErrorResponse(resp.Body)
		if errRes != nil && errRes.Message != "" {
			return nil, fmt.Errorf("failed to create connector: %s", errRes.Message)
		}
		return nil, fmt.Errorf("failed to create connector: %s", resp.Status)
	}

	defer resp.Body.Close()

	var connector Connector
	if err := json.NewDecoder(resp.Body).Decode(&connector); err != nil {
		return nil, err
	}

	return &connector, nil
}

// CreateS3SinkConnector essa func eu criei com base no curl mais para facilitar a criação da func CreateConnector
func (c *ConfluentClient) CreateS3SinkConnector(
	environmentId, clusterId, name, bucket, providerIntegrationId, topics string,
	transformsName, transformsInsertType, transformsInsertPartitionField,
	transformsInsertStaticField, transformsInsertStaticValue,
	transformsInsertTimestampField, transformsInsertTopicField,
	timeInterval string,
	tasksMax int,
) (*Connector, error) {
	cfg := map[string]interface{}{
		"connector.class":         "S3_SINK",
		"s3.bucket.name":          bucket,
		"authentication.method":   "IAM Roles",
		"provider.integration.id": providerIntegrationId,
		"topics":                  topics,
		"input.data.format":       "AVRO",
		"output.data.format":      "AVRO",
		"flush.size":              "1000",
		"partitioner.class":       "TimeBasedPartitioner",
		"time.interval":           timeInterval,
		"tasks.max":               fmt.Sprintf("%d", tasksMax),
	}

	// adiciona transforms se fornecidos
	if transformsName != "" {
		cfg["transforms"] = transformsName
	}
	if transformsInsertType != "" {
		cfg[fmt.Sprintf("transforms.%s.type", transformsName)] = transformsInsertType
	}
	if transformsInsertPartitionField != "" {
		cfg[fmt.Sprintf("transforms.%s.partition.field", transformsName)] = transformsInsertPartitionField
	}
	if transformsInsertStaticField != "" {
		cfg[fmt.Sprintf("transforms.%s.static.field", transformsName)] = transformsInsertStaticField
	}
	if transformsInsertStaticValue != "" {
		cfg[fmt.Sprintf("transforms.%s.static.value", transformsName)] = transformsInsertStaticValue
	}
	if transformsInsertTimestampField != "" {
		cfg[fmt.Sprintf("transforms.%s.timestamp.field", transformsName)] = transformsInsertTimestampField
	}
	if transformsInsertTopicField != "" {
		cfg[fmt.Sprintf("transforms.%s.topic.field", transformsName)] = transformsInsertTopicField
	}

	return c.CreateConnector(environmentId, clusterId, name, cfg)
}

// Get 1 de 9
// ListConnectors lista todos os connectors de um cluster Connect
type ConnectorList struct {
	common.BaseModel
	Data []Connector `json:"data"`
}

func (c *ConfluentClient) ListConnectors(environmentId, clusterId string) (*ConnectorList, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)
	resp, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		errRes, _ := common.NewErrorResponse(resp.Body)
		if errRes != nil && errRes.Message != "" {
			return nil, fmt.Errorf("failed to list connectors: %s", errRes.Message)
		}
		return nil, fmt.Errorf("failed to list connectors: %s", resp.Status)
	}

	defer resp.Body.Close()

	var connectors ConnectorList
	if err := json.NewDecoder(resp.Body).Decode(&connectors); err != nil {
		return nil, err
	}

	return &connectors, nil
}

// Get 2 de 9
// GetConnectorConfig retorna a configuração atual de um connector
type ConnectorConfig map[string]interface{}

func (c *ConfluentClient) GetConnectorConfig(environmentId, clusterId, connectorName string) (ConnectorConfig, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", environmentId, clusterId, connectorName)
	resp, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		errRes, _ := common.NewErrorResponse(resp.Body)
		if errRes != nil && errRes.Message != "" {
			return nil, fmt.Errorf("failed to get connector config: %s", errRes.Message)
		}
		return nil, fmt.Errorf("failed to get connector config: %s", resp.Status)
	}

	defer resp.Body.Close()

	var cfg ConnectorConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// DeleteConnector remove um connector existente do cluster Connect
func (c *ConfluentClient) DeleteConnector(environmentId, clusterId, connectorName string) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s", environmentId, clusterId, connectorName)
	resp, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to delete connector: %s", resp.Status)
	}

	return nil
}

// put UpdateConnectorConfig atualiza a configuração de um connector existente
func (c *ConfluentClient) UpdateConnectorConfig(environmentId, clusterId, connectorName string, newConfig ConnectorConfig) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", environmentId, clusterId, connectorName)
	resp, err := c.doRequest(urlPath, http.MethodPut, newConfig, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		errRes, _ := common.NewErrorResponse(resp.Body)
		if errRes != nil && errRes.Message != "" {
			return fmt.Errorf("failed to update connector config: %s", errRes.Message)
		}
		return fmt.Errorf("failed to update connector config: %s", resp.Status)
	}

	return nil
}
