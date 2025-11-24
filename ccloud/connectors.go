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
	Type   string                 `json:"type,omitempty"`
	Tasks  []Task                 `json:"tasks,omitempty"`
}

type Task struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}

type ConnectorList struct {
	common.BaseModel
	Data []Connector `json:"data"`
}

type S3SinkConnectorConfig struct {
	Bucket                string
	AuthenticationMethod  string
	ProviderIntegrationId string
	Topics                string
	InputDataFormat       string
	OutputDataFormat      string
	FlushSize             int
	PartitionerClass      string
	TimeInterval          string
	TasksMax              int
	Transforms            *TransformsConfig
}

type TransformsConfig struct {
	Name           string
	Type           string
	PartitionField string
	StaticField    string
	StaticValue    string
	TimestampField string
	TopicField     string
}

func (s *S3SinkConnectorConfig) ToMap() map[string]interface{} {
	config := map[string]interface{}{
		"connector.class":         "S3_SINK",
		"s3.bucket.name":          s.Bucket,
		"authentication.method":   s.AuthenticationMethod,
		"provider.integration.id": s.ProviderIntegrationId,
		"topics":                  s.Topics,
		"tasks.max":               fmt.Sprintf("%d", s.TasksMax),
	}

	if s.InputDataFormat != "" {
		config["input.data.format"] = s.InputDataFormat
	} else {
		config["input.data.format"] = "AVRO"
	}

	if s.OutputDataFormat != "" {
		config["output.data.format"] = s.OutputDataFormat
	} else {
		config["output.data.format"] = "AVRO"
	}

	if s.Transforms != nil {
		s.Transforms.addToConfig(config)
	}

	return config
}

func (t *TransformsConfig) addToConfig(config map[string]interface{}) {
	if t.Name == "" {
		return
	}

	config["transforms"] = t.Name

	if t.Type != "" {
		config[fmt.Sprintf("transforms.%s.type", t.Name)] = t.Type
	}
	if t.PartitionField != "" {
		config[fmt.Sprintf("transforms.%s.partition.field", t.Name)] = t.PartitionField
	}
	if t.StaticField != "" {
		config[fmt.Sprintf("transforms.%s.static.field", t.Name)] = t.StaticField
	}
	if t.StaticValue != "" {
		config[fmt.Sprintf("transforms.%s.static.value", t.Name)] = t.StaticValue
	}
	if t.TimestampField != "" {
		config[fmt.Sprintf("transforms.%s.timestamp.field", t.Name)] = t.TimestampField
	}
	if t.TopicField != "" {
		config[fmt.Sprintf("transforms.%s.topic.field", t.Name)] = t.TopicField
	}
}

func (c *ConfluentClient) CreateConnector(environmentId, clusterId, name string, config map[string]interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)

	connector := &Connector{
		Name:   name,
		Config: config,
	}

	req, err := c.doRequest(urlPath, http.MethodPost, connector, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != req.StatusCode && http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to create connector: %s", req.Status)
	}

	defer req.Body.Close()

	var result Connector
	err = json.NewDecoder(req.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *ConfluentClient) CreateS3SinkConnector(environmentId, clusterId, name string, config *S3SinkConnectorConfig) (*Connector, error) {
	return c.CreateConnector(environmentId, clusterId, name, config.ToMap())
}

func (c *ConfluentClient) ListConnectors(environmentId, clusterId string) (*ConnectorList, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list connectors: %s", req.Status)
	}

	defer req.Body.Close()

	var connectors ConnectorList
	err = json.NewDecoder(req.Body).Decode(&connectors)
	if err != nil {
		return nil, err
	}

	return &connectors, nil
}

func (c *ConfluentClient) GetConnector(environmentId, clusterId, connectorName string) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get connector: %s", req.Status)
	}

	defer req.Body.Close()

	var connector Connector
	err = json.NewDecoder(req.Body).Decode(&connector)
	if err != nil {
		return nil, err
	}

	return &connector, nil
}

func (c *ConfluentClient) GetConnectorConfig(environmentId, clusterId, connectorName string) (map[string]interface{}, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get connector config: %s", req.Status)
	}

	defer req.Body.Close()

	var cfg map[string]interface{}
	err = json.NewDecoder(req.Body).Decode(&cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *ConfluentClient) DeleteConnector(environmentId, clusterId, connectorName string) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusNoContent != req.StatusCode && http.StatusAccepted != req.StatusCode {
		return fmt.Errorf("failed to delete connector: %s", req.Status)
	}

	return nil
}

func (c *ConfluentClient) UpdateConnectorConfig(environmentId, clusterId, connectorName string, newConfig map[string]interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodPut, newConfig, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode && http.StatusCreated != req.StatusCode {
		return nil, fmt.Errorf("failed to update connector config: %s", req.Status)
	}

	defer req.Body.Close()

	var connector Connector
	err = json.NewDecoder(req.Body).Decode(&connector)
	if err != nil {
		return nil, err
	}

	return &connector, nil
}
