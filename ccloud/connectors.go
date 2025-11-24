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

type ConnectorConfig interface {
	ToMap() map[string]interface{}
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
	KafkaApiKey           string
	KafkaApiSecret        string
	Transforms            *TransformsConfig
	AdditionalProperties  map[string]interface{}
}

type TransformsConfig struct {
	Name           string
	Type           string
	PartitionField string
	StaticField    string
	StaticValue    string
	TimestampField string
	TopicField     string
	OffsetField    string
}

func (s *S3SinkConnectorConfig) ToMap() map[string]interface{} {
	config := map[string]interface{}{
		"connector.class":         "S3_SINK",
		"s3.bucket.name":          s.Bucket,
		"provider.integration.id": s.ProviderIntegrationId,
		"topics":                  s.Topics,
		"tasks.max":               fmt.Sprintf("%d", s.TasksMax),
	}

	// Set authentication method with default
	if s.AuthenticationMethod != "" {
		config["authentication.method"] = s.AuthenticationMethod
	} else {
		config["authentication.method"] = "IAM Roles"
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

	if s.FlushSize > 0 {
		config["flush.size"] = fmt.Sprintf("%d", s.FlushSize)
	}

	if s.PartitionerClass != "" {
		config["partitioner.class"] = s.PartitionerClass
	}

	if s.TimeInterval != "" {
		config["time.interval"] = s.TimeInterval
	}

	if s.KafkaApiKey != "" {
		config["kafka.api.key"] = s.KafkaApiKey
	}

	if s.KafkaApiSecret != "" {
		config["kafka.api.secret"] = s.KafkaApiSecret
	}

	if s.Transforms != nil {
		s.Transforms.addToConfig(config)
	}

	for key, value := range s.AdditionalProperties {
		config[key] = value
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
	if t.OffsetField != "" {
		config[fmt.Sprintf("transforms.%s.offset.field", t.Name)] = t.OffsetField
	}
}

func (c *ConfluentClient) CreateConnectorTyped(environmentId, clusterId, name string, config ConnectorConfig) (*Connector, error) {
	return c.CreateConnector(environmentId, clusterId, name, config.ToMap())
}

func (c *ConfluentClient) UpdateConnectorTyped(environmentId, clusterId, name string, config ConnectorConfig) (*Connector, error) {
	return c.UpdateConnectorConfig(environmentId, clusterId, name, config.ToMap())
}

func (c *ConfluentClient) CreateConnector(environmentId, clusterId, name string, config map[string]interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)

	configCopy := make(map[string]interface{})
	for k, v := range config {
		configCopy[k] = v
	}
	configCopy["name"] = name

	payload := map[string]interface{}{
		"name":   name,
		"config": configCopy,
	}

	req, err := c.doRequest(urlPath, http.MethodPost, payload, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != req.StatusCode && http.StatusOK != req.StatusCode {
		defer req.Body.Close()

		var errorBody map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&errorBody); err == nil {
			errorMsg, _ := json.Marshal(errorBody)
			return nil, fmt.Errorf("failed to create connector: %s - Response: %s", req.Status, string(errorMsg))
		}
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

func (c *ConfluentClient) ListConnectors(environmentId, clusterId string) ([]Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list connectors: %s", req.Status)
	}

	defer req.Body.Close()

	var names []string
	err = json.NewDecoder(req.Body).Decode(&names)
	if err != nil {
		return nil, err
	}

	var connectors []Connector
	for _, name := range names {
		conn, err := c.GetConnector(environmentId, clusterId, name)
		if err != nil {
			return nil, err
		}
		if conn != nil {
			connectors = append(connectors, *conn)
		}
	}

	return connectors, nil
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

	configCopy := make(map[string]interface{})
	for k, v := range newConfig {
		configCopy[k] = v
	}
	configCopy["name"] = connectorName

	req, err := c.doRequest(urlPath, http.MethodPut, configCopy, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode && http.StatusCreated != req.StatusCode {
		defer req.Body.Close()

		var errorBody map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&errorBody); err == nil {
			errorMsg, _ := json.Marshal(errorBody)
			return nil, fmt.Errorf("failed to update connector config: %s - Response: %s", req.Status, string(errorMsg))
		}
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
