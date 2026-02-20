package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

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
	ConnectorClass        string                 `json:"connector.class,omitempty" default:"S3_SINK"`
	Bucket                string                 `json:"s3.bucket.name,omitempty"`
	AuthenticationMethod  string                 `json:"authentication.method,omitempty" default:"IAM Roles"`
	ProviderIntegrationId string                 `json:"provider.integration.id,omitempty"`
	Topics                string                 `json:"topics,omitempty"`
	InputDataFormat       string                 `json:"input.data.format,omitempty" default:"AVRO"`
	OutputDataFormat      string                 `json:"output.data.format,omitempty" default:"AVRO"`
	FlushSize             string                 `json:"flush.size,omitempty"`
	PartitionerClass      string                 `json:"partitioner.class,omitempty"`
	TimeInterval          string                 `json:"time.interval,omitempty"`
	TasksMax              string                 `json:"tasks.max,omitempty"`
	KafkaApiKey           string                 `json:"kafka.api.key,omitempty"`
	KafkaApiSecret        string                 `json:"kafka.api.secret,omitempty"`
	Transforms            *TransformsConfig      `json:"-"`
	AdditionalProperties  map[string]interface{} `json:"-"`
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

type ConnectorStatus struct {
	Name      string              `json:"name"`
	Connector ConnectorTaskStatus `json:"connector"`
	Tasks     []TaskStatus        `json:"tasks"`
}

type ConnectorTaskStatus struct {
	State    string `json:"state"`
	WorkerId string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

type TaskStatus struct {
	Id       int    `json:"id"`
	State    string `json:"state"`
	WorkerId string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

type ConnectorId struct {
	Id     string `json:"id"`
	IdType string `json:"id_type"`
}

type ConnectorInfo struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
	Type   string                 `json:"type"`
}

type ConnectorWithExpansions struct {
	Id     ConnectorId     `json:"id"`
	Info   ConnectorInfo   `json:"info"`
	Status ConnectorStatus `json:"status"`
}

type listConnectorsParams struct {
	Expand string `url:"expand,omitempty"`
}

func applyDefaults(configMap map[string]interface{}, config interface{}) {
	v := reflect.ValueOf(config)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		if !fieldValue.CanInterface() {
			continue
		}

		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		jsonName := jsonTag
		if idx := len(jsonTag); idx > 0 {
			for j, c := range jsonTag {
				if c == ',' {
					jsonName = jsonTag[:j]
					break
				}
			}
		}
		defaultValue := field.Tag.Get("default")
		if defaultValue == "" {
			continue
		}

		if _, exists := configMap[jsonName]; !exists || configMap[jsonName] == "" || configMap[jsonName] == 0 {
			switch fieldValue.Kind() {
			case reflect.String:
				if fieldValue.String() == "" {
					configMap[jsonName] = defaultValue
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if fieldValue.Int() == 0 {
					configMap[jsonName] = defaultValue
				}
			}
		}
	}
}

func (c *ConfluentClient) CreateConnector(environmentId, clusterId, name string, config interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)

	configBytes, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	var configMap map[string]interface{}
	if err := json.Unmarshal(configBytes, &configMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	applyDefaults(configMap, config)
	configMap["name"] = name

	payload := map[string]interface{}{
		"name":   name,
		"config": configMap,
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

func (c *ConfluentClient) GetConnectorStatus(environmentId, clusterId, connectorName string) (*ConnectorStatus, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/status", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get connector status: %s", req.Status)
	}

	defer req.Body.Close()

	var status ConnectorStatus
	err = json.NewDecoder(req.Body).Decode(&status)
	if err != nil {
		return nil, err
	}

	return &status, nil
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

func (c *ConfluentClient) PauseConnector(environmentId, clusterId, connectorName string) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/pause", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodPut, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusAccepted != req.StatusCode {
		return fmt.Errorf("failed to pause connector: %s", req.Status)
	}

	return nil
}

func (c *ConfluentClient) ResumeConnector(environmentId, clusterId, connectorName string) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/resume", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodPut, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusAccepted != req.StatusCode {
		return fmt.Errorf("failed to resume connector: %s", req.Status)
	}

	return nil
}

func (c *ConfluentClient) RestartConnector(environmentId, clusterId, connectorName string) error {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/restart", environmentId, clusterId, connectorName)
	req, err := c.doRequest(urlPath, http.MethodPost, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusAccepted != req.StatusCode {
		return fmt.Errorf("failed to restart connector: %s", req.Status)
	}

	return nil
}

func (c *ConfluentClient) UpdateConnectorConfig(environmentId, clusterId, connectorName string, newConfig interface{}) (*Connector, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors/%s/config", environmentId, clusterId, connectorName)

	configBytes, err := json.Marshal(newConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	var configMap map[string]interface{}
	if err := json.Unmarshal(configBytes, &configMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	applyDefaults(configMap, newConfig)
	configMap["name"] = connectorName

	req, err := c.doRequest(urlPath, http.MethodPut, configMap, nil)
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

func (c *ConfluentClient) ListConnectorsWithExpansions(environmentId, clusterId string, expand ...string) (map[string]ConnectorWithExpansions, error) {
	urlPath := fmt.Sprintf("/connect/v1/environments/%s/clusters/%s/connectors", environmentId, clusterId)

	expandStr := strings.Join(expand, ",")

	params := listConnectorsParams{Expand: expandStr}

	req, err := c.doRequest(urlPath, http.MethodGet, nil, params)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		defer req.Body.Close()
		return nil, fmt.Errorf("failed to list connectors with expansions: %s", req.Status)
	}

	defer req.Body.Close()

	var result map[string]ConnectorWithExpansions
	err = json.NewDecoder(req.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *ConfluentClient) GetConnectorWithExpansions(environmentId, clusterId, connectorName string, expand ...string) (*ConnectorWithExpansions, error) {
	connectors, err := c.ListConnectorsWithExpansions(environmentId, clusterId, expand...)
	if err != nil {
		return nil, err
	}

	if connector, ok := connectors[connectorName]; ok {
		return &connector, nil
	}

	return nil, fmt.Errorf("connector '%s' not found", connectorName)
}
