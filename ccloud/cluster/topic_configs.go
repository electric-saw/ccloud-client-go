package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

func (c *ConfluentClusterClient) ListTopicConfigs(topicName string, opt *common.PaginationOptions) (*KafkaConfigList, error) {
	path := fmt.Sprintf("%s/configs", topicName)

	req, err := c.doRequest(c.clusterInfo.Topics.Related, path, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list kafka configs: %s", req.Status)
	}

	defer req.Body.Close()

	var configList KafkaConfigList
	err = json.NewDecoder(req.Body).Decode(&configList)
	if err != nil {
		return nil, err
	}

	return &configList, nil
}

func (c *ConfluentClusterClient) GetTopicConfig(topicName, configName string) (*KafkaConfig, error) {
	path := fmt.Sprintf("%s/configs/%s", topicName, configName)

	req, err := c.doRequest(c.clusterInfo.Topics.Related, path, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get kafka config: %s", req.Status)
	}

	defer req.Body.Close()

	var config KafkaConfig
	err = json.NewDecoder(req.Body).Decode(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *ConfluentClusterClient) UpdateTopicConfig(topicName, configName string, req *KafkaConfigUpdateReq) error {
	path := fmt.Sprintf("%s/configs/%s", topicName, configName)

	res, err := c.doRequest(c.clusterInfo.Topics.Related, path, http.MethodPut, req, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to update kafka config: %s", res.Status)
	}

	return nil
}

func (c *ConfluentClusterClient) UpdateTopicConfigBatch(topicName string, req *KafkaConfigUpdateBatch) error {
	path := fmt.Sprintf("%s/configs:alter", topicName)

	res, err := c.doRequest(c.clusterInfo.Topics.Related, path, http.MethodPost, req, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to update kafka config: %s", res.Status)
	}

	return nil
}

func (c *ConfluentClusterClient) ResetTopicConfig(topicName, configName string) error {
	path := fmt.Sprintf("%s/configs/%s", topicName, configName)
	res, err := c.doRequest(c.clusterInfo.Topics.Related, path, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to delete kafka config: %s", res.Status)
	}

	return nil
}
