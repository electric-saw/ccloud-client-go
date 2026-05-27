package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

func (c *ConfluentClusterClient) ListKafkaConfigs(opt *common.PaginationOptions) (*KafkaConfigList, error) {
	req, err := c.doRequest(c.clusterInfo.BrokerConfigs.Related, "", http.MethodGet, nil, opt)
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

func (c *ConfluentClusterClient) GetKafkaConfig(configName string) (*KafkaConfig, error) {
	req, err := c.doRequest(c.clusterInfo.BrokerConfigs.Related, configName, http.MethodGet, nil, nil)
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

func (c *ConfluentClusterClient) UpdateKafkaConfig(configName string, req *KafkaConfigUpdateReq) error {
	res, err := c.doRequest(c.clusterInfo.BrokerConfigs.Related, configName, http.MethodPut, req, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to update kafka config: %s", res.Status)
	}

	return nil
}

func (c *ConfluentClusterClient) UpdateKafkaConfigBatch(req *KafkaConfigUpdateBatch) error {
	urlPath := fmt.Sprintf("%s:alter", c.clusterInfo.BrokerConfigs.Related)
	res, err := c.doRequest(c.clusterInfo.BrokerConfigs.Related, urlPath, http.MethodPost, req, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to update kafka config: %s", res.Status)
	}

	return nil
}

func (c *ConfluentClusterClient) ResetKafkaConfig(configName string) error {
	res, err := c.doRequest(c.clusterInfo.BrokerConfigs.Related, configName, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != res.StatusCode {
		return fmt.Errorf("failed to delete kafka config: %s", res.Status)
	}

	return nil
}
