package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type KafkaConsumer struct {
	common.BaseModel
	ClusterId       string   `json:"cluster_id"`
	ConsumerGroupId string   `json:"consumer_group_id"`
	ConsumerId      string   `json:"consumer_id"`
	InstanceId      string   `json:"instance_id"`
	ClientId        string   `json:"client_id"`
	Assignments     Resource `json:"assignments"`
}

type KafkaConsumerList struct {
	common.BaseModel
	Data []KafkaConsumerGroup `json:"data"`
}

func (c *ConfluentClusterClient) ListConsumer(consumerGroupId string) (*KafkaConsumerList, error) {
	urlPath := fmt.Sprintf("%s/consumers", consumerGroupId)

	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to list consumer: %s", res.Status)
	}

	defer res.Body.Close()

	var list KafkaConsumerList
	err = json.NewDecoder(res.Body).Decode(&list)
	if err != nil {
		return nil, err
	}

	return &list, nil
}

func (c *ConfluentClusterClient) GetConsumer(consumerGroupId, consumerId string) (*KafkaConsumer, error) {
	urlPath := fmt.Sprintf("%s/consumers/%s", consumerGroupId, consumerId)

	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get consumer: %s", res.Status)
	}

	defer res.Body.Close()

	var consumer KafkaConsumer
	err = json.NewDecoder(res.Body).Decode(&consumer)
	if err != nil {
		return nil, err
	}

	return &consumer, nil
}

type KafkaConsumerLag struct {
	common.BaseModel
	ClusterId       string `json:"cluster_id"`
	ConsumerGroupId string `json:"consumer_group_id"`
	TopicName       string `json:"topic_name"`
	PartitionId     int    `json:"partition_id"`
	ConsumerId      string `json:"consumer_id"`
	InstanceId      string `json:"instance_id"`
	ClientId        string `json:"client_id"`
	CurrentOffset   int64  `json:"current_offset"`
	LogEndOffset    int64  `json:"log_end_offset"`
	Lag             int64  `json:"lag"`
}

type KafkaConsumerLagList struct {
	common.BaseModel
	Data []KafkaConsumerLag `json:"data"`
}

func (c *ConfluentClusterClient) ListConsumerLag(consumerGroupId string, opt *common.PaginationOptions) (*KafkaConsumerLagList, error) {
	urlPath := fmt.Sprintf("%s/lags", consumerGroupId)
	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get consumer group lag: %s", res.Status)
	}

	defer res.Body.Close()

	var lags KafkaConsumerLagList
	err = json.NewDecoder(res.Body).Decode(&lags)
	if err != nil {
		return nil, err
	}

	return &lags, nil
}
