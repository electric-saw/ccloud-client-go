package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type KafkaPartitionConsumerLag struct {
	common.BaseModel
	ClusterId       string `json:"cluster_id"`
	ConsumerGroupId string `json:"consumer_group_id"`
	TopicName       string `json:"topic_name"`
	PartitionId     int    `json:"partition_id"`
	ConsumerId      string `json:"consumer_id"`
	InstanceId      string `json:"instance_id"`
	ClientId        string `json:"client_id"`
	CurrentOffset   int    `json:"current_offset"`
	LogEndOffset    int    `json:"log_end_offset"`
	Lag             int    `json:"lag"`
}

func (c *ConfluentClusterClient) GetConsumerLag(consumerGroupId, topicName string, partitionId int) (*KafkaPartitionConsumerLag, error) {
	urlPath := fmt.Sprintf("/topics/%s/lags/%s/partitions/%d", consumerGroupId, topicName, partitionId)
	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get consumer lag: %s", res.Status)
	}

	defer res.Body.Close()

	var lag KafkaPartitionConsumerLag
	err = json.NewDecoder(res.Body).Decode(&lag)
	if err != nil {
		return nil, err
	}

	return &lag, nil
}

type KafkaPartition struct {
	common.BaseModel
	ClusterId    string   `json:"cluster_id"`
	TopicName    string   `json:"topic_name"`
	PartitionId  int      `json:"partition_id"`
	Leader       Resource `json:"leader"`
	Replicas     Resource `json:"replicas"`
	Reassignment Resource `json:"reassignment"`
}

type KafkaPartitionList struct {
	common.BaseModel
	Data []KafkaPartition `json:"data"`
}

func (c *ConfluentClusterClient) ListPartitions(topicName string) (*KafkaPartitionList, error) {
	urlPath := fmt.Sprintf("/%s/partitions", topicName)
	res, err := c.doRequest(c.clusterInfo.Topics.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to list partitions: %s", res.Status)
	}

	defer res.Body.Close()

	var partitions KafkaPartitionList
	err = json.NewDecoder(res.Body).Decode(&partitions)
	if err != nil {
		return nil, err
	}

	return &partitions, nil
}

func (c *ConfluentClusterClient) GetPartition(topicName string, partitionId int) (*KafkaPartition, error) {
	urlPath := fmt.Sprintf("/%s/partitions/%d", topicName, partitionId)
	res, err := c.doRequest(c.clusterInfo.Topics.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get partition: %s", res.Status)
	}

	defer res.Body.Close()

	var partition KafkaPartition
	err = json.NewDecoder(res.Body).Decode(&partition)
	if err != nil {
		return nil, err
	}

	return &partition, nil
}
