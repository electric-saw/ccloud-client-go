package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type KafkaConsumerGroup struct {
	common.BaseModel
	ClusterId         string   `json:"cluster_id"`
	ConsumerGroupId   string   `json:"consumer_group_id"`
	IsSimple          bool     `json:"is_simple"`
	PartitionAssignor string   `json:"partition_assignor"`
	State             string   `json:"state"`
	Coordinator       Resource `json:"coordinator"`
	Consumers         Resource `json:"consumers"`
	LagSummary        Resource `json:"lag_summary"`
}

type KafkaConsumerGroupList struct {
	common.BaseModel
	Data []KafkaConsumerGroup `json:"data"`
}

func (c *ConfluentClusterClient) ListConsumerGroups() (*KafkaConsumerGroupList, error) {
	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, "", http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to list consumer groups: %s", res.Status)
	}

	defer res.Body.Close()

	var list KafkaConsumerGroupList
	err = json.NewDecoder(res.Body).Decode(&list)
	if err != nil {
		return nil, err
	}

	return &list, nil
}

func (c *ConfluentClusterClient) GetConsumerGroup(consumerGroupId string) (*KafkaConsumerGroup, error) {
	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, consumerGroupId, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get consumer group: %s", res.Status)
	}

	defer res.Body.Close()

	var group KafkaConsumerGroup
	err = json.NewDecoder(res.Body).Decode(&group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

type KafkaConsumerGroupLag struct {
	common.BaseModel
	ClusterId         string   `json:"cluster_id"`
	ConsumerGroupId   string   `json:"consumer_group_id"`
	MaxLagConsumerId  string   `json:"max_lag_consumer_id"`
	MaxLagInstanceId  string   `json:"max_lag_instance_id"`
	MaxLagClientId    string   `json:"max_lag_client_id"`
	MaxLagTopicName   string   `json:"max_lag_topic_name"`
	MaxLagPartitionId int      `json:"max_lag_partition_id"`
	MaxLag            int64    `json:"max_lag"`
	TotalLag          int64    `json:"total_lag"`
	MaxLagConsumer    Resource `json:"max_lag_consumer"`
	MaxLagPartition   Resource `json:"max_lag_partition"`
}

func (c *ConfluentClusterClient) GetConsumerGroupLag(consumerGroupId string) (*KafkaConsumerGroupLag, error) {
	urlPath := fmt.Sprintf("%s/lag-summary", consumerGroupId)
	res, err := c.doRequest(c.clusterInfo.ConsumerGroups.Related, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != res.StatusCode {
		return nil, fmt.Errorf("failed to get consumer group lag: %s", res.Status)
	}

	defer res.Body.Close()

	var lag KafkaConsumerGroupLag
	err = json.NewDecoder(res.Body).Decode(&lag)
	if err != nil {
		return nil, err
	}

	return &lag, nil
}
