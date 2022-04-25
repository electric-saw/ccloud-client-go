package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type Topic struct {
	common.BaseModel
	ClusterId              string   `json:"cluster_id"`
	TopicName              string   `json:"topic_name"`
	IsInternal             bool     `json:"is_internal"`
	ReplicationFactor      int      `json:"replication_factor"`
	PartitionCount         int      `json:"partition_count"`
	Partitions             Resource `json:"partitions"`
	Configs                Resource `json:"configs"`
	PartitionReassignments Resource `json:"partition_reassignments"`
}

type TopicList struct {
	common.BaseModel
	Data []Topic `json:"data"`
}

func (c *ConfluentClusterClient) ListTopics(opts *common.PaginationOptions) (*TopicList, error) {
	req, err := c.doRequest(c.clusterInfo.Topics.Related, "", http.MethodGet, nil, opts)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list topics: %s", req.Status)
	}

	defer req.Body.Close()

	var topicList TopicList
	err = json.NewDecoder(req.Body).Decode(&topicList)
	if err != nil {
		return nil, err
	}

	return &topicList, nil
}

func (c *ConfluentClusterClient) GetTopic(topicId string) (*Topic, error) {
	req, err := c.doRequest(c.clusterInfo.Topics.Related, topicId, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get topic: %s", req.Status)
	}

	defer req.Body.Close()

	var topic Topic
	err = json.NewDecoder(req.Body).Decode(&topic)
	if err != nil {
		return nil, err
	}

	return &topic, nil
}

type TopicCreateReq struct {
	TopicName         string `json:"topic_name"`
	PartitionCount    int    `json:"partition_count"`
	ReplicationFactor int    `json:"replication_factor"`
	// ReplicasAssignment map[string][]int `json:"replicas_assignment"` TODO: implement this
	Configs []KafkaConfigUpdateItem `json:"configs"`
}

func (c *ConfluentClusterClient) CreateTopic(req *TopicCreateReq) (*Topic, error) {
	res, err := c.doRequest(c.clusterInfo.Topics.Related, "", http.MethodPost, req, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusCreated != res.StatusCode {
		return nil, fmt.Errorf("failed to create topic: %s", res.Status)
	}

	defer res.Body.Close()

	var topic Topic
	err = json.NewDecoder(res.Body).Decode(&topic)
	if err != nil {
		return nil, err
	}

	return &topic, nil
}

func (c *ConfluentClusterClient) DeleteTopic(topicId string) error {
	req, err := c.doRequest(c.clusterInfo.Topics.Related, topicId, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete topic: %s", req.Status)
	}

	return nil
}
