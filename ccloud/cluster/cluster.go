package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type Resource struct {
	Related string `json:"related"`
}

type KafkaCluster struct {
	common.BaseModel
	ClusterId              string    `json:"cluster_id"`
	Controller             *Resource `json:"controller"`
	Acls                   *Resource `json:"acls"`
	Brokers                *Resource `json:"brokers"`
	BrokerConfigs          *Resource `json:"broker_configs"`
	ConsumerGroups         *Resource `json:"consumer_groups"`
	Topics                 *Resource `json:"topics"`
	PartitionReassignments *Resource `json:"partition_reassignments"`
}

func (c *ConfluentClusterClient) getCluster() (*KafkaCluster, error) {
	urlPath := fmt.Sprintf("/kafka/v3/clusters/%s", c.ClusterId)
	req, err := c.doRequest(urlPath, "", http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get cluster: %s", req.Status)
	}

	defer req.Body.Close()

	var cluster KafkaCluster
	err = json.NewDecoder(req.Body).Decode(&cluster)
	if err != nil {
		return nil, err
	}

	return &cluster, nil
}
