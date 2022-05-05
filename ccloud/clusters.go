package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type KafkaClusterAvailability string

const (
	KafkaClusterAvailabilitySingleZone KafkaClusterAvailability = "SINGLE_ZONE"
	KafkaClusterAvailabilityMultiZone  KafkaClusterAvailability = "MULTI_ZONE"
)

type KafkaClusterKind string

const (
	KafkaClusterKindBasic     KafkaClusterKind = "Basic"
	KafkaClusterKindStandard  KafkaClusterKind = "Standard"
	KafkaClusterKindDedicated KafkaClusterKind = "Dedicated"
)

type KafkaCluster struct {
	common.BaseModel
	Spec struct {
		DisplayName            string                   `json:"display_name"`
		Availability           KafkaClusterAvailability `json:"availability"`
		Cloud                  common.CloudProvider     `json:"cloud"`
		Region                 string                   `json:"region"`
		KafkaBootstrapEndpoint string                   `json:"kafka_bootstrap_endpoint"`
		HttpEndpoint           string                   `json:"http_endpoint"`
		Config                 struct {
			Kind  KafkaClusterKind `json:"kind"`
			Cku   int              `json:"cku"`
			Zones []string         `json:"zones"`
		}
		Network struct {
			common.BaseModel
		}
		Environment Environment
	}
	Status struct {
		Phase string `json:"phase"`
		CKU   int    `json:"cku"`
	}
}

type KafkaClusterList struct {
	common.BaseModel
	Data []KafkaCluster `json:"data"`
}

type KafkaClusterListOptions struct {
	common.PaginationOptions
	EnvironmentId string `url:"environment,omitempty"`
}

func (c *ConfluentClient) ListKafkaClusters(opt *KafkaClusterListOptions) (*KafkaClusterList, error) {
	urlPath := "/cmk/v2/clusters"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list kafka clusters: %s", req.Status)
	}

	defer req.Body.Close()

	var KafkaClusters KafkaClusterList
	err = json.NewDecoder(req.Body).Decode(&KafkaClusters)
	if err != nil {
		return nil, err
	}

	return &KafkaClusters, nil
}

func (c *ConfluentClient) GetKafkaCluster(KafkaClusterId string, opt *KafkaClusterListOptions) (*KafkaCluster, error) {
	urlPath := fmt.Sprintf("/cmk/v2/clusters/%s", KafkaClusterId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get kafka cluster: %s", req.Status)
	}

	defer req.Body.Close()

	var KafkaCluster KafkaCluster
	err = json.NewDecoder(req.Body).Decode(&KafkaCluster)
	if err != nil {
		return nil, err
	}

	return &KafkaCluster, nil
}

type KafkaClusterCreateReq struct {
	DisplayName  string                   `json:"display_name"`
	Availability KafkaClusterAvailability `json:"availability"`
	Cloud        common.CloudProvider     `json:"cloud"`
	Region       string                   `json:"region"`
	Config       struct {
		Kind KafkaClusterKind `json:"kind"`
		CKU  int              `json:"cku"`
	} `json:"config"`
	Environment struct {
		Id          string `json:"id"`
		Environment string `json:"environment,omitempty"`
	} `json:"environment"`
}

func (c *ConfluentClient) CreateKafkaCluster(create *KafkaClusterCreateReq) (*KafkaCluster, error) {
	urlPath := "/cmk/v2/clusters"
	req, err := c.doRequest(urlPath, http.MethodPost, specWrap{create}, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusAccepted != req.StatusCode {
		return nil, fmt.Errorf("failed to create service account: %s", req.Status)
	}

	defer req.Body.Close()

	var KafkaCluster KafkaCluster
	err = json.NewDecoder(req.Body).Decode(&KafkaCluster)
	if err != nil {
		return nil, err
	}

	return &KafkaCluster, nil
}

type KafkaClusterUpdateReq struct {
	DisplayName string `json:"display_name"`
	Config      struct {
		Kind KafkaClusterKind `json:"kind"`
		CKU  int              `json:"cku"`
	} `json:"config"`
	Environment struct {
		Id          string `json:"id"`
		Environment string `json:"environment,omitempty"`
	} `json:"environment"`
}

func (c *ConfluentClient) UpdateKafkaCluster(KafkaClusterId string, update *KafkaClusterUpdateReq) (*KafkaCluster, error) {
	urlPath := fmt.Sprintf("/cmk/v2/clusters/%s", KafkaClusterId)
	req, err := c.doRequest(urlPath, http.MethodPatch, specWrap{update}, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get kafka cluster: %s", req.Status)
	}

	defer req.Body.Close()

	var KafkaCluster KafkaCluster
	err = json.NewDecoder(req.Body).Decode(&KafkaCluster)
	if err != nil {
		return nil, err
	}

	return &KafkaCluster, nil
}

func (c *ConfluentClient) DeleteKafkaCluster(KafkaClusterId string, opt KafkaClusterListOptions) error {
	urlPath := fmt.Sprintf("/cmk/v2/clusters/%s", KafkaClusterId)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, opt)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete kafka cluster: %s", req.Status)
	}

	return nil
}
