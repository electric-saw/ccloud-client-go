package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type SchemaRegistryCluster struct {
	common.BaseModel
	Spec struct {
		DisplayName string `json:"display_name"`
		Cloud       common.CloudProvider
		Region      string `json:"region"`
		Endpoint    string `json:"http_endpoint"`
		Package     string `json:"package"`
		Environment common.BaseModel
	}
}

type SchemaRegistryClusterList struct {
	common.BaseModel
	Data []SchemaRegistryCluster `json:"data"`
}

type SchemaRegistryClusterListOptions struct {
	common.PaginationOptions
	EnvironmentId string `url:"environment,omitempty"`
}

func (c *ConfluentClient) ListSchemaRegistry(opt *SchemaRegistryClusterListOptions) (*SchemaRegistryClusterList, error) {
	urlPath := "/srcm/v3/clusters"

	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list kafka clusters: %s", req.Status)
	}

	defer req.Body.Close()

	var schemaRegistryClusters SchemaRegistryClusterList
	err = json.NewDecoder(req.Body).Decode(&schemaRegistryClusters)
	if err != nil {
		return nil, err
	}

	return &schemaRegistryClusters, nil
}

func (c *ConfluentClient) GetSchemaRegistry(schemaRegistryId string, opt *SchemaRegistryClusterListOptions) (*SchemaRegistryCluster, error) {
	urlPath := fmt.Sprintf("/srcm/v3/clusters/%s", schemaRegistryId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get kafka cluster: %s", req.Status)
	}

	defer req.Body.Close()

	var schemaRegistryCluster SchemaRegistryCluster
	err = json.NewDecoder(req.Body).Decode(&schemaRegistryCluster)
	if err != nil {
		return nil, err
	}

	return &schemaRegistryCluster, nil
}
