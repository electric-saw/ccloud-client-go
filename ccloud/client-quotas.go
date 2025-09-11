package ccloud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type ClientQuotaSpec struct {
	DisplayName string                  `json:"display_name,omitempty"`
	Description string                  `json:"description,omitempty"`
	Throughput  *ClientQuotaThroughput  `json:"throughput,omitempty"`
	Cluster     *ClientQuotaCluster     `json:"cluster,omitempty"`
	Principals  []ClientQuotaPrincipal  `json:"principals,omitempty"`
	Environment *ClientQuotaEnvironment `json:"environment,omitempty"`
}

type ClientQuotaEnvironment struct {
	ID           string `json:"id,omitempty"`
	Related      string `json:"related,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
}

type ClientQuotaThroughput struct {
	IngressByteRate       string `json:"ingress_byte_rate,omitempty"`
	EgressByteRate        string `json:"egress_byte_rate,omitempty"`
	IngressBytesPerSecond string `json:"ingress_bytes_per_second,omitempty"`
	EgressBytesPerSecond  string `json:"egress_bytes_per_second,omitempty"`
}

type ClientQuotaCluster struct {
	ID           string `json:"id"`
	Environment  string `json:"environment,omitempty"`
	Related      string `json:"related,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
}

type ClientQuotaPrincipal struct {
	ID           string `json:"id,omitempty"`
	Related      string `json:"related,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
}

type ClientQuota struct {
	ApiVersion string          `json:"api_version,omitempty"`
	Kind       string          `json:"kind,omitempty"`
	ID         string          `json:"id,omitempty"`
	Metadata   ClientMetadata  `json:"metadata,omitempty"`
	Spec       ClientQuotaSpec `json:"spec,omitempty"`
}

func (c *ClientQuota) String() string {
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Client Quota ID: %s\n", c.ID))
	result.WriteString(fmt.Sprintf("Display Name: %s\n", c.Spec.DisplayName))
	result.WriteString(fmt.Sprintf("Description: %s\n", c.Spec.Description))

	if c.Spec.Throughput != nil {
		result.WriteString("Throughput:\n")
		result.WriteString(fmt.Sprintf("  Ingress Byte Rate: %s\n", c.Spec.Throughput.IngressByteRate))
		result.WriteString(fmt.Sprintf("  Egress Byte Rate: %s\n", c.Spec.Throughput.EgressByteRate))
	}

	if c.Spec.Cluster != nil {
		result.WriteString("Cluster:\n")
		result.WriteString(fmt.Sprintf("  ID: %s\n", c.Spec.Cluster.ID))
		result.WriteString(fmt.Sprintf("  Environment: %s\n", c.Spec.Cluster.Environment))
		result.WriteString(fmt.Sprintf("  Resource Name: %s\n", c.Spec.Cluster.ResourceName))
	}

	if len(c.Spec.Principals) > 0 {
		result.WriteString(fmt.Sprintf("Principals (%d):\n", len(c.Spec.Principals)))
		for i, p := range c.Spec.Principals {
			result.WriteString(fmt.Sprintf("  %d. ID: %s\n", i+1, p.ID))
			if p.Related != "" {
				result.WriteString(fmt.Sprintf("     Related: %s\n", p.Related))
			}
		}
	}

	if c.Spec.Environment != nil {
		result.WriteString("Environment:\n")
		result.WriteString(fmt.Sprintf("  ID: %s\n", c.Spec.Environment.ID))
		if c.Spec.Environment.Related != "" {
			result.WriteString(fmt.Sprintf("  Related: %s\n", c.Spec.Environment.Related))
		}
		if c.Spec.Environment.ResourceName != "" {
			result.WriteString(fmt.Sprintf("  Resource Name: %s\n", c.Spec.Environment.ResourceName))
		}
	}

	return result.String()
}

type ClientQuotaDetail struct {
	ApiVersion string                `json:"api_version,omitempty"`
	Kind       string                `json:"kind,omitempty"`
	ID         string                `json:"id,omitempty"`
	Metadata   ClientMetadata        `json:"metadata,omitempty"`
	Spec       ClientQuotaDetailSpec `json:"spec,omitempty"`
}

func (c *ClientQuotaDetail) String() string {
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Client Quota ID: %s\n", c.ID))
	result.WriteString(fmt.Sprintf("Display Name: %s\n", c.Spec.DisplayName))
	result.WriteString(fmt.Sprintf("Description: %s\n", c.Spec.Description))

	if c.Spec.Throughput != nil {
		result.WriteString("Throughput:\n")
		result.WriteString(fmt.Sprintf("  Ingress Byte Rate: %s\n", c.Spec.Throughput.IngressByteRate))
		result.WriteString(fmt.Sprintf("  Egress Byte Rate: %s\n", c.Spec.Throughput.EgressByteRate))
	}

	if c.Spec.Cluster != nil {
		result.WriteString("Cluster:\n")
		result.WriteString(fmt.Sprintf("  ID: %s\n", c.Spec.Cluster.ID))
		result.WriteString(fmt.Sprintf("  Environment: %s\n", c.Spec.Cluster.Environment))
		result.WriteString(fmt.Sprintf("  Resource Name: %s\n", c.Spec.Cluster.ResourceName))
	}

	if len(c.Spec.Principals) > 0 {
		result.WriteString(fmt.Sprintf("Principals (%d):\n", len(c.Spec.Principals)))
		for i, p := range c.Spec.Principals {
			result.WriteString(fmt.Sprintf("  %d. ID: %s\n", i+1, p.ID))
			if p.Related != "" {
				result.WriteString(fmt.Sprintf("     Related: %s\n", p.Related))
			}
		}
	}

	if c.Spec.Environment != nil {
		result.WriteString("Environment:\n")
		result.WriteString(fmt.Sprintf("  ID: %s\n", c.Spec.Environment.ID))
		if c.Spec.Environment.Related != "" {
			result.WriteString(fmt.Sprintf("  Related: %s\n", c.Spec.Environment.Related))
		}
		if c.Spec.Environment.ResourceName != "" {
			result.WriteString(fmt.Sprintf("  Resource Name: %s\n", c.Spec.Environment.ResourceName))
		}
	}

	return result.String()
}

type ClientQuotaDetailSpec = ClientQuotaSpec

type ClientMetadata struct {
	Self         string `json:"self,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
	DeletedAt    string `json:"deleted_at,omitempty"`
}

type ClientQuotaList struct {
	ApiVersion string        `json:"api_version,omitempty"`
	Kind       string        `json:"kind,omitempty"`
	Metadata   ListMetadata  `json:"metadata,omitempty"`
	Data       []ClientQuota `json:"data"`
}

type ListMetadata struct {
	First     string `json:"first,omitempty"`
	Last      string `json:"last,omitempty"`
	Prev      string `json:"prev,omitempty"`
	Next      string `json:"next,omitempty"`
	TotalSize int    `json:"total_size,omitempty"`
}

type ClientQuotaListOptions struct {
	common.PaginationOptions
	Cluster     string `url:"spec.cluster,omitempty"`
	Environment string `url:"environment,omitempty"`
}

type ClientQuotaCreateReq struct {
	DisplayName string                  `json:"display_name,omitempty"`
	Description string                  `json:"description,omitempty"`
	Throughput  *ClientQuotaThroughput  `json:"throughput,omitempty"`
	Cluster     *ClientQuotaCluster     `json:"cluster"`
	Principals  []ClientQuotaPrincipal  `json:"principals"`
	Environment *ClientQuotaEnvironment `json:"environment,omitempty"`
}

type ClientQuotaUpdateReq struct {
	DisplayName string                  `json:"display_name,omitempty"`
	Description string                  `json:"description,omitempty"`
	Throughput  *ClientQuotaThroughput  `json:"throughput,omitempty"`
	Principals  []ClientQuotaPrincipal  `json:"principals,omitempty"`
	Environment *ClientQuotaEnvironment `json:"environment,omitempty"`
}

func (c *ConfluentClient) ListClientQuotas(opt *ClientQuotaListOptions) (*ClientQuotaList, error) {
	if opt == nil {
		return nil, fmt.Errorf("client quota list options cannot be nil")
	}

	urlPath := "/kafka-quotas/v1/client-quotas"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		bodyBytes, _ := io.ReadAll(req.Body)
		defer req.Body.Close()
		return nil, fmt.Errorf("failed to list client quotas: status=%s, body=%s", req.Status, string(bodyBytes))
	}

	defer req.Body.Close()

	bodyBytes, _ := io.ReadAll(req.Body)

	var clientQuotas ClientQuotaList
	err = json.Unmarshal(bodyBytes, &clientQuotas)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w, body: %s", err, string(bodyBytes))
	}

	return &clientQuotas, nil
}

func (c *ConfluentClient) GetClientQuota(id string) (*ClientQuotaDetail, error) {
	urlPath := fmt.Sprintf("/kafka-quotas/v1/client-quotas/%s", id)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		bodyBytes, _ := io.ReadAll(req.Body)
		defer req.Body.Close()
		return nil, fmt.Errorf("failed to get client quota: status=%s, body=%s", req.Status, string(bodyBytes))
	}

	defer req.Body.Close()

	bodyBytes, _ := io.ReadAll(req.Body)

	var clientQuota ClientQuotaDetail
	err = json.Unmarshal(bodyBytes, &clientQuota)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w, body: %s", err, string(bodyBytes))
	}

	return &clientQuota, nil
}

func (c *ConfluentClient) CreateClientQuota(create *ClientQuotaCreateReq) (*ClientQuotaDetail, error) {
	urlPath := "/kafka-quotas/v1/client-quotas"
	req, err := c.doRequest(urlPath, http.MethodPost, specWrap{create}, nil)
	if err != nil {
		return nil, err
	}

	defer req.Body.Close()

	if http.StatusAccepted != req.StatusCode {
		bodyBytes, _ := io.ReadAll(req.Body)
		return nil, fmt.Errorf("failed to create client quota: status=%s, body=%s", req.Status, string(bodyBytes))
	}

	bodyBytes, _ := io.ReadAll(req.Body)

	var clientQuota ClientQuotaDetail
	err = json.Unmarshal(bodyBytes, &clientQuota)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w, body: %s", err, string(bodyBytes))
	}

	return &clientQuota, nil
}

func (c *ConfluentClient) UpdateClientQuota(id string, update *ClientQuotaUpdateReq) (*ClientQuotaDetail, error) {
	urlPath := fmt.Sprintf("/kafka-quotas/v1/client-quotas/%s", id)
	req, err := c.doRequest(urlPath, http.MethodPatch, specWrap{update}, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		bodyBytes, _ := io.ReadAll(req.Body)
		defer req.Body.Close()
		return nil, fmt.Errorf("failed to update client quota: status=%s, body=%s", req.Status, string(bodyBytes))
	}

	defer req.Body.Close()

	bodyBytes, _ := io.ReadAll(req.Body)

	var clientQuota ClientQuotaDetail
	err = json.Unmarshal(bodyBytes, &clientQuota)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w, body: %s", err, string(bodyBytes))
	}

	return &clientQuota, nil
}

func (c *ConfluentClient) DeleteClientQuota(id string) error {
	urlPath := fmt.Sprintf("/kafka-quotas/v1/client-quotas/%s", id)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusNoContent != req.StatusCode {
		bodyBytes, _ := io.ReadAll(req.Body)
		defer req.Body.Close()
		return fmt.Errorf("failed to delete client quota: status=%s, body=%s", req.Status, string(bodyBytes))
	}

	defer req.Body.Close()

	return nil
}
