package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type ClusterLinking struct {
	common.BaseModel
	Data []struct {
		common.BaseModel
		SourceClusterId      string   `json:"source_cluster_id,omitempty"`
		DestinationClusterId string   `json:"destination_cluster_id,omitempty"`
		RemoteClusterId      string   `json:"remote_cluster_id,omitempty"`
		LinkName             string   `json:"link_name,omitempty"`
		LinkId               string   `json:"link_id,omitempty"`
		ClusterLinkId        string   `json:"cluster_link_id,omitempty"`
		TopicNames           []string `json:"topic_names,omitempty"`
		LinkError            string   `json:"link_error,omitempty"`
		LinkErrorMessage     string   `json:"link_error_message,omitempty"`
		LinkState            string   `json:"link_state,omitempty"`
		Tasks                []string `json:"tasks,omitempty"`
	} `json:"data,omitempty"`
}

type ClusterLinkingConfig struct {
	common.BaseModel
	Data []struct {
		common.BaseModel
		ClusterId   string   `json:"cluster_id,omitempty"`
		Name        string   `json:"name,omitempty"`
		Value       string   `json:"value,omitempty"`
		ReadOnly    bool     `json:"is_read_only,omitempty"`
		Source      string   `json:"source,omitempty"`
		IsSensitive bool     `json:"is_sensitive,omitempty"`
		LinkName    string   `json:"link_name,omitempty"`
		IsDefault   bool     `json:"is_default,omitempty"`
		Synonyms    []string `json:"synonyms,omitempty"`
	} `json:"data,omitempty"`
}

type MirrorTopicReq struct {
	SourceTopicName string `json:"source_topic_name"`
	MirrorTopicName string `json:"mirror_topic_name"`
}

func (c *ConfluentClusterClient) GetClusterLinking() (*ClusterLinking, error) {
	urlPath := fmt.Sprintf("/kafka/v3/clusters/%s/links", c.ClusterId)
	req, err := c.doRequest(c.BaseUrl, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get cluster linkings: %s", req.Status)
	}

	defer req.Body.Close()

	var clusterLinking ClusterLinking
	err = json.NewDecoder(req.Body).Decode(&clusterLinking)
	if err != nil {
		return nil, err
	}

	return &clusterLinking, nil
}

func (c *ConfluentClusterClient) GetClusterLinkingConfig(linkName string) (*ClusterLinkingConfig, error) {
	urlPath := fmt.Sprintf("/kafka/v3/clusters/%s/links/%s/configs", c.ClusterId, linkName)
	req, err := c.doRequest(c.BaseUrl, urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get cluster linking config: %s", req.Status)
	}

	defer req.Body.Close()

	var clusterLinkingConfig ClusterLinkingConfig
	err = json.NewDecoder(req.Body).Decode(&clusterLinkingConfig)
	if err != nil {
		return nil, err
	}

	return &clusterLinkingConfig, nil
}

func (c *ConfluentClusterClient) CreateMirrorTopics(linkName string, topicName string, mirrorTopicName string) error {
	urlPath := fmt.Sprintf("/kafka/v3/clusters/%s/links/%s/mirrors", c.ClusterId, linkName)

	request := &MirrorTopicReq{
		SourceTopicName: topicName,
		MirrorTopicName: mirrorTopicName,
	}

	response, err := c.doRequest(c.BaseUrl, urlPath, http.MethodPost, request, nil)
	if err != nil {
		return err
	}

	if http.StatusCreated != response.StatusCode {
		return fmt.Errorf("failed create mirror topic: %s", response.Status)
	}

	return nil
}
