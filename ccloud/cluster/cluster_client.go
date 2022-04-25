package cluster

import (
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
)

type ConfluentClusterClient struct {
	user        string
	password    string
	BaseUrl     string
	ClusterId   string
	clusterInfo *KafkaCluster
}

func NewClusterClient(user, password, clusterId, clusterUrl string) (*ConfluentClusterClient, error) {
	client := &ConfluentClusterClient{
		user:      user,
		password:  password,
		BaseUrl:   clusterUrl,
		ClusterId: clusterId,
	}

	clusterInfo, err := client.getCluster()
	if err != nil {
		return nil, err
	}

	client.clusterInfo = clusterInfo

	return client, nil
}

func (c *ConfluentClusterClient) doRequest(base string, urlPath, method string, body, params interface{}) (*http.Response, error) {
	client := retryablehttp.NewClient()
	client.RetryMax = 10

	if base == "" {
		base = c.BaseUrl
	}

	url, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %s", err)
	}

	url.Path = path.Join(url.Path, urlPath)

	req, err := retryablehttp.NewRequest(method, url.String(), body)
	if err != nil {
		return nil, err
	}

	req.Request.SetBasicAuth(c.user, c.password)

	qry, err := query.Values(params)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query params: %s", err)
	}

	req.URL.RawQuery = qry.Encode()

	return client.Do(req)
}
