package ccloud

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
)

type ConfluentClient struct {
	user     string
	password string
	BaseUrl  string
}

func NewClient(user, password string) *ConfluentClient {
	return &ConfluentClient{
		user:     user,
		password: password,
		BaseUrl:  "https://api.confluent.cloud",
	}
}

func (c *ConfluentClient) doRequest(urlPath, method string, body, params interface{}) (*http.Response, error) {
	client := retryablehttp.NewClient()
	client.RetryMax = 10

	url, err := url.Parse(c.BaseUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %s", err)
	}

	url.Path = urlPath

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
