package ccloud

import (
	"bytes"
	"context"
	"io"

	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/electric-saw/ccloud-client-go/ccloud/client"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
)

const ContentTypeJSON = "application/json"

type ConfluentClient struct {
	auth    client.ClientAuth
	BaseUrl string
}

func NewClient() *ConfluentClient {
	return &ConfluentClient{
		BaseUrl: "https://api.confluent.cloud",
	}
}

func (c *ConfluentClient) WithAuth(auth client.ClientAuth) *ConfluentClient {
	c.auth = auth
	return c
}

func (c *ConfluentClient) WithBaseUrl(baseUrl string) *ConfluentClient {
	c.BaseUrl = baseUrl
	return c
}

type specWrap struct {
	Spec interface{} `json:"spec"`
}

func (c *ConfluentClient) doRequest(urlPath, method string, body, params any) (*http.Response, error) {
	client := retryablehttp.NewClient()
	client.RetryMax = 10

	client.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		ok, e := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if !ok && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode >= 500 && resp.StatusCode != 501) {
			return true, e
		}
		return ok, nil
	}

	url, err := url.Parse(c.BaseUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %s", err)
	}

	url.Path = urlPath

	var req *retryablehttp.Request

	var bodyReader io.Reader

	if body != nil {
		bodyBuffer := new(bytes.Buffer)
		err := json.NewEncoder(bodyBuffer).Encode(body)
		if err != nil {
			return nil, fmt.Errorf("failed to encode body: %s", err)
		}
		bodyReader = bodyBuffer
	}

	req, err = retryablehttp.NewRequest(method, url.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	if err := c.auth.SetAuth(req.Request); err != nil {
		return nil, fmt.Errorf("failed to set auth: %s", err)
	}

	req.Header.Add("Content-Type", ContentTypeJSON)

	qry, err := query.Values(params)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query params: %s", err)
	}

	req.URL.RawQuery = qry.Encode()

	return client.Do(req)
}
