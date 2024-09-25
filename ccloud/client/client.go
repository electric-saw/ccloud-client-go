package client

import "net/http"

type ClientAuth interface {
	SetAuth(req *http.Request) error
}
