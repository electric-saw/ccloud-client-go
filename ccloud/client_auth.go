package ccloud

import "net/http"

type BasicAuth struct {
	Username string
	Password string
}

func NewBasicAuth(username, password string) BasicAuth {
	return BasicAuth{
		Username: username,
		Password: password,
	}
}

func (a BasicAuth) SetAuth(req *http.Request) error {
	req.SetBasicAuth(a.Username, a.Password)
	return nil
}
