package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type User struct {
	common.BaseModel
	Email    string `json:"email"`
	FullName string `json:"full_name"`
}

type UserList struct {
	common.BaseModel
	Data []User `json:"data"`
}

func (c *ConfluentClient) ListUsers(opt *common.PaginationOptions) (*UserList, error) {
	urlPath := "/iam/v2/users"
	req, err := c.doRequest(urlPath, http.MethodGet, nil, opt)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list users: %s", req.Status)
	}

	defer req.Body.Close()

	var userList UserList
	err = json.NewDecoder(req.Body).Decode(&userList)
	if err != nil {
		return nil, err
	}

	return &userList, nil
}

func (c *ConfluentClient) GetUser(userId string) (*User, error) {
	urlPath := fmt.Sprintf("/iam/v2/users/%s", userId)
	req, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get user: %s", req.Status)
	}

	defer req.Body.Close()

	var user User
	err = json.NewDecoder(req.Body).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

type UserUpdateReq struct {
	FullName string `json:"full_name"`
}

func (c *ConfluentClient) UpdateUser(userId string, update *UserUpdateReq) (*User, error) {
	urlPath := fmt.Sprintf("/iam/v2/users/%s", userId)
	req, err := c.doRequest(urlPath, http.MethodPatch, update, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to get user: %s", req.Status)
	}

	defer req.Body.Close()

	var user User
	err = json.NewDecoder(req.Body).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (c *ConfluentClient) DeleteUser(userId string) error {
	urlPath := fmt.Sprintf("/iam/v2/users/%s", userId)
	req, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	if http.StatusOK != req.StatusCode && http.StatusNoContent != req.StatusCode {
		return fmt.Errorf("failed to delete user: %s", req.Status)
	}

	return nil
}
