package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/electric-saw/ccloud-client-go/ccloud/common"
)

type AuthType string

const (
	AuthTypeLocal AuthType = "AUTH_TYPE_LOCAL"
	AuthTypeSSO   AuthType = "AUTH_TYPE_SSO"
)

type InvitationStatus string

const (
	InvitationStatusSent        InvitationStatus = "INVITE_STATUS_SENT"
	InvitationStatusStaged      InvitationStatus = "INVITE_STATUS_STAGED"
	InvitationStatusAccepted    InvitationStatus = "INVITE_STATUS_ACCEPTED"
	InvitationStatusExpired     InvitationStatus = "INVITE_STATUS_EXPIRED"
	InvitationStatusDeactivated InvitationStatus = "INVITE_STATUS_DEACTIVATED"
)

type ObjectReference struct {
	ID           string `json:"id"`
	Related      string `json:"related"`
	ResourceName string `json:"resource_name"`
}

type Invitation struct {
	common.BaseModel
	Email      string           `json:"email"`
	AuthType   AuthType         `json:"auth_type,omitempty"`
	Status     InvitationStatus `json:"status,omitempty"`
	AcceptedAt *time.Time       `json:"accepted_at,omitempty"`
	ExpiresAt  *time.Time       `json:"expires_at,omitempty"`
	User       *ObjectReference `json:"user,omitempty"`
	Creator    *ObjectReference `json:"creator,omitempty"`
}

type InvitationList struct {
	common.BaseModel
	Data []Invitation `json:"data"`
}

type ListInvitationsQuery struct {
	common.PaginationOptions
	Email   string           `url:"email,omitempty"`
	Status  InvitationStatus `url:"status,omitempty"`
	User    string           `url:"user,omitempty"`
	Creator string           `url:"creator,omitempty"`
}

type InvitationCreateReq struct {
	Email    string   `json:"email"`
	AuthType AuthType `json:"auth_type,omitempty"`
}

func (c *ConfluentClient) ListInvitations(query *ListInvitationsQuery) (*InvitationList, error) {
	urlPath := "/iam/v2/invitations"
	response, err := c.doRequest(urlPath, http.MethodGet, nil, query)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if http.StatusOK != response.StatusCode {
		return nil, fmt.Errorf("failed to list invitations: %s", response.Status)
	}

	var invitationList InvitationList
	err = json.NewDecoder(response.Body).Decode(&invitationList)
	if err != nil {
		return nil, err
	}

	return &invitationList, nil
}

func (c *ConfluentClient) CreateInvitation(request *InvitationCreateReq) (*Invitation, error) {
	urlPath := "/iam/v2/invitations"
	response, err := c.doRequest(urlPath, http.MethodPost, request, nil)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if http.StatusCreated != response.StatusCode {
		return nil, fmt.Errorf("failed to create invitation: %s", response.Status)
	}

	var invitation Invitation
	err = json.NewDecoder(response.Body).Decode(&invitation)
	if err != nil {
		return nil, err
	}

	return &invitation, err
}

func (c *ConfluentClient) GetInvitation(id string) (*Invitation, error) {
	urlPath := fmt.Sprintf("/iam/v2/invitations/%s", id)
	response, err := c.doRequest(urlPath, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if http.StatusOK != response.StatusCode {
		return nil, fmt.Errorf("failed to get invitation: %s", response.Status)
	}

	var invitation Invitation
	err = json.NewDecoder(response.Body).Decode(&invitation)
	if err != nil {
		return nil, err
	}

	return &invitation, nil
}

func (c *ConfluentClient) DeleteInvitation(id string) error {
	urlPath := fmt.Sprintf("/iam/v2/invitations/%s", id)
	response, err := c.doRequest(urlPath, http.MethodDelete, nil, nil)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	if http.StatusOK != response.StatusCode && http.StatusNoContent != response.StatusCode {
		return fmt.Errorf("failed to delete invitation: %s", response.Status)
	}

	return nil
}
