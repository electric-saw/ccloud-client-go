package ccloud_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/electric-saw/ccloud-client-go/ccloud"
	"github.com/electric-saw/ccloud-client-go/ccloud/common"
	"github.com/stretchr/testify/assert"
)

type noopAuthInvitation struct{}

func (n noopAuthInvitation) SetAuth(req *http.Request) error { return nil }

func TestListInvitations(t *testing.T) {
	acceptedAt := time.Now().UTC()
	expiresAt := time.Now().UTC()

	expectedList := &ccloud.InvitationList{
		BaseModel: common.BaseModel{
			ApiVersion: "iam/v2",
			Kind:       "InvitationList",
		},
		Data: []ccloud.Invitation{
			{
				BaseModel: common.BaseModel{
					ApiVersion: "iam/v2",
					Kind:       "Invitation",
					Id:         "inv-001",
				},
				Email:      "user1@example.com",
				AuthType:   ccloud.AuthTypeLocal,
				Status:     ccloud.InvitationStatusAccepted,
				AcceptedAt: &acceptedAt,
				ExpiresAt:  &expiresAt,
				User: &ccloud.ObjectReference{
					ID:           "u-111aaa",
					ResourceName: "crn://confluent.cloud/user=u-111aaa",
				},
			},
			{
				BaseModel: common.BaseModel{
					ApiVersion: "iam/v2",
					Kind:       "Invitation",
					Id:         "inv-002",
				},
				Email:    "user2@example.com",
				AuthType: ccloud.AuthTypeSSO,
				Status:   ccloud.InvitationStatusSent,
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/iam/v2/invitations", r.URL.Path)

		q := r.URL.Query()
		assert.Equal(t, "user1@example.com", q.Get("email"))
		assert.Equal(t, string(ccloud.InvitationStatusSent), q.Get("status"))
		assert.Equal(t, "u-111aaa", q.Get("user"))
		assert.Equal(t, "u-222bbb", q.Get("creator"))
		assert.Equal(t, "5", q.Get("page_size"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expectedList)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.ListInvitations(&ccloud.ListInvitationsQuery{
		PaginationOptions: common.PaginationOptions{PageSize: 5},
		Email:             "user1@example.com",
		Status:            ccloud.InvitationStatusSent,
		User:              "u-111aaa",
		Creator:           "u-222bbb",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Data, 2)
	assert.Equal(t, "inv-001", result.Data[0].Id)
	assert.Equal(t, "user1@example.com", result.Data[0].Email)
	assert.Equal(t, ccloud.AuthTypeLocal, result.Data[0].AuthType)
	assert.Equal(t, ccloud.InvitationStatusAccepted, result.Data[0].Status)
	assert.Equal(t, acceptedAt, *result.Data[0].AcceptedAt)
	assert.Equal(t, "u-111aaa", result.Data[0].User.ID)
	assert.Equal(t, "user2@example.com", result.Data[1].Email)
	assert.Equal(t, ccloud.AuthTypeSSO, result.Data[1].AuthType)
}

func TestListInvitationsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid request"}`))
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.ListInvitations(&ccloud.ListInvitationsQuery{})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to list invitations")
}

func TestCreateInvitation(t *testing.T) {
	expectedReq := &ccloud.InvitationCreateReq{
		Email:    "newuser@example.com",
		AuthType: ccloud.AuthTypeLocal,
	}

	expectedRes := &ccloud.Invitation{
		BaseModel: common.BaseModel{
			ApiVersion: "iam/v2",
			Kind:       "Invitation",
			Id:         "inv-12345",
		},
		Email:    "newuser@example.com",
		AuthType: ccloud.AuthTypeLocal,
		Status:   ccloud.InvitationStatusSent,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/iam/v2/invitations", r.URL.Path)

		var body ccloud.InvitationCreateReq
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, expectedReq.Email, body.Email)
		assert.Equal(t, expectedReq.AuthType, body.AuthType)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(expectedRes)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.CreateInvitation(expectedReq)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedRes.Id, result.Id)
	assert.Equal(t, expectedRes.Email, result.Email)
	assert.Equal(t, expectedRes.AuthType, result.AuthType)
	assert.Equal(t, expectedRes.Status, result.Status)
}

func TestCreateInvitationError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid request"}`))
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.CreateInvitation(&ccloud.InvitationCreateReq{
		Email: "invalid",
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to create invitation")
}

func TestGetInvitation(t *testing.T) {
	expectedInvitation := &ccloud.Invitation{
		BaseModel: common.BaseModel{
			ApiVersion: "iam/v2",
			Kind:       "Invitation",
			Id:         "inv-12345",
		},
		Email:    "user@example.com",
		AuthType: ccloud.AuthTypeSSO,
		Status:   ccloud.InvitationStatusStaged,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/iam/v2/invitations/inv-12345", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expectedInvitation)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.GetInvitation("inv-12345")

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedInvitation.Id, result.Id)
	assert.Equal(t, expectedInvitation.Email, result.Email)
	assert.Equal(t, expectedInvitation.AuthType, result.AuthType)
	assert.Equal(t, expectedInvitation.Status, result.Status)
}

func TestGetInvitationNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error": "not found"}`))
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	result, err := client.GetInvitation("inv-nonexistent")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get invitation")
}

func TestDeleteInvitation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/iam/v2/invitations/inv-12345", r.URL.Path)

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	err := client.DeleteInvitation("inv-12345")

	assert.NoError(t, err)
}

func TestDeleteInvitationNoContent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/iam/v2/invitations/inv-12345", r.URL.Path)

		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	err := client.DeleteInvitation("inv-12345")

	assert.NoError(t, err)
}

func TestDeleteInvitationError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error": "not found"}`))
	}))
	defer ts.Close()

	client := ccloud.NewClient().WithAuth(noopAuthInvitation{}).WithBaseUrl(ts.URL)

	err := client.DeleteInvitation("inv-nonexistent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete invitation")
}
