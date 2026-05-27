package ccloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Profile struct {
	Error interface{} `json:"error"`
	User  struct {
		ID                 int       `json:"id"`
		Email              string    `json:"email"`
		FirstName          string    `json:"first_name"`
		LastName           string    `json:"last_name"`
		OrganizationID     int       `json:"organization_id"`
		Deactivated        bool      `json:"deactivated"`
		Verified           time.Time `json:"verified"`
		Created            time.Time `json:"created"`
		Modified           time.Time `json:"modified"`
		ServiceName        string    `json:"service_name"`
		ServiceDescription string    `json:"service_description"`
		ServiceAccount     bool      `json:"service_account"`
		Sso                struct {
			Enabled             bool        `json:"enabled"`
			Auth0ConnectionName string      `json:"auth0_connection_name"`
			TenantID            string      `json:"tenant_id"`
			MultiTenant         bool        `json:"multi_tenant"`
			Overrides           interface{} `json:"overrides"`
			Mode                string      `json:"mode"`
		} `json:"sso"`
		Preferences      map[string]string `json:"preferences"`
		Internal         bool              `json:"internal"`
		ResourceID       string            `json:"resource_id"`
		DeactivatedAt    interface{}       `json:"deactivated_at"`
		SocialConnection string            `json:"social_connection"`
		AuthType         string            `json:"auth_type"`
	} `json:"user"`
	Organization struct {
		ID               int       `json:"id"`
		Name             string    `json:"name"`
		Deactivated      bool      `json:"deactivated"`
		StripeCustomerID string    `json:"stripe_customer_id"`
		Created          time.Time `json:"created"`
		Modified         time.Time `json:"modified"`
		BillingEmail     string    `json:"billing_email"`
		Plan             struct {
			TaxAddress struct {
				Street1 string `json:"street1"`
				Street2 string `json:"street2"`
				City    string `json:"city"`
				State   string `json:"state"`
				Country string `json:"country"`
				Zip     string `json:"zip"`
			} `json:"tax_address"`
			ProductLevel string      `json:"product_level"`
			TrialStart   interface{} `json:"trial_start"`
			TrialEnd     interface{} `json:"trial_end"`
			PlanStart    interface{} `json:"plan_start"`
			PlanEnd      interface{} `json:"plan_end"`
			Product      interface{} `json:"product"`
			Billing      struct {
				Method           string `json:"method"`
				Interval         string `json:"interval"`
				AccruedThisCycle string `json:"accrued_this_cycle"`
				StripeCustomerID string `json:"stripe_customer_id"`
				Email            string `json:"email"`
			} `json:"billing"`
			ReferralCode      string `json:"referral_code"`
			AcceptTos         bool   `json:"accept_tos"`
			AllowMultiTenant  bool   `json:"allow_multi_tenant"`
			AcceptTosPlatform bool   `json:"accept_tos_platform"`
		} `json:"plan"`
		Saml interface{} `json:"saml"`
		Sso  struct {
			Enabled             bool        `json:"enabled"`
			Auth0ConnectionName string      `json:"auth0_connection_name"`
			TenantID            string      `json:"tenant_id"`
			MultiTenant         bool        `json:"multi_tenant"`
			Overrides           interface{} `json:"overrides"`
			Mode                string      `json:"mode"`
		} `json:"sso"`
		Marketplace struct {
			Partner           string `json:"partner"`
			CustomerID        string `json:"customer_id"`
			CustomerState     string `json:"customer_state"`
			ConsoleIntegrated bool   `json:"console_integrated"`
		} `json:"marketplace"`
		ResourceID     string `json:"resource_id"`
		HasEntitlement bool   `json:"has_entitlement"`
		ShowBilling    bool   `json:"show_billing"`
		AuditLog       struct {
			ClusterID                string `json:"cluster_id"`
			AccountID                string `json:"account_id"`
			ServiceAccountID         int    `json:"service_account_id"`
			TopicName                string `json:"topic_name"`
			ServiceAccountResourceID string `json:"service_account_resource_id"`
		} `json:"audit_log"`
		HasCommitment           bool        `json:"has_commitment"`
		MarketplaceSubscription string      `json:"marketplace_subscription"`
		DeactivatedAt           interface{} `json:"deactivated_at"`
		SuspensionStatus        struct {
			Suspended              interface{} `json:"suspended"`
			Status                 string      `json:"status"`
			EventType              string      `json:"event_type"`
			ScheduledDeactivatedAt interface{} `json:"scheduled_deactivated_at"`
		} `json:"suspension_status"`
		DisplayLabel string `json:"display_label"`
	} `json:"organization"`
	Accounts []struct {
		ID             string    `json:"id"`
		Name           string    `json:"name"`
		OrganizationID int       `json:"organization_id"`
		Deactivated    bool      `json:"deactivated"`
		Created        time.Time `json:"created"`
		Modified       time.Time `json:"modified"`
		Config         struct {
			MaxKafkaClusters int `json:"max_kafka_clusters"`
		} `json:"config"`
		Internal      bool        `json:"internal"`
		DeactivatedAt interface{} `json:"deactivated_at"`
		OrgResourceID string      `json:"org_resource_id"`
	} `json:"accounts"`
	Account struct {
		ID             string    `json:"id"`
		Name           string    `json:"name"`
		OrganizationID int       `json:"organization_id"`
		Deactivated    bool      `json:"deactivated"`
		Created        time.Time `json:"created"`
		Modified       time.Time `json:"modified"`
		Config         struct {
			MaxKafkaClusters int `json:"max_kafka_clusters"`
		} `json:"config"`
		Internal      bool        `json:"internal"`
		DeactivatedAt interface{} `json:"deactivated_at"`
		OrgResourceID string      `json:"org_resource_id"`
	} `json:"account"`
}

func (c *ConfluentClient) GetMe() (*Profile, error) {

	req, err := c.doRequest("me", http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != req.StatusCode {
		return nil, fmt.Errorf("failed to list users: %s", req.Status)
	}

	defer req.Body.Close()

	var profile Profile
	err = json.NewDecoder(req.Body).Decode(&profile)
	if err != nil {
		return nil, err
	}

	return &profile, nil
}
