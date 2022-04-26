package common

import "net/url"

type PaginationOptions struct {
	PageSize  int    `url:"page_size,omitempty"`
	PageToken string `url:"page_token,omitempty"`
}

type CloudProvider string

const (
	CloudProviderAWS   CloudProvider = "AWS"
	CloudProviderGCP   CloudProvider = "GCP"
	CloudProviderAzure CloudProvider = "AZURE"
)

type BaseModel struct {
	ApiVersion   string `json:"api_version,omitempty"`
	Kind         string `json:"kind,omitempty"`
	Id           string `json:"id,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
	Related      string `json:"related,omitempty"`
	Metadata     struct {
		Self         *string `json:"self,omitempty"`
		ResourceName *string `json:"resource_name,omitempty"`
		CreatedAt    *string `json:"created_at,omitempty"`
		UpdatedAt    *string `json:"updated_at,omitempty"`
		DeleteAt     *string `json:"delete_at,omitempty"`
		First        *string `json:"first,omitempty"`
		Last         *string `json:"last,omitempty"`
		Next         *string `json:"next,omitempty"`
		Prev         *string `json:"prev,omitempty"`
		TotalSize    *int    `json:"total_size,omitempty"`
	} `json:"metadata,omitempty"`
}

func (b *BaseModel) GetPageNextToken() string {
	if b.Metadata.Next != nil {
		u, _ := url.Parse(*b.Metadata.Next)
		return u.Query().Get("page_token")
	} else {
		return ""
	}
}
