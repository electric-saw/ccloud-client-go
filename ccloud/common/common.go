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
	ApiVersion   string `json:"api_version"`
	Kind         string `json:"kind"`
	Id           string `json:"id"`
	ResourceName string `json:"resource_name"`
	Related      string `json:"related"`
	Metadata     struct {
		Self         *string `json:"self"`
		ResourceName *string `json:"resource_name"`
		CreatedAt    *string `json:"created_at"`
		UpdatedAt    *string `json:"updated_at"`
		DeleteAt     *string `json:"delete_at"`
		First        *string `json:"first"`
		Last         *string `json:"last"`
		Next         *string `json:"next"`
		Prev         *string `json:"prev"`
		TotalSize    *int    `json:"total_size"`
	} `json:"metadata"`
}

func (b *BaseModel) GetPageNextToken() string {
	if b.Metadata.Next != nil {
		u, _ := url.Parse(*b.Metadata.Next)
		return u.Query().Get("page_token")
	} else {
		return ""
	}
}
