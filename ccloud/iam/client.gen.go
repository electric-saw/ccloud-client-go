package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oapi-codegen/runtime"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

const (
	Artifactv1 ArtifactV1FlinkArtifactApiVersion = "artifact/v1"
)

func (e ArtifactV1FlinkArtifactApiVersion) Valid() bool {
	switch e {
	case Artifactv1:
		return true
	default:
		return false
	}
}

const (
	FlinkArtifact ArtifactV1FlinkArtifactKind = "FlinkArtifact"
)

func (e ArtifactV1FlinkArtifactKind) Valid() bool {
	switch e {
	case FlinkArtifact:
		return true
	default:
		return false
	}
}

const (
	ArtifactV1UploadSource ArtifactV1UploadSourcePresignedUrlApiVersion = "artifact.v1/UploadSource"
)

func (e ArtifactV1UploadSourcePresignedUrlApiVersion) Valid() bool {
	switch e {
	case ArtifactV1UploadSource:
		return true
	default:
		return false
	}
}

const (
	PresignedUrl ArtifactV1UploadSourcePresignedUrlKind = "PresignedUrl"
)

func (e ArtifactV1UploadSourcePresignedUrlKind) Valid() bool {
	switch e {
	case PresignedUrl:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyApiVersionIamv2 IamV2ApiKeyApiVersion = "iam/v2"
)

func (e IamV2ApiKeyApiVersion) Valid() bool {
	switch e {
	case IamV2ApiKeyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyKindApiKey IamV2ApiKeyKind = "ApiKey"
)

func (e IamV2ApiKeyKind) Valid() bool {
	switch e {
	case IamV2ApiKeyKindApiKey:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyListApiVersionIamv2 IamV2ApiKeyListApiVersion = "iam/v2"
)

func (e IamV2ApiKeyListApiVersion) Valid() bool {
	switch e {
	case IamV2ApiKeyListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyListDataApiVersionIamv2 IamV2ApiKeyListDataApiVersion = "iam/v2"
)

func (e IamV2ApiKeyListDataApiVersion) Valid() bool {
	switch e {
	case IamV2ApiKeyListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyListDataKindApiKey IamV2ApiKeyListDataKind = "ApiKey"
)

func (e IamV2ApiKeyListDataKind) Valid() bool {
	switch e {
	case IamV2ApiKeyListDataKindApiKey:
		return true
	default:
		return false
	}
}

const (
	IamV2ApiKeyListKindApiKeyList IamV2ApiKeyListKind = "ApiKeyList"
)

func (e IamV2ApiKeyListKind) Valid() bool {
	switch e {
	case IamV2ApiKeyListKindApiKeyList:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateAuthorityApiVersionIamv2 IamV2CertificateAuthorityApiVersion = "iam/v2"
)

func (e IamV2CertificateAuthorityApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateAuthorityApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateAuthorityKindCertificateAuthority IamV2CertificateAuthorityKind = "CertificateAuthority"
)

func (e IamV2CertificateAuthorityKind) Valid() bool {
	switch e {
	case IamV2CertificateAuthorityKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateAuthorityListApiVersionIamv2 IamV2CertificateAuthorityListApiVersion = "iam/v2"
)

func (e IamV2CertificateAuthorityListApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateAuthorityListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateAuthorityListDataApiVersionIamv2 IamV2CertificateAuthorityListDataApiVersion = "iam/v2"
)

func (e IamV2CertificateAuthorityListDataApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateAuthorityListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateAuthorityListDataKindCertificateAuthority IamV2CertificateAuthorityListDataKind = "CertificateAuthority"
)

func (e IamV2CertificateAuthorityListDataKind) Valid() bool {
	switch e {
	case IamV2CertificateAuthorityListDataKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	CertificateAuthorityList IamV2CertificateAuthorityListKind = "CertificateAuthorityList"
)

func (e IamV2CertificateAuthorityListKind) Valid() bool {
	switch e {
	case CertificateAuthorityList:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateIdentityPoolApiVersionIamv2 IamV2CertificateIdentityPoolApiVersion = "iam/v2"
)

func (e IamV2CertificateIdentityPoolApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateIdentityPoolApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateIdentityPoolKindCertificateIdentityPool IamV2CertificateIdentityPoolKind = "CertificateIdentityPool"
)

func (e IamV2CertificateIdentityPoolKind) Valid() bool {
	switch e {
	case IamV2CertificateIdentityPoolKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateIdentityPoolListApiVersionIamv2 IamV2CertificateIdentityPoolListApiVersion = "iam/v2"
)

func (e IamV2CertificateIdentityPoolListApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateIdentityPoolListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateIdentityPoolListDataApiVersionIamv2 IamV2CertificateIdentityPoolListDataApiVersion = "iam/v2"
)

func (e IamV2CertificateIdentityPoolListDataApiVersion) Valid() bool {
	switch e {
	case IamV2CertificateIdentityPoolListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2CertificateIdentityPoolListDataKindCertificateIdentityPool IamV2CertificateIdentityPoolListDataKind = "CertificateIdentityPool"
)

func (e IamV2CertificateIdentityPoolListDataKind) Valid() bool {
	switch e {
	case IamV2CertificateIdentityPoolListDataKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	CertificateIdentityPoolList IamV2CertificateIdentityPoolListKind = "CertificateIdentityPoolList"
)

func (e IamV2CertificateIdentityPoolListKind) Valid() bool {
	switch e {
	case CertificateIdentityPoolList:
		return true
	default:
		return false
	}
}

const (
	IamV2CreateCertRequestApiVersionIamv2 IamV2CreateCertRequestApiVersion = "iam/v2"
)

func (e IamV2CreateCertRequestApiVersion) Valid() bool {
	switch e {
	case IamV2CreateCertRequestApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateCertRequest IamV2CreateCertRequestKind = "CreateCertRequest"
)

func (e IamV2CreateCertRequestKind) Valid() bool {
	switch e {
	case CreateCertRequest:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityPoolApiVersionIamv2 IamV2IdentityPoolApiVersion = "iam/v2"
)

func (e IamV2IdentityPoolApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityPoolApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityPoolKindIdentityPool IamV2IdentityPoolKind = "IdentityPool"
)

func (e IamV2IdentityPoolKind) Valid() bool {
	switch e {
	case IamV2IdentityPoolKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityPoolListApiVersionIamv2 IamV2IdentityPoolListApiVersion = "iam/v2"
)

func (e IamV2IdentityPoolListApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityPoolListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityPoolListDataApiVersionIamv2 IamV2IdentityPoolListDataApiVersion = "iam/v2"
)

func (e IamV2IdentityPoolListDataApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityPoolListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityPoolListDataKindIdentityPool IamV2IdentityPoolListDataKind = "IdentityPool"
)

func (e IamV2IdentityPoolListDataKind) Valid() bool {
	switch e {
	case IamV2IdentityPoolListDataKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	IdentityPoolList IamV2IdentityPoolListKind = "IdentityPoolList"
)

func (e IamV2IdentityPoolListKind) Valid() bool {
	switch e {
	case IdentityPoolList:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityProviderApiVersionIamv2 IamV2IdentityProviderApiVersion = "iam/v2"
)

func (e IamV2IdentityProviderApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityProviderApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityProviderKindIdentityProvider IamV2IdentityProviderKind = "IdentityProvider"
)

func (e IamV2IdentityProviderKind) Valid() bool {
	switch e {
	case IamV2IdentityProviderKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityProviderListApiVersionIamv2 IamV2IdentityProviderListApiVersion = "iam/v2"
)

func (e IamV2IdentityProviderListApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityProviderListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityProviderListDataApiVersionIamv2 IamV2IdentityProviderListDataApiVersion = "iam/v2"
)

func (e IamV2IdentityProviderListDataApiVersion) Valid() bool {
	switch e {
	case IamV2IdentityProviderListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IdentityProviderListDataKindIdentityProvider IamV2IdentityProviderListDataKind = "IdentityProvider"
)

func (e IamV2IdentityProviderListDataKind) Valid() bool {
	switch e {
	case IamV2IdentityProviderListDataKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	IdentityProviderList IamV2IdentityProviderListKind = "IdentityProviderList"
)

func (e IamV2IdentityProviderListKind) Valid() bool {
	switch e {
	case IdentityProviderList:
		return true
	default:
		return false
	}
}

const (
	IamV2InvitationApiVersionIamv2 IamV2InvitationApiVersion = "iam/v2"
)

func (e IamV2InvitationApiVersion) Valid() bool {
	switch e {
	case IamV2InvitationApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2InvitationKindInvitation IamV2InvitationKind = "Invitation"
)

func (e IamV2InvitationKind) Valid() bool {
	switch e {
	case IamV2InvitationKindInvitation:
		return true
	default:
		return false
	}
}

const (
	IamV2InvitationListApiVersionIamv2 IamV2InvitationListApiVersion = "iam/v2"
)

func (e IamV2InvitationListApiVersion) Valid() bool {
	switch e {
	case IamV2InvitationListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2InvitationListDataApiVersionIamv2 IamV2InvitationListDataApiVersion = "iam/v2"
)

func (e IamV2InvitationListDataApiVersion) Valid() bool {
	switch e {
	case IamV2InvitationListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2InvitationListDataKindInvitation IamV2InvitationListDataKind = "Invitation"
)

func (e IamV2InvitationListDataKind) Valid() bool {
	switch e {
	case IamV2InvitationListDataKindInvitation:
		return true
	default:
		return false
	}
}

const (
	InvitationList IamV2InvitationListKind = "InvitationList"
)

func (e IamV2InvitationListKind) Valid() bool {
	switch e {
	case InvitationList:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterApiVersionIamv2 IamV2IpFilterApiVersion = "iam/v2"
)

func (e IamV2IpFilterApiVersion) Valid() bool {
	switch e {
	case IamV2IpFilterApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterKindIpFilter IamV2IpFilterKind = "IpFilter"
)

func (e IamV2IpFilterKind) Valid() bool {
	switch e {
	case IamV2IpFilterKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterListApiVersionIamv2 IamV2IpFilterListApiVersion = "iam/v2"
)

func (e IamV2IpFilterListApiVersion) Valid() bool {
	switch e {
	case IamV2IpFilterListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterListDataApiVersionIamv2 IamV2IpFilterListDataApiVersion = "iam/v2"
)

func (e IamV2IpFilterListDataApiVersion) Valid() bool {
	switch e {
	case IamV2IpFilterListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterListDataKindIpFilter IamV2IpFilterListDataKind = "IpFilter"
)

func (e IamV2IpFilterListDataKind) Valid() bool {
	switch e {
	case IamV2IpFilterListDataKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	IpFilterList IamV2IpFilterListKind = "IpFilterList"
)

func (e IamV2IpFilterListKind) Valid() bool {
	switch e {
	case IpFilterList:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterSummaryApiVersionIamv2 IamV2IpFilterSummaryApiVersion = "iam/v2"
)

func (e IamV2IpFilterSummaryApiVersion) Valid() bool {
	switch e {
	case IamV2IpFilterSummaryApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpFilterSummaryKindIpFilterSummary IamV2IpFilterSummaryKind = "IpFilterSummary"
)

func (e IamV2IpFilterSummaryKind) Valid() bool {
	switch e {
	case IamV2IpFilterSummaryKindIpFilterSummary:
		return true
	default:
		return false
	}
}

const (
	IamV2IpGroupApiVersionIamv2 IamV2IpGroupApiVersion = "iam/v2"
)

func (e IamV2IpGroupApiVersion) Valid() bool {
	switch e {
	case IamV2IpGroupApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpGroupKindIpGroup IamV2IpGroupKind = "IpGroup"
)

func (e IamV2IpGroupKind) Valid() bool {
	switch e {
	case IamV2IpGroupKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	IamV2IpGroupListApiVersionIamv2 IamV2IpGroupListApiVersion = "iam/v2"
)

func (e IamV2IpGroupListApiVersion) Valid() bool {
	switch e {
	case IamV2IpGroupListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpGroupListDataApiVersionIamv2 IamV2IpGroupListDataApiVersion = "iam/v2"
)

func (e IamV2IpGroupListDataApiVersion) Valid() bool {
	switch e {
	case IamV2IpGroupListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2IpGroupListDataKindIpGroup IamV2IpGroupListDataKind = "IpGroup"
)

func (e IamV2IpGroupListDataKind) Valid() bool {
	switch e {
	case IamV2IpGroupListDataKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	IpGroupList IamV2IpGroupListKind = "IpGroupList"
)

func (e IamV2IpGroupListKind) Valid() bool {
	switch e {
	case IpGroupList:
		return true
	default:
		return false
	}
}

const (
	IamV2JwksApiVersionIamv2 IamV2JwksApiVersion = "iam/v2"
)

func (e IamV2JwksApiVersion) Valid() bool {
	switch e {
	case IamV2JwksApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2JwksKindJwks IamV2JwksKind = "Jwks"
)

func (e IamV2JwksKind) Valid() bool {
	switch e {
	case IamV2JwksKindJwks:
		return true
	default:
		return false
	}
}

const (
	IamV2RoleBindingApiVersionIamv2 IamV2RoleBindingApiVersion = "iam/v2"
)

func (e IamV2RoleBindingApiVersion) Valid() bool {
	switch e {
	case IamV2RoleBindingApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2RoleBindingKindRoleBinding IamV2RoleBindingKind = "RoleBinding"
)

func (e IamV2RoleBindingKind) Valid() bool {
	switch e {
	case IamV2RoleBindingKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	IamV2RoleBindingListApiVersionIamv2 IamV2RoleBindingListApiVersion = "iam/v2"
)

func (e IamV2RoleBindingListApiVersion) Valid() bool {
	switch e {
	case IamV2RoleBindingListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2RoleBindingListDataApiVersionIamv2 IamV2RoleBindingListDataApiVersion = "iam/v2"
)

func (e IamV2RoleBindingListDataApiVersion) Valid() bool {
	switch e {
	case IamV2RoleBindingListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2RoleBindingListDataKindRoleBinding IamV2RoleBindingListDataKind = "RoleBinding"
)

func (e IamV2RoleBindingListDataKind) Valid() bool {
	switch e {
	case IamV2RoleBindingListDataKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	RoleBindingList IamV2RoleBindingListKind = "RoleBindingList"
)

func (e IamV2RoleBindingListKind) Valid() bool {
	switch e {
	case RoleBindingList:
		return true
	default:
		return false
	}
}

const (
	IamV2ServiceAccountApiVersionIamv2 IamV2ServiceAccountApiVersion = "iam/v2"
)

func (e IamV2ServiceAccountApiVersion) Valid() bool {
	switch e {
	case IamV2ServiceAccountApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ServiceAccountKindServiceAccount IamV2ServiceAccountKind = "ServiceAccount"
)

func (e IamV2ServiceAccountKind) Valid() bool {
	switch e {
	case IamV2ServiceAccountKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	IamV2ServiceAccountListApiVersionIamv2 IamV2ServiceAccountListApiVersion = "iam/v2"
)

func (e IamV2ServiceAccountListApiVersion) Valid() bool {
	switch e {
	case IamV2ServiceAccountListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ServiceAccountListDataApiVersionIamv2 IamV2ServiceAccountListDataApiVersion = "iam/v2"
)

func (e IamV2ServiceAccountListDataApiVersion) Valid() bool {
	switch e {
	case IamV2ServiceAccountListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2ServiceAccountListDataKindServiceAccount IamV2ServiceAccountListDataKind = "ServiceAccount"
)

func (e IamV2ServiceAccountListDataKind) Valid() bool {
	switch e {
	case IamV2ServiceAccountListDataKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	ServiceAccountList IamV2ServiceAccountListKind = "ServiceAccountList"
)

func (e IamV2ServiceAccountListKind) Valid() bool {
	switch e {
	case ServiceAccountList:
		return true
	default:
		return false
	}
}

const (
	IamV2UpdateCertRequestApiVersionIamv2 IamV2UpdateCertRequestApiVersion = "iam/v2"
)

func (e IamV2UpdateCertRequestApiVersion) Valid() bool {
	switch e {
	case IamV2UpdateCertRequestApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateCertRequest IamV2UpdateCertRequestKind = "UpdateCertRequest"
)

func (e IamV2UpdateCertRequestKind) Valid() bool {
	switch e {
	case UpdateCertRequest:
		return true
	default:
		return false
	}
}

const (
	IamV2UserApiVersionIamv2 IamV2UserApiVersion = "iam/v2"
)

func (e IamV2UserApiVersion) Valid() bool {
	switch e {
	case IamV2UserApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2UserKindUser IamV2UserKind = "User"
)

func (e IamV2UserKind) Valid() bool {
	switch e {
	case IamV2UserKindUser:
		return true
	default:
		return false
	}
}

const (
	IamV2UserConfigureUserAuthRequestApiVersionIamV2User IamV2UserConfigureUserAuthRequestApiVersion = "iam.v2/User"
)

func (e IamV2UserConfigureUserAuthRequestApiVersion) Valid() bool {
	switch e {
	case IamV2UserConfigureUserAuthRequestApiVersionIamV2User:
		return true
	default:
		return false
	}
}

const (
	ConfigureUserAuthRequest IamV2UserConfigureUserAuthRequestKind = "ConfigureUserAuthRequest"
)

func (e IamV2UserConfigureUserAuthRequestKind) Valid() bool {
	switch e {
	case ConfigureUserAuthRequest:
		return true
	default:
		return false
	}
}

const (
	IamV2UserListApiVersionIamv2 IamV2UserListApiVersion = "iam/v2"
)

func (e IamV2UserListApiVersion) Valid() bool {
	switch e {
	case IamV2UserListApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2UserListDataApiVersionIamv2 IamV2UserListDataApiVersion = "iam/v2"
)

func (e IamV2UserListDataApiVersion) Valid() bool {
	switch e {
	case IamV2UserListDataApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	IamV2UserListDataKindUser IamV2UserListDataKind = "User"
)

func (e IamV2UserListDataKind) Valid() bool {
	switch e {
	case IamV2UserListDataKindUser:
		return true
	default:
		return false
	}
}

const (
	UserList IamV2UserListKind = "UserList"
)

func (e IamV2UserListKind) Valid() bool {
	switch e {
	case UserList:
		return true
	default:
		return false
	}
}

const (
	IamV2SsoGroupMappingApiVersionIamV2sso IamV2SsoGroupMappingApiVersion = "iam.v2/sso"
)

func (e IamV2SsoGroupMappingApiVersion) Valid() bool {
	switch e {
	case IamV2SsoGroupMappingApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	IamV2SsoGroupMappingKindGroupMapping IamV2SsoGroupMappingKind = "GroupMapping"
)

func (e IamV2SsoGroupMappingKind) Valid() bool {
	switch e {
	case IamV2SsoGroupMappingKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	IamV2SsoGroupMappingListApiVersionIamV2sso IamV2SsoGroupMappingListApiVersion = "iam.v2/sso"
)

func (e IamV2SsoGroupMappingListApiVersion) Valid() bool {
	switch e {
	case IamV2SsoGroupMappingListApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	IamV2SsoGroupMappingListDataApiVersionIamV2sso IamV2SsoGroupMappingListDataApiVersion = "iam.v2/sso"
)

func (e IamV2SsoGroupMappingListDataApiVersion) Valid() bool {
	switch e {
	case IamV2SsoGroupMappingListDataApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	IamV2SsoGroupMappingListDataKindGroupMapping IamV2SsoGroupMappingListDataKind = "GroupMapping"
)

func (e IamV2SsoGroupMappingListDataKind) Valid() bool {
	switch e {
	case IamV2SsoGroupMappingListDataKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	GroupMappingList IamV2SsoGroupMappingListKind = "GroupMappingList"
)

func (e IamV2SsoGroupMappingListKind) Valid() bool {
	switch e {
	case GroupMappingList:
		return true
	default:
		return false
	}
}

const (
	ListIamV2ApiKeys200JSONResponseBodyApiVersionIamv2 ListIamV2ApiKeys200JSONResponseBodyApiVersion = "iam/v2"
)

func (e ListIamV2ApiKeys200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListIamV2ApiKeys200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	ListIamV2ApiKeys200JSONResponseBodyKindApiKeyList ListIamV2ApiKeys200JSONResponseBodyKind = "ApiKeyList"
)

func (e ListIamV2ApiKeys200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListIamV2ApiKeys200JSONResponseBodyKindApiKeyList:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ApiKeyJSONBodyApiVersionIamv2 CreateIamV2ApiKeyJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2ApiKeyJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2ApiKeyJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ApiKeyJSONBodyKindApiKey CreateIamV2ApiKeyJSONBodyKind = "ApiKey"
)

func (e CreateIamV2ApiKeyJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2ApiKeyJSONBodyKindApiKey:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ApiKey202JSONResponseBodyApiVersionIamv2 CreateIamV2ApiKey202JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2ApiKey202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2ApiKey202JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ApiKey202JSONResponseBodyKindApiKey CreateIamV2ApiKey202JSONResponseBodyKind = "ApiKey"
)

func (e CreateIamV2ApiKey202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2ApiKey202JSONResponseBodyKindApiKey:
		return true
	default:
		return false
	}
}

const (
	GetIamV2ApiKey200JSONResponseBodyApiVersionIamv2 GetIamV2ApiKey200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2ApiKey200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2ApiKey200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2ApiKey200JSONResponseBodyKindApiKey GetIamV2ApiKey200JSONResponseBodyKind = "ApiKey"
)

func (e GetIamV2ApiKey200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2ApiKey200JSONResponseBodyKindApiKey:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2ApiKey200JSONResponseBodyApiVersionIamv2 UpdateIamV2ApiKey200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2ApiKey200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2ApiKey200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2ApiKey200JSONResponseBodyKindApiKey UpdateIamV2ApiKey200JSONResponseBodyKind = "ApiKey"
)

func (e UpdateIamV2ApiKey200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2ApiKey200JSONResponseBodyKindApiKey:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateAuthority201JSONResponseBodyApiVersionIamv2 CreateIamV2CertificateAuthority201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2CertificateAuthority201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2CertificateAuthority201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateAuthority201JSONResponseBodyKindCertificateAuthority CreateIamV2CertificateAuthority201JSONResponseBodyKind = "CertificateAuthority"
)

func (e CreateIamV2CertificateAuthority201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2CertificateAuthority201JSONResponseBodyKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateIdentityPoolJSONBodyApiVersionIamv2 CreateIamV2CertificateIdentityPoolJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2CertificateIdentityPoolJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2CertificateIdentityPoolJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateIdentityPoolJSONBodyKindCertificateIdentityPool CreateIamV2CertificateIdentityPoolJSONBodyKind = "CertificateIdentityPool"
)

func (e CreateIamV2CertificateIdentityPoolJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2CertificateIdentityPoolJSONBodyKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateIdentityPool201JSONResponseBodyApiVersionIamv2 CreateIamV2CertificateIdentityPool201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2CertificateIdentityPool201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2CertificateIdentityPool201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2CertificateIdentityPool201JSONResponseBodyKindCertificateIdentityPool CreateIamV2CertificateIdentityPool201JSONResponseBodyKind = "CertificateIdentityPool"
)

func (e CreateIamV2CertificateIdentityPool201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2CertificateIdentityPool201JSONResponseBodyKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2 DeleteIamV2CertificateIdentityPool200JSONResponseBodyApiVersion = "iam/v2"
)

func (e DeleteIamV2CertificateIdentityPool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case DeleteIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool DeleteIamV2CertificateIdentityPool200JSONResponseBodyKind = "CertificateIdentityPool"
)

func (e DeleteIamV2CertificateIdentityPool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case DeleteIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	GetIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2 GetIamV2CertificateIdentityPool200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2CertificateIdentityPool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool GetIamV2CertificateIdentityPool200JSONResponseBodyKind = "CertificateIdentityPool"
)

func (e GetIamV2CertificateIdentityPool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2 UpdateIamV2CertificateIdentityPool200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2CertificateIdentityPool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2CertificateIdentityPool200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool UpdateIamV2CertificateIdentityPool200JSONResponseBodyKind = "CertificateIdentityPool"
)

func (e UpdateIamV2CertificateIdentityPool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2CertificateIdentityPool200JSONResponseBodyKindCertificateIdentityPool:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2 DeleteIamV2CertificateAuthority200JSONResponseBodyApiVersion = "iam/v2"
)

func (e DeleteIamV2CertificateAuthority200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case DeleteIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority DeleteIamV2CertificateAuthority200JSONResponseBodyKind = "CertificateAuthority"
)

func (e DeleteIamV2CertificateAuthority200JSONResponseBodyKind) Valid() bool {
	switch e {
	case DeleteIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	GetIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2 GetIamV2CertificateAuthority200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2CertificateAuthority200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority GetIamV2CertificateAuthority200JSONResponseBodyKind = "CertificateAuthority"
)

func (e GetIamV2CertificateAuthority200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2 UpdateIamV2CertificateAuthority200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2CertificateAuthority200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2CertificateAuthority200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority UpdateIamV2CertificateAuthority200JSONResponseBodyKind = "CertificateAuthority"
)

func (e UpdateIamV2CertificateAuthority200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2CertificateAuthority200JSONResponseBodyKindCertificateAuthority:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityProviderJSONBodyApiVersionIamv2 CreateIamV2IdentityProviderJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IdentityProviderJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IdentityProviderJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityProviderJSONBodyKindIdentityProvider CreateIamV2IdentityProviderJSONBodyKind = "IdentityProvider"
)

func (e CreateIamV2IdentityProviderJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IdentityProviderJSONBodyKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityProvider201JSONResponseBodyApiVersionIamv2 CreateIamV2IdentityProvider201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IdentityProvider201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IdentityProvider201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityProvider201JSONResponseBodyKindIdentityProvider CreateIamV2IdentityProvider201JSONResponseBodyKind = "IdentityProvider"
)

func (e CreateIamV2IdentityProvider201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IdentityProvider201JSONResponseBodyKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IdentityProvider200JSONResponseBodyApiVersionIamv2 GetIamV2IdentityProvider200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2IdentityProvider200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2IdentityProvider200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IdentityProvider200JSONResponseBodyKindIdentityProvider GetIamV2IdentityProvider200JSONResponseBodyKind = "IdentityProvider"
)

func (e GetIamV2IdentityProvider200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2IdentityProvider200JSONResponseBodyKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IdentityProvider200JSONResponseBodyApiVersionIamv2 UpdateIamV2IdentityProvider200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2IdentityProvider200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2IdentityProvider200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IdentityProvider200JSONResponseBodyKindIdentityProvider UpdateIamV2IdentityProvider200JSONResponseBodyKind = "IdentityProvider"
)

func (e UpdateIamV2IdentityProvider200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2IdentityProvider200JSONResponseBodyKindIdentityProvider:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityPoolJSONBodyApiVersionIamv2 CreateIamV2IdentityPoolJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IdentityPoolJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IdentityPoolJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityPoolJSONBodyKindIdentityPool CreateIamV2IdentityPoolJSONBodyKind = "IdentityPool"
)

func (e CreateIamV2IdentityPoolJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IdentityPoolJSONBodyKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityPool201JSONResponseBodyApiVersionIamv2 CreateIamV2IdentityPool201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IdentityPool201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IdentityPool201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IdentityPool201JSONResponseBodyKindIdentityPool CreateIamV2IdentityPool201JSONResponseBodyKind = "IdentityPool"
)

func (e CreateIamV2IdentityPool201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IdentityPool201JSONResponseBodyKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IdentityPool200JSONResponseBodyApiVersionIamv2 GetIamV2IdentityPool200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2IdentityPool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2IdentityPool200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IdentityPool200JSONResponseBodyKindIdentityPool GetIamV2IdentityPool200JSONResponseBodyKind = "IdentityPool"
)

func (e GetIamV2IdentityPool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2IdentityPool200JSONResponseBodyKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IdentityPool200JSONResponseBodyApiVersionIamv2 UpdateIamV2IdentityPool200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2IdentityPool200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2IdentityPool200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IdentityPool200JSONResponseBodyKindIdentityPool UpdateIamV2IdentityPool200JSONResponseBodyKind = "IdentityPool"
)

func (e UpdateIamV2IdentityPool200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2IdentityPool200JSONResponseBodyKindIdentityPool:
		return true
	default:
		return false
	}
}

const (
	RefreshIamV2JsonWebKeySet200JSONResponseBodyApiVersionIamv2 RefreshIamV2JsonWebKeySet200JSONResponseBodyApiVersion = "iam/v2"
)

func (e RefreshIamV2JsonWebKeySet200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case RefreshIamV2JsonWebKeySet200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	RefreshIamV2JsonWebKeySet200JSONResponseBodyKindJwks RefreshIamV2JsonWebKeySet200JSONResponseBodyKind = "Jwks"
)

func (e RefreshIamV2JsonWebKeySet200JSONResponseBodyKind) Valid() bool {
	switch e {
	case RefreshIamV2JsonWebKeySet200JSONResponseBodyKindJwks:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2InvitationJSONBodyApiVersionIamv2 CreateIamV2InvitationJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2InvitationJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2InvitationJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2InvitationJSONBodyKindInvitation CreateIamV2InvitationJSONBodyKind = "Invitation"
)

func (e CreateIamV2InvitationJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2InvitationJSONBodyKindInvitation:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2Invitation201JSONResponseBodyApiVersionIamv2 CreateIamV2Invitation201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2Invitation201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2Invitation201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2Invitation201JSONResponseBodyKindInvitation CreateIamV2Invitation201JSONResponseBodyKind = "Invitation"
)

func (e CreateIamV2Invitation201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2Invitation201JSONResponseBodyKindInvitation:
		return true
	default:
		return false
	}
}

const (
	GetIamV2Invitation200JSONResponseBodyApiVersionIamv2 GetIamV2Invitation200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2Invitation200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2Invitation200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2Invitation200JSONResponseBodyKindInvitation GetIamV2Invitation200JSONResponseBodyKind = "Invitation"
)

func (e GetIamV2Invitation200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2Invitation200JSONResponseBodyKindInvitation:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpFilterSummary200JSONResponseBodyApiVersionIamv2 GetIamV2IpFilterSummary200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2IpFilterSummary200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2IpFilterSummary200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpFilterSummary200JSONResponseBodyKindIpFilterSummary GetIamV2IpFilterSummary200JSONResponseBodyKind = "IpFilterSummary"
)

func (e GetIamV2IpFilterSummary200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2IpFilterSummary200JSONResponseBodyKindIpFilterSummary:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpFilterJSONBodyApiVersionIamv2 CreateIamV2IpFilterJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IpFilterJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IpFilterJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpFilterJSONBodyKindIpFilter CreateIamV2IpFilterJSONBodyKind = "IpFilter"
)

func (e CreateIamV2IpFilterJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IpFilterJSONBodyKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpFilter201JSONResponseBodyApiVersionIamv2 CreateIamV2IpFilter201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IpFilter201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IpFilter201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpFilter201JSONResponseBodyKindIpFilter CreateIamV2IpFilter201JSONResponseBodyKind = "IpFilter"
)

func (e CreateIamV2IpFilter201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IpFilter201JSONResponseBodyKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpFilter200JSONResponseBodyApiVersionIamv2 GetIamV2IpFilter200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2IpFilter200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2IpFilter200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpFilter200JSONResponseBodyKindIpFilter GetIamV2IpFilter200JSONResponseBodyKind = "IpFilter"
)

func (e GetIamV2IpFilter200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2IpFilter200JSONResponseBodyKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IpFilter200JSONResponseBodyApiVersionIamv2 UpdateIamV2IpFilter200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2IpFilter200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2IpFilter200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IpFilter200JSONResponseBodyKindIpFilter UpdateIamV2IpFilter200JSONResponseBodyKind = "IpFilter"
)

func (e UpdateIamV2IpFilter200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2IpFilter200JSONResponseBodyKindIpFilter:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpGroupJSONBodyApiVersionIamv2 CreateIamV2IpGroupJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IpGroupJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IpGroupJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpGroupJSONBodyKindIpGroup CreateIamV2IpGroupJSONBodyKind = "IpGroup"
)

func (e CreateIamV2IpGroupJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IpGroupJSONBodyKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpGroup201JSONResponseBodyApiVersionIamv2 CreateIamV2IpGroup201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2IpGroup201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2IpGroup201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2IpGroup201JSONResponseBodyKindIpGroup CreateIamV2IpGroup201JSONResponseBodyKind = "IpGroup"
)

func (e CreateIamV2IpGroup201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2IpGroup201JSONResponseBodyKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpGroup200JSONResponseBodyApiVersionIamv2 GetIamV2IpGroup200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2IpGroup200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2IpGroup200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2IpGroup200JSONResponseBodyKindIpGroup GetIamV2IpGroup200JSONResponseBodyKind = "IpGroup"
)

func (e GetIamV2IpGroup200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2IpGroup200JSONResponseBodyKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IpGroup200JSONResponseBodyApiVersionIamv2 UpdateIamV2IpGroup200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2IpGroup200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2IpGroup200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2IpGroup200JSONResponseBodyKindIpGroup UpdateIamV2IpGroup200JSONResponseBodyKind = "IpGroup"
)

func (e UpdateIamV2IpGroup200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2IpGroup200JSONResponseBodyKindIpGroup:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2RoleBindingJSONBodyApiVersionIamv2 CreateIamV2RoleBindingJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2RoleBindingJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2RoleBindingJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2RoleBindingJSONBodyKindRoleBinding CreateIamV2RoleBindingJSONBodyKind = "RoleBinding"
)

func (e CreateIamV2RoleBindingJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2RoleBindingJSONBodyKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2RoleBinding201JSONResponseBodyApiVersionIamv2 CreateIamV2RoleBinding201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2RoleBinding201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2RoleBinding201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2RoleBinding201JSONResponseBodyKindRoleBinding CreateIamV2RoleBinding201JSONResponseBodyKind = "RoleBinding"
)

func (e CreateIamV2RoleBinding201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2RoleBinding201JSONResponseBodyKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2RoleBinding200JSONResponseBodyApiVersionIamv2 DeleteIamV2RoleBinding200JSONResponseBodyApiVersion = "iam/v2"
)

func (e DeleteIamV2RoleBinding200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case DeleteIamV2RoleBinding200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	DeleteIamV2RoleBinding200JSONResponseBodyKindRoleBinding DeleteIamV2RoleBinding200JSONResponseBodyKind = "RoleBinding"
)

func (e DeleteIamV2RoleBinding200JSONResponseBodyKind) Valid() bool {
	switch e {
	case DeleteIamV2RoleBinding200JSONResponseBodyKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	GetIamV2RoleBinding200JSONResponseBodyApiVersionIamv2 GetIamV2RoleBinding200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2RoleBinding200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2RoleBinding200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2RoleBinding200JSONResponseBodyKindRoleBinding GetIamV2RoleBinding200JSONResponseBodyKind = "RoleBinding"
)

func (e GetIamV2RoleBinding200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2RoleBinding200JSONResponseBodyKindRoleBinding:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ServiceAccountJSONBodyApiVersionIamv2 CreateIamV2ServiceAccountJSONBodyApiVersion = "iam/v2"
)

func (e CreateIamV2ServiceAccountJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2ServiceAccountJSONBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ServiceAccountJSONBodyKindServiceAccount CreateIamV2ServiceAccountJSONBodyKind = "ServiceAccount"
)

func (e CreateIamV2ServiceAccountJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2ServiceAccountJSONBodyKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ServiceAccount201JSONResponseBodyApiVersionIamv2 CreateIamV2ServiceAccount201JSONResponseBodyApiVersion = "iam/v2"
)

func (e CreateIamV2ServiceAccount201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2ServiceAccount201JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2ServiceAccount201JSONResponseBodyKindServiceAccount CreateIamV2ServiceAccount201JSONResponseBodyKind = "ServiceAccount"
)

func (e CreateIamV2ServiceAccount201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2ServiceAccount201JSONResponseBodyKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	GetIamV2ServiceAccount200JSONResponseBodyApiVersionIamv2 GetIamV2ServiceAccount200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2ServiceAccount200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2ServiceAccount200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2ServiceAccount200JSONResponseBodyKindServiceAccount GetIamV2ServiceAccount200JSONResponseBodyKind = "ServiceAccount"
)

func (e GetIamV2ServiceAccount200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2ServiceAccount200JSONResponseBodyKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2ServiceAccount200JSONResponseBodyApiVersionIamv2 UpdateIamV2ServiceAccount200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2ServiceAccount200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2ServiceAccount200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2ServiceAccount200JSONResponseBodyKindServiceAccount UpdateIamV2ServiceAccount200JSONResponseBodyKind = "ServiceAccount"
)

func (e UpdateIamV2ServiceAccount200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2ServiceAccount200JSONResponseBodyKindServiceAccount:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2SsoGroupMappingJSONBodyApiVersionIamV2sso CreateIamV2SsoGroupMappingJSONBodyApiVersion = "iam.v2/sso"
)

func (e CreateIamV2SsoGroupMappingJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2SsoGroupMappingJSONBodyApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2SsoGroupMappingJSONBodyKindGroupMapping CreateIamV2SsoGroupMappingJSONBodyKind = "GroupMapping"
)

func (e CreateIamV2SsoGroupMappingJSONBodyKind) Valid() bool {
	switch e {
	case CreateIamV2SsoGroupMappingJSONBodyKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2SsoGroupMapping201JSONResponseBodyApiVersionIamV2sso CreateIamV2SsoGroupMapping201JSONResponseBodyApiVersion = "iam.v2/sso"
)

func (e CreateIamV2SsoGroupMapping201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateIamV2SsoGroupMapping201JSONResponseBodyApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	CreateIamV2SsoGroupMapping201JSONResponseBodyKindGroupMapping CreateIamV2SsoGroupMapping201JSONResponseBodyKind = "GroupMapping"
)

func (e CreateIamV2SsoGroupMapping201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateIamV2SsoGroupMapping201JSONResponseBodyKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	GetIamV2SsoGroupMapping200JSONResponseBodyApiVersionIamV2sso GetIamV2SsoGroupMapping200JSONResponseBodyApiVersion = "iam.v2/sso"
)

func (e GetIamV2SsoGroupMapping200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2SsoGroupMapping200JSONResponseBodyApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	GetIamV2SsoGroupMapping200JSONResponseBodyKindGroupMapping GetIamV2SsoGroupMapping200JSONResponseBodyKind = "GroupMapping"
)

func (e GetIamV2SsoGroupMapping200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2SsoGroupMapping200JSONResponseBodyKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2SsoGroupMapping200JSONResponseBodyApiVersionIamV2sso UpdateIamV2SsoGroupMapping200JSONResponseBodyApiVersion = "iam.v2/sso"
)

func (e UpdateIamV2SsoGroupMapping200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2SsoGroupMapping200JSONResponseBodyApiVersionIamV2sso:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2SsoGroupMapping200JSONResponseBodyKindGroupMapping UpdateIamV2SsoGroupMapping200JSONResponseBodyKind = "GroupMapping"
)

func (e UpdateIamV2SsoGroupMapping200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2SsoGroupMapping200JSONResponseBodyKindGroupMapping:
		return true
	default:
		return false
	}
}

const (
	GetIamV2User200JSONResponseBodyApiVersionIamv2 GetIamV2User200JSONResponseBodyApiVersion = "iam/v2"
)

func (e GetIamV2User200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetIamV2User200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	GetIamV2User200JSONResponseBodyKindUser GetIamV2User200JSONResponseBodyKind = "User"
)

func (e GetIamV2User200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetIamV2User200JSONResponseBodyKindUser:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2User200JSONResponseBodyApiVersionIamv2 UpdateIamV2User200JSONResponseBodyApiVersion = "iam/v2"
)

func (e UpdateIamV2User200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateIamV2User200JSONResponseBodyApiVersionIamv2:
		return true
	default:
		return false
	}
}

const (
	UpdateIamV2User200JSONResponseBodyKindUser UpdateIamV2User200JSONResponseBodyKind = "User"
)

func (e UpdateIamV2User200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateIamV2User200JSONResponseBodyKindUser:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type DataType struct {
	// ClassName The class name of the structured data type (if applicable).
	ClassName *string `json:"class_name,omitempty"`

	// ElementType The type of the element in the data type (if applicable).
	ElementType *DataType `json:"element_type,omitempty"`

	// Fields The fields of the element in the data type (if applicable).
	Fields *[]RowFieldType `json:"fields,omitempty"`

	// FractionalPrecision The fractional precision of the data type (if applicable).
	FractionalPrecision *int32 `json:"fractional_precision,omitempty"`

	// KeyType The type of the key in the data type (if applicable).
	KeyType *DataType `json:"key_type,omitempty"`

	// Length The length of the data type.
	Length *int32 `json:"length,omitempty"`

	// Nullable Indicates whether values in this column can be null.
	Nullable bool `json:"nullable"`

	// Precision The precision of the data type.
	Precision *int32 `json:"precision,omitempty"`

	// Resolution The resolution of the data type (if applicable).
	Resolution *string `json:"resolution,omitempty"`

	// Scale The scale of the data type.
	Scale *int32 `json:"scale,omitempty"`

	// Type The data type of the column.
	Type string `json:"type"`

	// ValueType The type of the value in the data type (if applicable).
	ValueType *DataType `json:"value_type,omitempty"`
}
type Error struct {
	// Code An application-specific error code, expressed as a string value.
	Code *string `json:"code,omitempty"`

	// Detail A human-readable explanation specific to this occurrence of the problem.
	Detail    *string `json:"detail,omitempty"`
	ErrorCode *int32  `json:"error_code,omitempty"`

	// Id A unique identifier for this particular occurrence of the problem.
	Id      *string `json:"id,omitempty"`
	Message *string `json:"message,omitempty"`

	// Source If this error was caused by a particular part of the API request, the source will point to the query string parameter or request body property that caused it.
	Source *struct {
		// Parameter A string indicating which query parameter caused the error.
		Parameter *string `json:"parameter,omitempty"`

		// Pointer A JSON Pointer [RFC6901] to the associated entity in the request document [e.g. "/spec" for a spec object, or "/spec/title" for a specific field].
		Pointer *string `json:"pointer,omitempty"`
	} `json:"source,omitempty"`

	// Status The HTTP status code applicable to this problem, expressed as a string value.
	Status *string `json:"status,omitempty"`

	// Title A short, human-readable summary of the problem. It **SHOULD NOT** change from occurrence to occurrence of the problem, except for purposes of localization.
	Title *string `json:"title,omitempty"`
}
type Failure struct {
	// Errors List of errors which caused this operation to fail
	Errors []Error `json:"errors"`
}
type GlobalObjectReference struct {
	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
}
type ListMeta struct {
	// First A link to the first page of results. If a response does not contain a first link, then direct navigation to the first page is not supported.
	First *string `json:"first,omitempty"`

	// Last A link to the last page of results. If a response does not contain a last link, then direct navigation to the last page is not supported.
	Last *string `json:"last,omitempty"`

	// Next A link to the next page of results. If a response does not contain a next link, then there is no more data available.
	Next *string `json:"next,omitempty"`

	// Prev A link to the previous page of results. If a response does not contain a prev link, then either there is no previous data or backwards traversal through the result set is not supported.
	Prev *string `json:"prev,omitempty"`

	// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
	TotalSize *int32 `json:"total_size,omitempty"`
}
type MultipleSearchFilter = []string
type ObjectMeta struct {
	// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
	CreatedAt *time.Time `json:"created_at,omitempty"`

	// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
	DeletedAt *time.Time `json:"deleted_at,omitempty"`

	// ResourceName Resource Name is a Uniform Resource Identifier (URI) that is globally unique across space and time. It is represented as a Confluent Resource Name
	ResourceName *string `json:"resource_name,omitempty"`

	// Self Self is a Uniform Resource Locator (URL) at which an object can be addressed. This URL encodes the service location, API version, and other particulars necessary to locate the resource at a point in time
	Self string `json:"self"`

	// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}
type RowFieldType struct {
	// Description The description of the field.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType DataType `json:"field_type"`

	// Name The name of the field.
	Name string `json:"name"`
}
type SearchFilter = string
type TypedEnvScopedObjectReference struct {
	// ApiVersion API group and version of the referred resource
	ApiVersion *string `json:"api_version,omitempty"`

	// Environment Environment of the referred resource, if env-scoped
	Environment *string `json:"environment,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Kind Kind of the referred resource
	Kind *string `json:"kind,omitempty"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
}
type TypedGlobalObjectReference struct {
	// ApiVersion API group and version of the referred resource
	ApiVersion *string `json:"api_version,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Kind Kind of the referred resource
	Kind *string `json:"kind,omitempty"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
}
type ArtifactV1FlinkArtifact struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1FlinkArtifactApiVersion `json:"api_version,omitempty"`

	// Class Java class or alias for the artifact as provided by developer. Deprecated
	// Deprecated: this property has been marked as deprecated upstream, but no `x-deprecated-reason` was set
	Class *string `json:"class,omitempty"`

	// Cloud Cloud provider where the Flink Artifact archive is uploaded.
	Cloud *string `json:"cloud,omitempty"`

	// ContentFormat Archive format of the Flink Artifact.
	ContentFormat *string `json:"content_format,omitempty"`

	// Description Description of the Flink Artifact.
	Description *string `json:"description,omitempty"`

	// DisplayName Unique name of the Flink Artifact per cloud, region, environment scope.
	DisplayName *string `json:"display_name,omitempty"`

	// DocumentationLink Documentation link of the Flink Artifact.
	DocumentationLink *string `json:"documentation_link,omitempty"`

	// Environment Environment the Flink Artifact belongs to.
	Environment *string `json:"environment,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *ArtifactV1FlinkArtifactKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Region The Cloud provider region the Flink Artifact archive is uploaded.
	Region *string `json:"region,omitempty"`

	// RuntimeLanguage Runtime language of the Flink Artifact.
	RuntimeLanguage *string `json:"runtime_language,omitempty"`

	// Versions Versions associated with this Flink Artifact.
	Versions *[]ArtifactV1FlinkArtifactVersion `json:"versions,omitempty"`
}
type ArtifactV1FlinkArtifactApiVersion string
type ArtifactV1FlinkArtifactKind string
type ArtifactV1FlinkArtifactVersion struct {
	// ArtifactId The Flink Artifact this version belongs to.
	ArtifactId ArtifactV1FlinkArtifact `json:"artifact_id"`

	// IsBeta Flag to specify stability of the version
	IsBeta *bool `json:"is_beta,omitempty"`

	// ReleaseNotes Release Notes of the Flink Artifact version.
	ReleaseNotes *string `json:"release_notes,omitempty"`

	// UploadSource Upload source of the Flink Artifact Version.
	UploadSource ArtifactV1FlinkArtifactVersion_UploadSource `json:"upload_source"`

	// Version Version id of the Flink Artifact.
	Version string `json:"version"`
}
type ArtifactV1FlinkArtifactVersion_UploadSource struct {
	union json.RawMessage
}
type ArtifactV1UploadSourcePresignedUrl struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *ArtifactV1UploadSourcePresignedUrlApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *ArtifactV1UploadSourcePresignedUrlKind `json:"kind,omitempty"`

	// Location Location of the Flink Artifact source.
	Location *string `json:"location,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// UploadId Upload ID returned by the `/presigned-upload-url` API. This field returns an empty string in all responses.
	UploadId *string `json:"upload_id,omitempty"`
}
type ArtifactV1UploadSourcePresignedUrlApiVersion string
type ArtifactV1UploadSourcePresignedUrlKind string
type IamV2ApiKey struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2ApiKeyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2ApiKeyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Spec The desired state of the Api Key
	Spec *IamV2ApiKeySpec `json:"spec,omitempty"`
}
type IamV2ApiKeyApiVersion string
type IamV2ApiKeyKind string
type IamV2ApiKeyList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2ApiKeyListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2ApiKeyListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2ApiKeyListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
		Spec map[string]interface{} `json:"spec"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2ApiKeyListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2ApiKeyListApiVersion string
type IamV2ApiKeyListDataApiVersion string
type IamV2ApiKeyListDataKind string
type IamV2ApiKeyListKind string
type IamV2ApiKeySpec struct {
	// Description A human readable description for the API key
	Description *string `json:"description,omitempty"`

	// DisplayName A human readable name for the API key
	DisplayName *string `json:"display_name,omitempty"`

	// Owner The owner to which this belongs. The owner can be one of iam.v2.User, iam.v2.ServiceAccount.
	Owner *TypedGlobalObjectReference `json:"owner,omitempty"`

	// Resource The resource associated with this object. The resource can be one of Kafka Cluster ID (example: lkc-12345),
	// Schema Registry Cluster ID (example: lsrc-12345), ksqlDB Cluster ID (example: lksqlc-12345), or Flink
	// (Environment + Region pair, example: env-abc123.aws.us-east-2).
	// May be null or omitted if not associated with a resource. For creating Cloud API key, resource id should be `CLOUD`,
	// for creating Tableflow API key, resource id should be `TABLEFLOW`, for creating Global API key, resource id should be `GLOBAL`.
	// The resource id is case-insensitive.
	// [Learn more in Authentication](https://docs.confluent.io/cloud/current/api.html#section/Authentication).
	//
	// Note - Flink is in the [Preview lifecycle stage](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	Resource *TypedEnvScopedObjectReference `json:"resource,omitempty"`

	// Secret The API key secret. Only provided in `create` responses, not in `get` or `list`.
	Secret *string `json:"secret,omitempty"`
}
type IamV2CertificateAuthority struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2CertificateAuthorityApiVersion `json:"api_version,omitempty"`

	// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
	CertificateChainFilename *string `json:"certificate_chain_filename,omitempty"`

	// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
	// either local file uploaded (LOCAL) or from url of CRL (URL).
	CrlSource *string `json:"crl_source,omitempty"`

	// CrlUpdatedAt The timestamp for when CRL was last updated.
	CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description *string `json:"description,omitempty"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName *string `json:"display_name,omitempty"`

	// ExpirationDates The expiration dates of certificates in the chain.
	ExpirationDates *[]time.Time `json:"expiration_dates,omitempty"`

	// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
	// strings that act as unique identifiers for the certificates in the chain.
	Fingerprints *[]string `json:"fingerprints,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2CertificateAuthorityKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate *bool `json:"require_crl_on_client_certificate,omitempty"`

	// SerialNumbers The serial numbers for each certificate in the certificate chain.
	SerialNumbers *[]string `json:"serial_numbers,omitempty"`

	// State The current state of the certificate authority.
	State *string `json:"state,omitempty"`
}
type IamV2CertificateAuthorityApiVersion string
type IamV2CertificateAuthorityKind string
type IamV2CertificateAuthorityList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2CertificateAuthorityListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2CertificateAuthorityListDataApiVersion `json:"api_version,omitempty"`

		// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
		CertificateChainFilename string `json:"certificate_chain_filename"`

		// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
		// either local file uploaded (LOCAL) or from url of CRL (URL).
		CrlSource *string `json:"crl_source,omitempty"`

		// CrlUpdatedAt The timestamp for when CRL was last updated.
		CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

		// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
		CrlUrl *string `json:"crl_url,omitempty"`

		// Description A description of the certificate authority.
		Description string `json:"description"`

		// DisplayName The human-readable name of the certificate authority.
		DisplayName string `json:"display_name"`

		// ExpirationDates The expiration dates of certificates in the chain.
		ExpirationDates []time.Time `json:"expiration_dates"`

		// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
		// strings that act as unique identifiers for the certificates in the chain.
		Fingerprints []string `json:"fingerprints"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2CertificateAuthorityListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
		// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
		// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
		// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
		// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
		RequireCrlOnClientCertificate bool `json:"require_crl_on_client_certificate"`

		// SerialNumbers The serial numbers for each certificate in the certificate chain.
		SerialNumbers []string `json:"serial_numbers"`

		// State The current state of the certificate authority.
		State string `json:"state"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2CertificateAuthorityListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2CertificateAuthorityListApiVersion string
type IamV2CertificateAuthorityListDataApiVersion string
type IamV2CertificateAuthorityListDataKind string
type IamV2CertificateAuthorityListKind string
type IamV2CertificateIdentityPool struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2CertificateIdentityPoolApiVersion `json:"api_version,omitempty"`

	// Description A description of how this `IdentityPool` is used
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName *string `json:"display_name,omitempty"`

	// ExternalIdentifier The certificate field that will be used to represent the
	// pool's external identifier for audit logging.
	ExternalIdentifier *string `json:"external_identifier,omitempty"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
	Filter *string `json:"filter,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2CertificateIdentityPoolKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the identity pool
	State *string `json:"state,omitempty"`
}
type IamV2CertificateIdentityPoolApiVersion string
type IamV2CertificateIdentityPoolKind string
type IamV2CertificateIdentityPoolList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2CertificateIdentityPoolListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2CertificateIdentityPoolListDataApiVersion `json:"api_version,omitempty"`

		// Description A description of how this `IdentityPool` is used
		Description string `json:"description"`

		// DisplayName The name of the `IdentityPool`.
		DisplayName string `json:"display_name"`

		// ExternalIdentifier The certificate field that will be used to represent the
		// pool's external identifier for audit logging.
		ExternalIdentifier string `json:"external_identifier"`

		// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
		Filter string `json:"filter"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2CertificateIdentityPoolListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Principal Represents the federated identity associated with this pool.
		Principal string `json:"principal"`

		// State The current state of the identity pool
		State string `json:"state"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2CertificateIdentityPoolListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2CertificateIdentityPoolListApiVersion string
type IamV2CertificateIdentityPoolListDataApiVersion string
type IamV2CertificateIdentityPoolListDataKind string
type IamV2CertificateIdentityPoolListKind string
type IamV2CreateCertRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2CreateCertRequestApiVersion `json:"api_version,omitempty"`

	// CertificateChain The PEM encoded string containing the signing certificate chain
	// used to validate client certs.
	CertificateChain *string `json:"certificate_chain,omitempty"`

	// CertificateChainFilename The name of the certificate file.
	CertificateChainFilename *string `json:"certificate_chain_filename,omitempty"`

	// CrlChain The PEM encoded string containing the CRL for this certificate authority.
	// Defaults to this over `crl_url` if available.
	CrlChain *string `json:"crl_chain,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description *string `json:"description,omitempty"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2CreateCertRequestKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate *bool `json:"require_crl_on_client_certificate,omitempty"`
}
type IamV2CreateCertRequestApiVersion string
type IamV2CreateCertRequestKind string
type IamV2IdentityPool struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2IdentityPoolApiVersion `json:"api_version,omitempty"`

	// Description A description of how this `IdentityPool` is used
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName *string `json:"display_name,omitempty"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#supported-common-expression-language-cel-filters) that specifies which identities can authenticate using your identity pool (see [Set identity pool filters](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#set-identity-pool-filters) for more details).
	Filter *string `json:"filter,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// (see [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1) for more details).
	// This appears in the audit log records, showing, for example, that "identity Z used identity pool X to access
	// topic A".
	IdentityClaim *string `json:"identity_claim,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2IdentityPoolKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the identity pool
	State *string `json:"state,omitempty"`
}
type IamV2IdentityPoolApiVersion string
type IamV2IdentityPoolKind string
type IamV2IdentityPoolList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2IdentityPoolListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2IdentityPoolListDataApiVersion `json:"api_version,omitempty"`

		// Description A description of how this `IdentityPool` is used
		Description string `json:"description"`

		// DisplayName The name of the `IdentityPool`.
		DisplayName string `json:"display_name"`

		// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#supported-common-expression-language-cel-filters) that specifies which identities can authenticate using your identity pool (see [Set identity pool filters](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#set-identity-pool-filters) for more details).
		Filter string `json:"filter"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
		// (see [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1) for more details).
		// This appears in the audit log records, showing, for example, that "identity Z used identity pool X to access
		// topic A".
		IdentityClaim string `json:"identity_claim"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2IdentityPoolListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Principal Represents the federated identity associated with this pool.
		Principal string `json:"principal"`

		// State The current state of the identity pool
		State string `json:"state"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2IdentityPoolListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2IdentityPoolListApiVersion string
type IamV2IdentityPoolListDataApiVersion string
type IamV2IdentityPoolListDataKind string
type IamV2IdentityPoolListKind string
type IamV2IdentityProvider struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2IdentityProviderApiVersion `json:"api_version,omitempty"`

	// Description A description of the identity provider.
	Description *string `json:"description,omitempty"`

	// DisplayName The human-readable name of the OAuth identity provider.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1). This appears
	// in audit log records. Note: if the client specifies mapping to one identity pool ID, the identity
	// claim configured with that pool will be used instead.
	// Note - The attribute is in an [Early Access lifecycle stage]
	// (https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	IdentityClaim *string `json:"identity_claim,omitempty"`

	// Issuer A publicly accessible URL uniquely identifying the OAuth
	// identity provider authorized to issue access tokens.
	Issuer *string `json:"issuer,omitempty"`

	// JwksUri A publicly accessible JSON Web Key Set (JWKS) URI for the OAuth
	// identity provider. JWKS provides a set of crypotgraphic keys
	// used to verify the authenticity and integrity of JSON Web
	// Tokens (JWTs) issued by the OAuth identity provider.
	JwksUri *string `json:"jwks_uri,omitempty"`

	// Keys The JWKS issued by the OAuth identity provider. Only `kid` (key ID)
	// and `alg` (algorithm) properties for each key set are included.
	Keys *[]IamV2JwksObject `json:"keys,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2IdentityProviderKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// State The current state of the identity provider.
	State *string `json:"state,omitempty"`
}
type IamV2IdentityProviderApiVersion string
type IamV2IdentityProviderKind string
type IamV2IdentityProviderList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2IdentityProviderListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2IdentityProviderListDataApiVersion `json:"api_version,omitempty"`

		// Description A description of the identity provider.
		Description string `json:"description"`

		// DisplayName The human-readable name of the OAuth identity provider.
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
		// [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1). This appears
		// in audit log records. Note: if the client specifies mapping to one identity pool ID, the identity
		// claim configured with that pool will be used instead.
		// Note - The attribute is in an [Early Access lifecycle stage]
		// (https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
		IdentityClaim *string `json:"identity_claim,omitempty"`

		// Issuer A publicly accessible URL uniquely identifying the OAuth
		// identity provider authorized to issue access tokens.
		Issuer string `json:"issuer"`

		// JwksUri A publicly accessible JSON Web Key Set (JWKS) URI for the OAuth
		// identity provider. JWKS provides a set of crypotgraphic keys
		// used to verify the authenticity and integrity of JSON Web
		// Tokens (JWTs) issued by the OAuth identity provider.
		JwksUri string `json:"jwks_uri"`

		// Keys The JWKS issued by the OAuth identity provider. Only `kid` (key ID)
		// and `alg` (algorithm) properties for each key set are included.
		Keys *[]IamV2JwksObject `json:"keys,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2IdentityProviderListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// State The current state of the identity provider.
		State string `json:"state"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2IdentityProviderListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2IdentityProviderListApiVersion string
type IamV2IdentityProviderListDataApiVersion string
type IamV2IdentityProviderListDataKind string
type IamV2IdentityProviderListKind string
type IamV2Invitation struct {
	// AcceptedAt The timestamp that the invitation was accepted
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2InvitationApiVersion `json:"api_version,omitempty"`

	// AuthType The user/invitee's authentication type. Note that only the [OrganizationAdmin role](https://docs.confluent.io/cloud/current/access-management/access-control/cloud-rbac.html#organizationadmin)
	// can invite AUTH_TYPE_LOCAL users to SSO organizations.
	// The user's auth_type is set as AUTH_TYPE_SSO by default if the organization has SSO enabled.
	// Otherwise, the user's auth_type is AUTH_TYPE_LOCAL by default.
	AuthType *string `json:"auth_type,omitempty"`

	// Creator The invitation creator
	Creator *GlobalObjectReference `json:"creator,omitempty"`

	// Email The user/invitee's email address
	Email *openapi_types.Email `json:"email,omitempty"`

	// ExpiresAt The timestamp that the invitation will expire
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2InvitationKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Status The status of invitations
	Status *string `json:"status,omitempty"`

	// User The user/invitee
	User *GlobalObjectReference `json:"user,omitempty"`
}
type IamV2InvitationApiVersion string
type IamV2InvitationKind string
type IamV2InvitationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2InvitationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// AcceptedAt The timestamp that the invitation was accepted
		AcceptedAt *time.Time `json:"accepted_at,omitempty"`

		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2InvitationListDataApiVersion `json:"api_version,omitempty"`

		// AuthType The user/invitee's authentication type. Note that only the [OrganizationAdmin role](https://docs.confluent.io/cloud/current/access-management/access-control/cloud-rbac.html#organizationadmin)
		// can invite AUTH_TYPE_LOCAL users to SSO organizations.
		// The user's auth_type is set as AUTH_TYPE_SSO by default if the organization has SSO enabled.
		// Otherwise, the user's auth_type is AUTH_TYPE_LOCAL by default.
		AuthType *string `json:"auth_type,omitempty"`

		// Creator The invitation creator
		Creator *GlobalObjectReference `json:"creator,omitempty"`

		// Email The user/invitee's email address
		Email openapi_types.Email `json:"email"`

		// ExpiresAt The timestamp that the invitation will expire
		ExpiresAt *time.Time `json:"expires_at,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2InvitationListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Status The status of invitations
		Status *string `json:"status,omitempty"`

		// User The user/invitee
		User *GlobalObjectReference `json:"user,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2InvitationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2InvitationListApiVersion string
type IamV2InvitationListDataApiVersion string
type IamV2InvitationListDataKind string
type IamV2InvitationListKind string
type IamV2IpFilter struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2IpFilterApiVersion `json:"api_version,omitempty"`

	// FilterName A human readable name for an IP Filter. Can contain any unicode letter or number, the ASCII space character,
	// or any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	FilterName *string `json:"filter_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IpGroups A list of IP Groups.
	IpGroups *[]GlobalObjectReference `json:"ip_groups,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2IpFilterKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// OperationGroups Scope of resources covered by this IP filter. Resource group must be set to 'multiple'
	// in order to use this property.During update operations, note that the operation
	// groups passed in will replace the list of existing operation groups
	// (passing in an empty list will remove all operation groups) from the filter
	// (in line with the behavior for ip_groups).
	OperationGroups *[]string `json:"operation_groups,omitempty"`

	// ResourceGroup Scope of resources covered by this IP filter. Available resource groups include "management" and "multiple".
	ResourceGroup *string `json:"resource_group,omitempty"`

	// ResourceScope A CRN that specifies the scope of the ip filter, specifically the organization
	// or environment. Without specifying this property, the ip filter
	// would apply to the whole organization.
	ResourceScope *string `json:"resource_scope,omitempty"`
}
type IamV2IpFilterApiVersion string
type IamV2IpFilterKind string
type IamV2IpFilterList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2IpFilterListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2IpFilterListDataApiVersion `json:"api_version,omitempty"`

		// FilterName A human readable name for an IP Filter. Can contain any unicode letter or number, the ASCII space character,
		// or any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
		FilterName string `json:"filter_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// IpGroups A list of IP Groups.
		IpGroups []GlobalObjectReference `json:"ip_groups"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2IpFilterListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// OperationGroups Scope of resources covered by this IP filter. Resource group must be set to 'multiple'
		// in order to use this property.During update operations, note that the operation
		// groups passed in will replace the list of existing operation groups
		// (passing in an empty list will remove all operation groups) from the filter
		// (in line with the behavior for ip_groups).
		OperationGroups *[]string `json:"operation_groups,omitempty"`

		// ResourceGroup Scope of resources covered by this IP filter. Available resource groups include "management" and "multiple".
		ResourceGroup string `json:"resource_group"`

		// ResourceScope A CRN that specifies the scope of the ip filter, specifically the organization
		// or environment. Without specifying this property, the ip filter
		// would apply to the whole organization.
		ResourceScope *string `json:"resource_scope,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2IpFilterListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2IpFilterListApiVersion string
type IamV2IpFilterListDataApiVersion string
type IamV2IpFilterListDataKind string
type IamV2IpFilterListKind string
type IamV2IpFilterSummary struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2IpFilterSummaryApiVersion `json:"api_version,omitempty"`

	// Categories Summary of the operation groups and IP filters created in those operation groups.
	Categories *[]struct {
		// Name Name of the category.
		Name *string `json:"name,omitempty"`

		// OperationGroups Operation groups part of this category.
		OperationGroups *[]struct {
			// Name Name of the operation group.
			Name *string `json:"name,omitempty"`

			// Status Open, limited, or no access.
			Status *string `json:"status,omitempty"`
		} `json:"operation_groups,omitempty"`

		// Status Open, limited, or mixed.
		Status *string `json:"status,omitempty"`
	} `json:"categories,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *IamV2IpFilterSummaryKind `json:"kind,omitempty"`

	// Scope The scope associated with this object.
	Scope *string `json:"scope,omitempty"`
}
type IamV2IpFilterSummaryApiVersion string
type IamV2IpFilterSummaryKind string
type IamV2IpGroup struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2IpGroupApiVersion `json:"api_version,omitempty"`

	// CidrBlocks A list of CIDRs.
	CidrBlocks *[]string `json:"cidr_blocks,omitempty"`

	// GroupName A human readable name for an IP Group. Can contain any unicode letter or number, the ASCII space character, or
	// any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	GroupName *string `json:"group_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2IpGroupKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
}
type IamV2IpGroupApiVersion string
type IamV2IpGroupKind string
type IamV2IpGroupList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2IpGroupListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2IpGroupListDataApiVersion `json:"api_version,omitempty"`

		// CidrBlocks A list of CIDRs.
		CidrBlocks []string `json:"cidr_blocks"`

		// GroupName A human readable name for an IP Group. Can contain any unicode letter or number, the ASCII space character, or
		// any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
		GroupName string `json:"group_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2IpGroupListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2IpGroupListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2IpGroupListApiVersion string
type IamV2IpGroupListDataApiVersion string
type IamV2IpGroupListDataKind string
type IamV2IpGroupListKind string
type IamV2Jwks struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2JwksApiVersion `json:"api_version,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *IamV2JwksKind `json:"kind,omitempty"`

	// Spec The desired state of the Jwks
	Spec *IamV2JwksSpec `json:"spec,omitempty"`

	// Status The status of the Jwks
	Status *IamV2JwksStatus `json:"status,omitempty"`
}
type IamV2JwksApiVersion string
type IamV2JwksKind string
type IamV2JwksObject struct {
	// Alg Specifies the algorithm to be used to generate the public key
	Alg string `json:"alg"`

	// E Specifies the exponent of the RSA public key.
	E *string `json:"e,omitempty"`

	// Kid Specifies the key-id issued by the OpenIDProvider for the particular tenant
	Kid string `json:"kid"`

	// Kty Specifies the cryptographic algorithm family used with the key
	Kty string `json:"kty"`

	// N Specifies the modulus of the RSA public key. Represented as a Base64urlUInt-encoded value
	N *string `json:"n,omitempty"`

	// Use Specifies the intended usage of the key
	Use *string `json:"use,omitempty"`
}
type IamV2JwksSpec struct {
	// JwksStatus The desired state of the public key data
	JwksStatus *string `json:"jwks_status,omitempty"`
}
type IamV2JwksStatus struct {
	// JwksLastRefreshAt The last successful refresh time for the public key data
	JwksLastRefreshAt *time.Time `json:"jwks_last_refresh_at,omitempty"`

	// JwksStatus The actual state of the public key data
	JwksStatus *string `json:"jwks_status,omitempty"`
}
type IamV2RoleBinding struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2RoleBindingApiVersion `json:"api_version,omitempty"`

	// CrnPattern A CRN that specifies the scope and resource patterns necessary for the role to bind
	CrnPattern *string `json:"crn_pattern,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2RoleBindingKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The principal User to bind the role to
	Principal *string `json:"principal,omitempty"`

	// RoleName The name of the role to bind to the principal
	RoleName *string `json:"role_name,omitempty"`
}
type IamV2RoleBindingApiVersion string
type IamV2RoleBindingKind string
type IamV2RoleBindingList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2RoleBindingListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2RoleBindingListDataApiVersion `json:"api_version,omitempty"`

		// CrnPattern A CRN that specifies the scope and resource patterns necessary for the role to bind
		CrnPattern string `json:"crn_pattern"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2RoleBindingListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Principal The principal User to bind the role to
		Principal string `json:"principal"`

		// RoleName The name of the role to bind to the principal
		RoleName string `json:"role_name"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2RoleBindingListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2RoleBindingListApiVersion string
type IamV2RoleBindingListDataApiVersion string
type IamV2RoleBindingListDataKind string
type IamV2RoleBindingListKind string
type IamV2ServiceAccount struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2ServiceAccountApiVersion `json:"api_version,omitempty"`

	// Description A free-form description of the Service Account
	Description *string `json:"description,omitempty"`

	// DisplayName A human-readable name for the Service Account
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2ServiceAccountKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
}
type IamV2ServiceAccountApiVersion string
type IamV2ServiceAccountKind string
type IamV2ServiceAccountList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2ServiceAccountListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2ServiceAccountListDataApiVersion `json:"api_version,omitempty"`

		// Description A free-form description of the Service Account
		Description *string `json:"description,omitempty"`

		// DisplayName A human-readable name for the Service Account
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2ServiceAccountListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2ServiceAccountListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2ServiceAccountListApiVersion string
type IamV2ServiceAccountListDataApiVersion string
type IamV2ServiceAccountListDataKind string
type IamV2ServiceAccountListKind string
type IamV2UpdateCertRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2UpdateCertRequestApiVersion `json:"api_version,omitempty"`

	// CertificateChain The PEM encoded string containing the signing certificate chain
	// used to validate client certs.
	CertificateChain *string `json:"certificate_chain,omitempty"`

	// CertificateChainFilename The name of the certificate file. Must be set if certificate is updated.
	CertificateChainFilename *string `json:"certificate_chain_filename,omitempty"`

	// CrlChain The PEM encoded string containing the CRL for this certificate authority.
	// Defaults to this over `crl_url` if available.
	CrlChain *string `json:"crl_chain,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description *string `json:"description,omitempty"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2UpdateCertRequestKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate *bool `json:"require_crl_on_client_certificate,omitempty"`
}
type IamV2UpdateCertRequestApiVersion string
type IamV2UpdateCertRequestKind string
type IamV2User struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2UserApiVersion `json:"api_version,omitempty"`

	// AuthType The user's authentication method
	AuthType *string `json:"auth_type,omitempty"`

	// Email The user's email address
	Email *openapi_types.Email `json:"email,omitempty"`

	// FullName The user's full name
	FullName *string `json:"full_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2UserKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
}
type IamV2UserApiVersion string
type IamV2UserKind string
type IamV2UserConfigureUserAuthRequest struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2UserConfigureUserAuthRequestApiVersion `json:"api_version,omitempty"`

	// AuthType The user's authentication method.
	AuthType *string `json:"auth_type,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2UserConfigureUserAuthRequestKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
}
type IamV2UserConfigureUserAuthRequestApiVersion string
type IamV2UserConfigureUserAuthRequestKind string
type IamV2UserList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2UserListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2UserListDataApiVersion `json:"api_version,omitempty"`

		// AuthType The user's authentication method
		AuthType *string `json:"auth_type,omitempty"`

		// Email The user's email address
		Email openapi_types.Email `json:"email"`

		// FullName The user's full name
		FullName *string `json:"full_name,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2UserListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2UserListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2UserListApiVersion string
type IamV2UserListDataApiVersion string
type IamV2UserListDataKind string
type IamV2UserListKind string
type IamV2SsoGroupMapping struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *IamV2SsoGroupMappingApiVersion `json:"api_version,omitempty"`

	// Description A description explaining the purpose and use of the group mapping.
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the group mapping.
	DisplayName *string `json:"display_name,omitempty"`

	// Filter A single group identifier or a condition based on [supported CEL operators](https://docs.confluent.io/cloud/current/access-management/authenticate/sso/group-mapping/overview.html#supported-cel-operators-for-group-mapping) that defines which groups are included.
	Filter *string `json:"filter,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *IamV2SsoGroupMappingKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The unique federated identity associated with this group mapping.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the group mapping.
	State *string `json:"state,omitempty"`
}
type IamV2SsoGroupMappingApiVersion string
type IamV2SsoGroupMappingKind string
type IamV2SsoGroupMappingList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion IamV2SsoGroupMappingListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *IamV2SsoGroupMappingListDataApiVersion `json:"api_version,omitempty"`

		// Description A description explaining the purpose and use of the group mapping.
		Description string `json:"description"`

		// DisplayName The name of the group mapping.
		DisplayName string `json:"display_name"`

		// Filter A single group identifier or a condition based on [supported CEL operators](https://docs.confluent.io/cloud/current/access-management/authenticate/sso/group-mapping/overview.html#supported-cel-operators-for-group-mapping) that defines which groups are included.
		Filter string `json:"filter"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *IamV2SsoGroupMappingListDataKind `json:"kind,omitempty"`
		Metadata struct {
			// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
			CreatedAt *time.Time `json:"created_at,omitempty"`

			// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
			DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
			ResourceName interface{} `json:"resource_name,omitempty"`
			Self         interface{} `json:"self"`

			// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
			UpdatedAt *time.Time `json:"updated_at,omitempty"`
		} `json:"metadata"`

		// Principal The unique federated identity associated with this group mapping.
		Principal string `json:"principal"`

		// State The current state of the group mapping.
		State string `json:"state"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     IamV2SsoGroupMappingListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type IamV2SsoGroupMappingListApiVersion string
type IamV2SsoGroupMappingListDataApiVersion string
type IamV2SsoGroupMappingListDataKind string
type IamV2SsoGroupMappingListKind string
type QueryV1alpha1DataType struct {
	// ClassName Class name of a structured type. Present in the Flink type model; the engine does not currently emit structured types.
	ClassName *string `json:"class_name,omitempty"`

	// ElementType Element type of an `ARRAY` or `MULTISET`.
	ElementType *QueryV1alpha1DataType `json:"elementType,omitempty"`

	// Fields Fields of a `ROW`, in declaration order.
	Fields *[]QueryV1alpha1RowFieldType `json:"fields,omitempty"`

	// FractionalPrecision Fractional-second precision of `INTERVAL_DAY_TIME`.
	FractionalPrecision *int32 `json:"fractionalPrecision,omitempty"`

	// KeyType Key type of a `MAP`.
	KeyType *QueryV1alpha1DataType `json:"keyType,omitempty"`

	// Length Declared length of `CHAR`, `VARCHAR`, `BINARY` and `VARBINARY`. Unbounded `VARCHAR` and `VARBINARY` report 2147483647.
	Length *int32 `json:"length,omitempty"`

	// Nullable Whether values of this column or field can be null.
	Nullable bool `json:"nullable"`

	// Precision Declared precision of `DECIMAL`, the `TIME`/`TIMESTAMP` types and the `INTERVAL_*` types.
	Precision *int32 `json:"precision,omitempty"`

	// Resolution Interval resolution, for example `YEAR_TO_MONTH` for `INTERVAL_YEAR_MONTH` or `DAY_TO_SECOND` for `INTERVAL_DAY_TIME`.
	Resolution *string `json:"resolution,omitempty"`

	// Scale Declared scale of `DECIMAL`.
	Scale *int32 `json:"scale,omitempty"`

	// Type The Flink logical type name of the column or field.
	Type string `json:"type"`

	// ValueType Value type of a `MAP`.
	ValueType *QueryV1alpha1DataType `json:"valueType,omitempty"`
}
type QueryV1alpha1ResultValue struct {
	union json.RawMessage
}
type QueryV1alpha1ResultValue0 = string
type QueryV1alpha1ResultValue1 = []QueryV1alpha1ResultValue
type QueryV1alpha1RowFieldType struct {
	// Description Optional field comment from the type declaration.
	Description *string `json:"description,omitempty"`

	// FieldType The data type of the field.
	FieldType QueryV1alpha1DataType `json:"fieldType"`

	// Name The name of the field.
	Name string `json:"name"`
}
type ClusterId = string
type ConfigName = string
type ConsumerGroupId = string
type ConsumerId = string
type GroupId = string
type IncludePartitionLevelTruncationData = bool
type LinkConfigName = string
type LinkName = string
type MemberId = string
type MirrorTopicName = string
type PartitionId = int
type SubtopologyId = string
type TopicName = string
type ValidateOnly = bool
type BadRequestError = Failure
type ConflictError = Failure
type DefaultSystemError = Failure
type NotFoundError = Failure
type OverQuotaError = Failure
type UnauthenticatedError = Failure
type UnauthorizedError = Failure
type ValidationError = Failure
type UpdateAuthTypeIamV2UserJSONRequestBody = IamV2UserConfigureUserAuthRequest

func (t ArtifactV1FlinkArtifactVersion_UploadSource) AsArtifactV1UploadSourcePresignedUrl() (ArtifactV1UploadSourcePresignedUrl, error) {
	var body ArtifactV1UploadSourcePresignedUrl
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) FromArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	t.union = b
	return err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) MergeArtifactV1UploadSourcePresignedUrl(v ArtifactV1UploadSourcePresignedUrl) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"location":"PRESIGNED_URL_LOCATION"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"location"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PRESIGNED_URL_LOCATION":
		return t.AsArtifactV1UploadSourcePresignedUrl()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t ArtifactV1FlinkArtifactVersion_UploadSource) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *ArtifactV1FlinkArtifactVersion_UploadSource) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t QueryV1alpha1ResultValue) AsQueryV1alpha1ResultValue0() (QueryV1alpha1ResultValue0, error) {
	var body QueryV1alpha1ResultValue0
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *QueryV1alpha1ResultValue) FromQueryV1alpha1ResultValue0(v QueryV1alpha1ResultValue0) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *QueryV1alpha1ResultValue) MergeQueryV1alpha1ResultValue0(v QueryV1alpha1ResultValue0) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t QueryV1alpha1ResultValue) AsQueryV1alpha1ResultValue1() (QueryV1alpha1ResultValue1, error) {
	var body QueryV1alpha1ResultValue1
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *QueryV1alpha1ResultValue) FromQueryV1alpha1ResultValue1(v QueryV1alpha1ResultValue1) error {
	b, err := json.Marshal(v)
	t.union = b
	return err
}
func (t *QueryV1alpha1ResultValue) MergeQueryV1alpha1ResultValue1(v QueryV1alpha1ResultValue1) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t QueryV1alpha1ResultValue) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *QueryV1alpha1ResultValue) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}

type RequestEditorFn func(ctx context.Context, req *http.Request) error
type HttpRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}
type Client struct {
	// The endpoint of the server conforming to this interface, with scheme,
	// https://api.deepmap.com for example. This can contain a path relative
	// to the server, such as https://api.deepmap.com/dev-test, and all the
	// paths in the swagger spec will be appended to the server.
	Server string

	// Doer for performing requests, typically a *http.Client with any
	// customized settings, such as certificate chains.
	Client HttpRequestDoer

	// A list of callbacks for modifying requests which are generated before sending over
	// the network.
	RequestEditors []RequestEditorFn
}
type ClientOption func(*Client) error

func NewClient(server string, opts ...ClientOption) (*Client, error) {
	// create a client with sane default values
	client := Client{
		Server: server,
	}
	// mutate client and add all optional params
	for _, o := range opts {
		if err := o(&client); err != nil {
			return nil, err
		}
	}
	// ensure the server URL always has a trailing slash
	if !strings.HasSuffix(client.Server, "/") {
		client.Server += "/"
	}
	// create httpClient, if not already present
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	return &client, nil
}
func WithHTTPClient(doer HttpRequestDoer) ClientOption {
	return func(c *Client) error {
		c.Client = doer
		return nil
	}
}
func WithRequestEditorFn(fn RequestEditorFn) ClientOption {
	return func(c *Client) error {
		c.RequestEditors = append(c.RequestEditors, fn)
		return nil
	}
}

type ClientInterface interface {

	// ListIamV2ApiKeys List of API Keys
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all API keys.
	//
	// This can show all keys for a single owner (across resources - Kafka clusters), or all keys for a single
	// resource (across owners). If no `owner` or `resource` filters are specified, returns all API Keys in the
	// organization. You will only see the keys that are accessible to the account making the API request.
	//
	// Corresponds with GET /iam/v2/api-keys (the `ListIamV2ApiKeys` operationId).
	ListIamV2ApiKeys(ctx context.Context, params *ListIamV2ApiKeysParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2ApiKeyWithBody Create an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an API key.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/api-keys (the `CreateIamV2ApiKey` operationId).
	CreateIamV2ApiKeyWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2ApiKey Create an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an API key.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/api-keys (the `CreateIamV2ApiKey` operationId).
	CreateIamV2ApiKey(ctx context.Context, body CreateIamV2ApiKeyJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2ApiKey Delete an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an API key.
	//
	// Corresponds with DELETE /iam/v2/api-keys/{id} (the `DeleteIamV2ApiKey` operationId).
	DeleteIamV2ApiKey(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2ApiKey Read an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an API key.
	//
	// Corresponds with GET /iam/v2/api-keys/{id} (the `GetIamV2ApiKey` operationId).
	GetIamV2ApiKey(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2ApiKeyWithBody Update an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an API key.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/api-keys/{id} (the `UpdateIamV2ApiKey` operationId).
	UpdateIamV2ApiKeyWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2ApiKey Update an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an API key.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/api-keys/{id} (the `UpdateIamV2ApiKey` operationId).
	UpdateIamV2ApiKey(ctx context.Context, id string, body UpdateIamV2ApiKeyJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2CertificateAuthorities List of Certificate Authorities
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all certificate authorities.
	//
	// Corresponds with GET /iam/v2/certificate-authorities (the `ListIamV2CertificateAuthorities` operationId).
	ListIamV2CertificateAuthorities(ctx context.Context, params *ListIamV2CertificateAuthoritiesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2CertificateAuthorityWithBody Create a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate authority.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/certificate-authorities (the `CreateIamV2CertificateAuthority` operationId).
	CreateIamV2CertificateAuthorityWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2CertificateAuthority Create a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate authority.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/certificate-authorities (the `CreateIamV2CertificateAuthority` operationId).
	CreateIamV2CertificateAuthority(ctx context.Context, body CreateIamV2CertificateAuthorityJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2CertificateIdentityPools List of Certificate Identity Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all certificate identity pools.
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `ListIamV2CertificateIdentityPools` operationId).
	ListIamV2CertificateIdentityPools(ctx context.Context, certificateAuthorityId string, params *ListIamV2CertificateIdentityPoolsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2CertificateIdentityPoolWithBody Create a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate identity pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `CreateIamV2CertificateIdentityPool` operationId).
	CreateIamV2CertificateIdentityPoolWithBody(ctx context.Context, certificateAuthorityId string, params *CreateIamV2CertificateIdentityPoolParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2CertificateIdentityPool Create a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate identity pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `CreateIamV2CertificateIdentityPool` operationId).
	CreateIamV2CertificateIdentityPool(ctx context.Context, certificateAuthorityId string, params *CreateIamV2CertificateIdentityPoolParams, body CreateIamV2CertificateIdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2CertificateIdentityPool Delete a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a certificate identity pool.
	//
	// Corresponds with DELETE /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `DeleteIamV2CertificateIdentityPool` operationId).
	DeleteIamV2CertificateIdentityPool(ctx context.Context, certificateAuthorityId string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2CertificateIdentityPool Read a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a certificate identity pool.
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `GetIamV2CertificateIdentityPool` operationId).
	GetIamV2CertificateIdentityPool(ctx context.Context, certificateAuthorityId string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2CertificateIdentityPoolWithBody Update a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate identity pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `UpdateIamV2CertificateIdentityPool` operationId).
	UpdateIamV2CertificateIdentityPoolWithBody(ctx context.Context, certificateAuthorityId string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2CertificateIdentityPool Update a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate identity pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `UpdateIamV2CertificateIdentityPool` operationId).
	UpdateIamV2CertificateIdentityPool(ctx context.Context, certificateAuthorityId string, id string, body UpdateIamV2CertificateIdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2CertificateAuthority Delete a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a certificate authority.
	//
	// Corresponds with DELETE /iam/v2/certificate-authorities/{id} (the `DeleteIamV2CertificateAuthority` operationId).
	DeleteIamV2CertificateAuthority(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2CertificateAuthority Read a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a certificate authority.
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{id} (the `GetIamV2CertificateAuthority` operationId).
	GetIamV2CertificateAuthority(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2CertificateAuthorityWithBody Update a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate authority.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{id} (the `UpdateIamV2CertificateAuthority` operationId).
	UpdateIamV2CertificateAuthorityWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2CertificateAuthority Update a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate authority.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{id} (the `UpdateIamV2CertificateAuthority` operationId).
	UpdateIamV2CertificateAuthority(ctx context.Context, id string, body UpdateIamV2CertificateAuthorityJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2IdentityProviders List of Identity Providers
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all identity providers.
	//
	// Corresponds with GET /iam/v2/identity-providers (the `ListIamV2IdentityProviders` operationId).
	ListIamV2IdentityProviders(ctx context.Context, params *ListIamV2IdentityProvidersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IdentityProviderWithBody Create an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity provider.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/identity-providers (the `CreateIamV2IdentityProvider` operationId).
	CreateIamV2IdentityProviderWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IdentityProvider Create an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity provider.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/identity-providers (the `CreateIamV2IdentityProvider` operationId).
	CreateIamV2IdentityProvider(ctx context.Context, body CreateIamV2IdentityProviderJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2IdentityProvider Delete an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an identity provider.
	//
	// Corresponds with DELETE /iam/v2/identity-providers/{id} (the `DeleteIamV2IdentityProvider` operationId).
	DeleteIamV2IdentityProvider(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2IdentityProvider Read an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an identity provider.
	//
	// Corresponds with GET /iam/v2/identity-providers/{id} (the `GetIamV2IdentityProvider` operationId).
	GetIamV2IdentityProvider(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IdentityProviderWithBody Update an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity provider.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{id} (the `UpdateIamV2IdentityProvider` operationId).
	UpdateIamV2IdentityProviderWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IdentityProvider Update an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity provider.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{id} (the `UpdateIamV2IdentityProvider` operationId).
	UpdateIamV2IdentityProvider(ctx context.Context, id string, body UpdateIamV2IdentityProviderJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2IdentityPools List of Identity Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all identity pools.
	//
	// Corresponds with GET /iam/v2/identity-providers/{provider_id}/identity-pools (the `ListIamV2IdentityPools` operationId).
	ListIamV2IdentityPools(ctx context.Context, providerId string, params *ListIamV2IdentityPoolsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IdentityPoolWithBody Create an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/identity-providers/{provider_id}/identity-pools (the `CreateIamV2IdentityPool` operationId).
	CreateIamV2IdentityPoolWithBody(ctx context.Context, providerId string, params *CreateIamV2IdentityPoolParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IdentityPool Create an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/identity-providers/{provider_id}/identity-pools (the `CreateIamV2IdentityPool` operationId).
	CreateIamV2IdentityPool(ctx context.Context, providerId string, params *CreateIamV2IdentityPoolParams, body CreateIamV2IdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2IdentityPool Delete an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an identity pool.
	//
	// Corresponds with DELETE /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `DeleteIamV2IdentityPool` operationId).
	DeleteIamV2IdentityPool(ctx context.Context, providerId string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2IdentityPool Read an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an identity pool.
	//
	// Corresponds with GET /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `GetIamV2IdentityPool` operationId).
	GetIamV2IdentityPool(ctx context.Context, providerId string, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IdentityPoolWithBody Update an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity pool.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `UpdateIamV2IdentityPool` operationId).
	UpdateIamV2IdentityPoolWithBody(ctx context.Context, providerId string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IdentityPool Update an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity pool.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `UpdateIamV2IdentityPool` operationId).
	UpdateIamV2IdentityPool(ctx context.Context, providerId string, id string, body UpdateIamV2IdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RefreshIamV2JsonWebKeySetWithBody Refresh a provider's JWKS
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to refresh the provider's JWKS
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/jwks (the `RefreshIamV2JsonWebKeySet` operationId).
	RefreshIamV2JsonWebKeySetWithBody(ctx context.Context, providerId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// RefreshIamV2JsonWebKeySet Refresh a provider's JWKS
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to refresh the provider's JWKS
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/jwks (the `RefreshIamV2JsonWebKeySet` operationId).
	RefreshIamV2JsonWebKeySet(ctx context.Context, providerId string, body RefreshIamV2JsonWebKeySetJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2Invitations List of Invitations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all invitations.
	//
	// Corresponds with GET /iam/v2/invitations (the `ListIamV2Invitations` operationId).
	ListIamV2Invitations(ctx context.Context, params *ListIamV2InvitationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2InvitationWithBody Create an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an invitation.
	//
	// The newly invited user will not have any permissions. Give the user permission by assigning them to one or
	// more roles by creating
	// [role bindings](https://docs.confluent.io/cloud/current/api.html#tag/Role-Bindings-(iamv2))
	// for the created `user`.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/invitations (the `CreateIamV2Invitation` operationId).
	CreateIamV2InvitationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2Invitation Create an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an invitation.
	//
	// The newly invited user will not have any permissions. Give the user permission by assigning them to one or
	// more roles by creating
	// [role bindings](https://docs.confluent.io/cloud/current/api.html#tag/Role-Bindings-(iamv2))
	// for the created `user`.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/invitations (the `CreateIamV2Invitation` operationId).
	CreateIamV2Invitation(ctx context.Context, body CreateIamV2InvitationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2Invitation Delete an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an invitation.
	//
	// Delete will deactivate the user if the user didn't accept the invitation yet.
	//
	// Corresponds with DELETE /iam/v2/invitations/{id} (the `DeleteIamV2Invitation` operationId).
	DeleteIamV2Invitation(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2Invitation Read an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an invitation.
	//
	// Corresponds with GET /iam/v2/invitations/{id} (the `GetIamV2Invitation` operationId).
	GetIamV2Invitation(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2IpFilterSummary Read an IP Filter Summary
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP filter summary.
	//
	// Corresponds with GET /iam/v2/ip-filter-summary (the `GetIamV2IpFilterSummary` operationId).
	GetIamV2IpFilterSummary(ctx context.Context, params *GetIamV2IpFilterSummaryParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2IpFilters List of IP Filters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all IP filters.
	//
	// Corresponds with GET /iam/v2/ip-filters (the `ListIamV2IpFilters` operationId).
	ListIamV2IpFilters(ctx context.Context, params *ListIamV2IpFiltersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IpFilterWithBody Create an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP filter.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/ip-filters (the `CreateIamV2IpFilter` operationId).
	CreateIamV2IpFilterWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IpFilter Create an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP filter.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/ip-filters (the `CreateIamV2IpFilter` operationId).
	CreateIamV2IpFilter(ctx context.Context, body CreateIamV2IpFilterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2IpFilter Delete an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an IP filter.
	//
	// Corresponds with DELETE /iam/v2/ip-filters/{id} (the `DeleteIamV2IpFilter` operationId).
	DeleteIamV2IpFilter(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2IpFilter Read an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP filter.
	//
	// Corresponds with GET /iam/v2/ip-filters/{id} (the `GetIamV2IpFilter` operationId).
	GetIamV2IpFilter(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IpFilterWithBody Update an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP filter.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/ip-filters/{id} (the `UpdateIamV2IpFilter` operationId).
	UpdateIamV2IpFilterWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IpFilter Update an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP filter.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/ip-filters/{id} (the `UpdateIamV2IpFilter` operationId).
	UpdateIamV2IpFilter(ctx context.Context, id string, body UpdateIamV2IpFilterJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2IpGroups List of IP Groups
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all IP groups.
	//
	// Corresponds with GET /iam/v2/ip-groups (the `ListIamV2IpGroups` operationId).
	ListIamV2IpGroups(ctx context.Context, params *ListIamV2IpGroupsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IpGroupWithBody Create an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP group.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/ip-groups (the `CreateIamV2IpGroup` operationId).
	CreateIamV2IpGroupWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2IpGroup Create an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP group.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/ip-groups (the `CreateIamV2IpGroup` operationId).
	CreateIamV2IpGroup(ctx context.Context, body CreateIamV2IpGroupJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2IpGroup Delete an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an IP group.
	//
	// Corresponds with DELETE /iam/v2/ip-groups/{id} (the `DeleteIamV2IpGroup` operationId).
	DeleteIamV2IpGroup(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2IpGroup Read an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP group.
	//
	// Corresponds with GET /iam/v2/ip-groups/{id} (the `GetIamV2IpGroup` operationId).
	GetIamV2IpGroup(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IpGroupWithBody Update an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP group.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/ip-groups/{id} (the `UpdateIamV2IpGroup` operationId).
	UpdateIamV2IpGroupWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2IpGroup Update an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP group.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/ip-groups/{id} (the `UpdateIamV2IpGroup` operationId).
	UpdateIamV2IpGroup(ctx context.Context, id string, body UpdateIamV2IpGroupJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2RoleBindings List of Role Bindings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all role bindings.
	//
	// Corresponds with GET /iam/v2/role-bindings (the `ListIamV2RoleBindings` operationId).
	ListIamV2RoleBindings(ctx context.Context, params *ListIamV2RoleBindingsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2RoleBindingWithBody Create a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a role binding.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/role-bindings (the `CreateIamV2RoleBinding` operationId).
	CreateIamV2RoleBindingWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2RoleBinding Create a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a role binding.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/role-bindings (the `CreateIamV2RoleBinding` operationId).
	CreateIamV2RoleBinding(ctx context.Context, body CreateIamV2RoleBindingJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2RoleBinding Delete a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a role binding.
	//
	// Corresponds with DELETE /iam/v2/role-bindings/{id} (the `DeleteIamV2RoleBinding` operationId).
	DeleteIamV2RoleBinding(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2RoleBinding Read a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a role binding.
	//
	// Corresponds with GET /iam/v2/role-bindings/{id} (the `GetIamV2RoleBinding` operationId).
	GetIamV2RoleBinding(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2ServiceAccounts List of Service Accounts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all service accounts.
	//
	// Corresponds with GET /iam/v2/service-accounts (the `ListIamV2ServiceAccounts` operationId).
	ListIamV2ServiceAccounts(ctx context.Context, params *ListIamV2ServiceAccountsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2ServiceAccountWithBody Create a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a service account.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/service-accounts (the `CreateIamV2ServiceAccount` operationId).
	CreateIamV2ServiceAccountWithBody(ctx context.Context, params *CreateIamV2ServiceAccountParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2ServiceAccount Create a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a service account.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/service-accounts (the `CreateIamV2ServiceAccount` operationId).
	CreateIamV2ServiceAccount(ctx context.Context, params *CreateIamV2ServiceAccountParams, body CreateIamV2ServiceAccountJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2ServiceAccount Delete a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a service account.
	//
	// If successful, this request will also recursively delete all of the service account's associated resources,
	// including its cloud and cluster API keys.
	//
	// Corresponds with DELETE /iam/v2/service-accounts/{id} (the `DeleteIamV2ServiceAccount` operationId).
	DeleteIamV2ServiceAccount(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2ServiceAccount Read a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a service account.
	//
	// Corresponds with GET /iam/v2/service-accounts/{id} (the `GetIamV2ServiceAccount` operationId).
	GetIamV2ServiceAccount(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2ServiceAccountWithBody Update a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a service account.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/service-accounts/{id} (the `UpdateIamV2ServiceAccount` operationId).
	UpdateIamV2ServiceAccountWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2ServiceAccount Update a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a service account.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/service-accounts/{id} (the `UpdateIamV2ServiceAccount` operationId).
	UpdateIamV2ServiceAccount(ctx context.Context, id string, body UpdateIamV2ServiceAccountJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2SsoGroupMappings List of Group Mappings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all group mappings.
	//
	// Corresponds with GET /iam/v2/sso/group-mappings (the `ListIamV2SsoGroupMappings` operationId).
	ListIamV2SsoGroupMappings(ctx context.Context, params *ListIamV2SsoGroupMappingsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2SsoGroupMappingWithBody Create a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a group mapping.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /iam/v2/sso/group-mappings (the `CreateIamV2SsoGroupMapping` operationId).
	CreateIamV2SsoGroupMappingWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateIamV2SsoGroupMapping Create a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a group mapping.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /iam/v2/sso/group-mappings (the `CreateIamV2SsoGroupMapping` operationId).
	CreateIamV2SsoGroupMapping(ctx context.Context, body CreateIamV2SsoGroupMappingJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2SsoGroupMapping Delete a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a group mapping.
	//
	// Corresponds with DELETE /iam/v2/sso/group-mappings/{id} (the `DeleteIamV2SsoGroupMapping` operationId).
	DeleteIamV2SsoGroupMapping(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2SsoGroupMapping Read a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a group mapping.
	//
	// Corresponds with GET /iam/v2/sso/group-mappings/{id} (the `GetIamV2SsoGroupMapping` operationId).
	GetIamV2SsoGroupMapping(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2SsoGroupMappingWithBody Update a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a group mapping.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/sso/group-mappings/{id} (the `UpdateIamV2SsoGroupMapping` operationId).
	UpdateIamV2SsoGroupMappingWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2SsoGroupMapping Update a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a group mapping.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/sso/group-mappings/{id} (the `UpdateIamV2SsoGroupMapping` operationId).
	UpdateIamV2SsoGroupMapping(ctx context.Context, id string, body UpdateIamV2SsoGroupMappingJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListIamV2Users List of Users
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all users.
	//
	// Corresponds with GET /iam/v2/users (the `ListIamV2Users` operationId).
	ListIamV2Users(ctx context.Context, params *ListIamV2UsersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteIamV2User Delete a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a user.
	//
	// If successful, this request will also recursively delete all of the user's associated resources,
	// including its cloud and cluster API keys.
	//
	// Corresponds with DELETE /iam/v2/users/{id} (the `DeleteIamV2User` operationId).
	DeleteIamV2User(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetIamV2User Read a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a user.
	//
	// Corresponds with GET /iam/v2/users/{id} (the `GetIamV2User` operationId).
	GetIamV2User(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2UserWithBody Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/users/{id} (the `UpdateIamV2User` operationId).
	UpdateIamV2UserWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateIamV2User Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/users/{id} (the `UpdateIamV2User` operationId).
	UpdateIamV2User(ctx context.Context, id string, body UpdateIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateAuthTypeIamV2UserWithBody Update Auth Type of a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the auth type of a user
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /iam/v2/users/{id}/auth (the `UpdateAuthTypeIamV2User` operationId).
	UpdateAuthTypeIamV2UserWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateAuthTypeIamV2User Update Auth Type of a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the auth type of a user
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /iam/v2/users/{id}/auth (the `UpdateAuthTypeIamV2User` operationId).
	UpdateAuthTypeIamV2User(ctx context.Context, id string, body UpdateAuthTypeIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *Client) UpdateAuthTypeIamV2UserWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewUpdateAuthTypeIamV2UserRequestWithBody(c.Server, id, contentType, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func (c *Client) UpdateAuthTypeIamV2User(ctx context.Context, id string, body UpdateAuthTypeIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	req, err := NewUpdateAuthTypeIamV2UserRequest(c.Server, id, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := c.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return c.Client.Do(req)
}
func NewUpdateAuthTypeIamV2UserRequest(server string, id string, body UpdateAuthTypeIamV2UserJSONRequestBody) (*http.Request, error) {
	var bodyReader io.Reader
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	bodyReader = bytes.NewReader(buf)
	return NewUpdateAuthTypeIamV2UserRequestWithBody(server, id, "application/json", bodyReader)
}
func NewUpdateAuthTypeIamV2UserRequestWithBody(server string, id string, contentType string, body io.Reader) (*http.Request, error) {
	var err error

	var pathParam0 string

	pathParam0, err = runtime.StyleParamWithOptions("simple", false, "id", id, runtime.StyleParamOptions{ParamLocation: runtime.ParamLocationPath, Type: "string", Format: ""})
	if err != nil {
		return nil, err
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	operationPath := fmt.Sprintf("/iam/v2/users/%s/auth", pathParam0)
	if operationPath[0] == '/' {
		operationPath = "." + operationPath
	}

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPatch, queryURL.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", contentType)

	return req, nil
}
func (c *Client) applyEditors(ctx context.Context, req *http.Request, additionalEditors []RequestEditorFn) error {
	for _, r := range c.RequestEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	for _, r := range additionalEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

type ClientWithResponses struct {
	ClientInterface
}

func NewClientWithResponses(server string, opts ...ClientOption) (*ClientWithResponses, error) {
	client, err := NewClient(server, opts...)
	if err != nil {
		return nil, err
	}
	return &ClientWithResponses{client}, nil
}
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) error {
		newBaseURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}
		c.Server = newBaseURL.String()
		return nil
	}
}

type ClientWithResponsesInterface interface {

	// ListIamV2ApiKeysWithResponse List of API Keys
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all API keys.
	//
	// This can show all keys for a single owner (across resources - Kafka clusters), or all keys for a single
	// resource (across owners). If no `owner` or `resource` filters are specified, returns all API Keys in the
	// organization. You will only see the keys that are accessible to the account making the API request.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/api-keys (the `ListIamV2ApiKeys` operationId).
	ListIamV2ApiKeysWithResponse(ctx context.Context, params *ListIamV2ApiKeysParams, reqEditors ...RequestEditorFn) (*ListIamV2ApiKeysResponse, error)

	// CreateIamV2ApiKeyWithBodyWithResponse Create an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an API key.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/api-keys (the `CreateIamV2ApiKey` operationId).
	CreateIamV2ApiKeyWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2ApiKeyResponse, error)

	// CreateIamV2ApiKeyWithResponse Create an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an API key.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/api-keys (the `CreateIamV2ApiKey` operationId).
	CreateIamV2ApiKeyWithResponse(ctx context.Context, body CreateIamV2ApiKeyJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2ApiKeyResponse, error)

	// DeleteIamV2ApiKeyWithResponse Delete an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an API key.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/api-keys/{id} (the `DeleteIamV2ApiKey` operationId).
	DeleteIamV2ApiKeyWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2ApiKeyResponse, error)

	// GetIamV2ApiKeyWithResponse Read an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an API key.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/api-keys/{id} (the `GetIamV2ApiKey` operationId).
	GetIamV2ApiKeyWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2ApiKeyResponse, error)

	// UpdateIamV2ApiKeyWithBodyWithResponse Update an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an API key.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/api-keys/{id} (the `UpdateIamV2ApiKey` operationId).
	UpdateIamV2ApiKeyWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2ApiKeyResponse, error)

	// UpdateIamV2ApiKeyWithResponse Update an API Key
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an API key.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/api-keys/{id} (the `UpdateIamV2ApiKey` operationId).
	UpdateIamV2ApiKeyWithResponse(ctx context.Context, id string, body UpdateIamV2ApiKeyJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2ApiKeyResponse, error)

	// ListIamV2CertificateAuthoritiesWithResponse List of Certificate Authorities
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all certificate authorities.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/certificate-authorities (the `ListIamV2CertificateAuthorities` operationId).
	ListIamV2CertificateAuthoritiesWithResponse(ctx context.Context, params *ListIamV2CertificateAuthoritiesParams, reqEditors ...RequestEditorFn) (*ListIamV2CertificateAuthoritiesResponse, error)

	// CreateIamV2CertificateAuthorityWithBodyWithResponse Create a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate authority.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/certificate-authorities (the `CreateIamV2CertificateAuthority` operationId).
	CreateIamV2CertificateAuthorityWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2CertificateAuthorityResponse, error)

	// CreateIamV2CertificateAuthorityWithResponse Create a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate authority.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/certificate-authorities (the `CreateIamV2CertificateAuthority` operationId).
	CreateIamV2CertificateAuthorityWithResponse(ctx context.Context, body CreateIamV2CertificateAuthorityJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2CertificateAuthorityResponse, error)

	// ListIamV2CertificateIdentityPoolsWithResponse List of Certificate Identity Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all certificate identity pools.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `ListIamV2CertificateIdentityPools` operationId).
	ListIamV2CertificateIdentityPoolsWithResponse(ctx context.Context, certificateAuthorityId string, params *ListIamV2CertificateIdentityPoolsParams, reqEditors ...RequestEditorFn) (*ListIamV2CertificateIdentityPoolsResponse, error)

	// CreateIamV2CertificateIdentityPoolWithBodyWithResponse Create a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate identity pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `CreateIamV2CertificateIdentityPool` operationId).
	CreateIamV2CertificateIdentityPoolWithBodyWithResponse(ctx context.Context, certificateAuthorityId string, params *CreateIamV2CertificateIdentityPoolParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2CertificateIdentityPoolResponse, error)

	// CreateIamV2CertificateIdentityPoolWithResponse Create a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a certificate identity pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools (the `CreateIamV2CertificateIdentityPool` operationId).
	CreateIamV2CertificateIdentityPoolWithResponse(ctx context.Context, certificateAuthorityId string, params *CreateIamV2CertificateIdentityPoolParams, body CreateIamV2CertificateIdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2CertificateIdentityPoolResponse, error)

	// DeleteIamV2CertificateIdentityPoolWithResponse Delete a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a certificate identity pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `DeleteIamV2CertificateIdentityPool` operationId).
	DeleteIamV2CertificateIdentityPoolWithResponse(ctx context.Context, certificateAuthorityId string, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2CertificateIdentityPoolResponse, error)

	// GetIamV2CertificateIdentityPoolWithResponse Read a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a certificate identity pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `GetIamV2CertificateIdentityPool` operationId).
	GetIamV2CertificateIdentityPoolWithResponse(ctx context.Context, certificateAuthorityId string, id string, reqEditors ...RequestEditorFn) (*GetIamV2CertificateIdentityPoolResponse, error)

	// UpdateIamV2CertificateIdentityPoolWithBodyWithResponse Update a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate identity pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `UpdateIamV2CertificateIdentityPool` operationId).
	UpdateIamV2CertificateIdentityPoolWithBodyWithResponse(ctx context.Context, certificateAuthorityId string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2CertificateIdentityPoolResponse, error)

	// UpdateIamV2CertificateIdentityPoolWithResponse Update a Certificate Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate identity pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{certificate_authority_id}/identity-pools/{id} (the `UpdateIamV2CertificateIdentityPool` operationId).
	UpdateIamV2CertificateIdentityPoolWithResponse(ctx context.Context, certificateAuthorityId string, id string, body UpdateIamV2CertificateIdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2CertificateIdentityPoolResponse, error)

	// DeleteIamV2CertificateAuthorityWithResponse Delete a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a certificate authority.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/certificate-authorities/{id} (the `DeleteIamV2CertificateAuthority` operationId).
	DeleteIamV2CertificateAuthorityWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2CertificateAuthorityResponse, error)

	// GetIamV2CertificateAuthorityWithResponse Read a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a certificate authority.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/certificate-authorities/{id} (the `GetIamV2CertificateAuthority` operationId).
	GetIamV2CertificateAuthorityWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2CertificateAuthorityResponse, error)

	// UpdateIamV2CertificateAuthorityWithBodyWithResponse Update a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate authority.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{id} (the `UpdateIamV2CertificateAuthority` operationId).
	UpdateIamV2CertificateAuthorityWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2CertificateAuthorityResponse, error)

	// UpdateIamV2CertificateAuthorityWithResponse Update a Certificate Authority
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a certificate authority.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PUT /iam/v2/certificate-authorities/{id} (the `UpdateIamV2CertificateAuthority` operationId).
	UpdateIamV2CertificateAuthorityWithResponse(ctx context.Context, id string, body UpdateIamV2CertificateAuthorityJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2CertificateAuthorityResponse, error)

	// ListIamV2IdentityProvidersWithResponse List of Identity Providers
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all identity providers.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/identity-providers (the `ListIamV2IdentityProviders` operationId).
	ListIamV2IdentityProvidersWithResponse(ctx context.Context, params *ListIamV2IdentityProvidersParams, reqEditors ...RequestEditorFn) (*ListIamV2IdentityProvidersResponse, error)

	// CreateIamV2IdentityProviderWithBodyWithResponse Create an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity provider.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/identity-providers (the `CreateIamV2IdentityProvider` operationId).
	CreateIamV2IdentityProviderWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2IdentityProviderResponse, error)

	// CreateIamV2IdentityProviderWithResponse Create an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity provider.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/identity-providers (the `CreateIamV2IdentityProvider` operationId).
	CreateIamV2IdentityProviderWithResponse(ctx context.Context, body CreateIamV2IdentityProviderJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2IdentityProviderResponse, error)

	// DeleteIamV2IdentityProviderWithResponse Delete an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an identity provider.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/identity-providers/{id} (the `DeleteIamV2IdentityProvider` operationId).
	DeleteIamV2IdentityProviderWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2IdentityProviderResponse, error)

	// GetIamV2IdentityProviderWithResponse Read an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an identity provider.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/identity-providers/{id} (the `GetIamV2IdentityProvider` operationId).
	GetIamV2IdentityProviderWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2IdentityProviderResponse, error)

	// UpdateIamV2IdentityProviderWithBodyWithResponse Update an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity provider.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{id} (the `UpdateIamV2IdentityProvider` operationId).
	UpdateIamV2IdentityProviderWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2IdentityProviderResponse, error)

	// UpdateIamV2IdentityProviderWithResponse Update an Identity Provider
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity provider.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{id} (the `UpdateIamV2IdentityProvider` operationId).
	UpdateIamV2IdentityProviderWithResponse(ctx context.Context, id string, body UpdateIamV2IdentityProviderJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2IdentityProviderResponse, error)

	// ListIamV2IdentityPoolsWithResponse List of Identity Pools
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all identity pools.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/identity-providers/{provider_id}/identity-pools (the `ListIamV2IdentityPools` operationId).
	ListIamV2IdentityPoolsWithResponse(ctx context.Context, providerId string, params *ListIamV2IdentityPoolsParams, reqEditors ...RequestEditorFn) (*ListIamV2IdentityPoolsResponse, error)

	// CreateIamV2IdentityPoolWithBodyWithResponse Create an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/identity-providers/{provider_id}/identity-pools (the `CreateIamV2IdentityPool` operationId).
	CreateIamV2IdentityPoolWithBodyWithResponse(ctx context.Context, providerId string, params *CreateIamV2IdentityPoolParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2IdentityPoolResponse, error)

	// CreateIamV2IdentityPoolWithResponse Create an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an identity pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/identity-providers/{provider_id}/identity-pools (the `CreateIamV2IdentityPool` operationId).
	CreateIamV2IdentityPoolWithResponse(ctx context.Context, providerId string, params *CreateIamV2IdentityPoolParams, body CreateIamV2IdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2IdentityPoolResponse, error)

	// DeleteIamV2IdentityPoolWithResponse Delete an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an identity pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `DeleteIamV2IdentityPool` operationId).
	DeleteIamV2IdentityPoolWithResponse(ctx context.Context, providerId string, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2IdentityPoolResponse, error)

	// GetIamV2IdentityPoolWithResponse Read an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an identity pool.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `GetIamV2IdentityPool` operationId).
	GetIamV2IdentityPoolWithResponse(ctx context.Context, providerId string, id string, reqEditors ...RequestEditorFn) (*GetIamV2IdentityPoolResponse, error)

	// UpdateIamV2IdentityPoolWithBodyWithResponse Update an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity pool.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `UpdateIamV2IdentityPool` operationId).
	UpdateIamV2IdentityPoolWithBodyWithResponse(ctx context.Context, providerId string, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2IdentityPoolResponse, error)

	// UpdateIamV2IdentityPoolWithResponse Update an Identity Pool
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an identity pool.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/identity-pools/{id} (the `UpdateIamV2IdentityPool` operationId).
	UpdateIamV2IdentityPoolWithResponse(ctx context.Context, providerId string, id string, body UpdateIamV2IdentityPoolJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2IdentityPoolResponse, error)

	// RefreshIamV2JsonWebKeySetWithBodyWithResponse Refresh a provider's JWKS
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to refresh the provider's JWKS
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/jwks (the `RefreshIamV2JsonWebKeySet` operationId).
	RefreshIamV2JsonWebKeySetWithBodyWithResponse(ctx context.Context, providerId string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*RefreshIamV2JsonWebKeySetResponse, error)

	// RefreshIamV2JsonWebKeySetWithResponse Refresh a provider's JWKS
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to refresh the provider's JWKS
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/identity-providers/{provider_id}/jwks (the `RefreshIamV2JsonWebKeySet` operationId).
	RefreshIamV2JsonWebKeySetWithResponse(ctx context.Context, providerId string, body RefreshIamV2JsonWebKeySetJSONRequestBody, reqEditors ...RequestEditorFn) (*RefreshIamV2JsonWebKeySetResponse, error)

	// ListIamV2InvitationsWithResponse List of Invitations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all invitations.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/invitations (the `ListIamV2Invitations` operationId).
	ListIamV2InvitationsWithResponse(ctx context.Context, params *ListIamV2InvitationsParams, reqEditors ...RequestEditorFn) (*ListIamV2InvitationsResponse, error)

	// CreateIamV2InvitationWithBodyWithResponse Create an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an invitation.
	//
	// The newly invited user will not have any permissions. Give the user permission by assigning them to one or
	// more roles by creating
	// [role bindings](https://docs.confluent.io/cloud/current/api.html#tag/Role-Bindings-(iamv2))
	// for the created `user`.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/invitations (the `CreateIamV2Invitation` operationId).
	CreateIamV2InvitationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2InvitationResponse, error)

	// CreateIamV2InvitationWithResponse Create an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an invitation.
	//
	// The newly invited user will not have any permissions. Give the user permission by assigning them to one or
	// more roles by creating
	// [role bindings](https://docs.confluent.io/cloud/current/api.html#tag/Role-Bindings-(iamv2))
	// for the created `user`.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/invitations (the `CreateIamV2Invitation` operationId).
	CreateIamV2InvitationWithResponse(ctx context.Context, body CreateIamV2InvitationJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2InvitationResponse, error)

	// DeleteIamV2InvitationWithResponse Delete an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an invitation.
	//
	// Delete will deactivate the user if the user didn't accept the invitation yet.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/invitations/{id} (the `DeleteIamV2Invitation` operationId).
	DeleteIamV2InvitationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2InvitationResponse, error)

	// GetIamV2InvitationWithResponse Read an Invitation
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an invitation.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/invitations/{id} (the `GetIamV2Invitation` operationId).
	GetIamV2InvitationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2InvitationResponse, error)

	// GetIamV2IpFilterSummaryWithResponse Read an IP Filter Summary
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP filter summary.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/ip-filter-summary (the `GetIamV2IpFilterSummary` operationId).
	GetIamV2IpFilterSummaryWithResponse(ctx context.Context, params *GetIamV2IpFilterSummaryParams, reqEditors ...RequestEditorFn) (*GetIamV2IpFilterSummaryResponse, error)

	// ListIamV2IpFiltersWithResponse List of IP Filters
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all IP filters.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/ip-filters (the `ListIamV2IpFilters` operationId).
	ListIamV2IpFiltersWithResponse(ctx context.Context, params *ListIamV2IpFiltersParams, reqEditors ...RequestEditorFn) (*ListIamV2IpFiltersResponse, error)

	// CreateIamV2IpFilterWithBodyWithResponse Create an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP filter.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/ip-filters (the `CreateIamV2IpFilter` operationId).
	CreateIamV2IpFilterWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2IpFilterResponse, error)

	// CreateIamV2IpFilterWithResponse Create an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP filter.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/ip-filters (the `CreateIamV2IpFilter` operationId).
	CreateIamV2IpFilterWithResponse(ctx context.Context, body CreateIamV2IpFilterJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2IpFilterResponse, error)

	// DeleteIamV2IpFilterWithResponse Delete an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an IP filter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/ip-filters/{id} (the `DeleteIamV2IpFilter` operationId).
	DeleteIamV2IpFilterWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2IpFilterResponse, error)

	// GetIamV2IpFilterWithResponse Read an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP filter.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/ip-filters/{id} (the `GetIamV2IpFilter` operationId).
	GetIamV2IpFilterWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2IpFilterResponse, error)

	// UpdateIamV2IpFilterWithBodyWithResponse Update an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP filter.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/ip-filters/{id} (the `UpdateIamV2IpFilter` operationId).
	UpdateIamV2IpFilterWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2IpFilterResponse, error)

	// UpdateIamV2IpFilterWithResponse Update an IP Filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP filter.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/ip-filters/{id} (the `UpdateIamV2IpFilter` operationId).
	UpdateIamV2IpFilterWithResponse(ctx context.Context, id string, body UpdateIamV2IpFilterJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2IpFilterResponse, error)

	// ListIamV2IpGroupsWithResponse List of IP Groups
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all IP groups.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/ip-groups (the `ListIamV2IpGroups` operationId).
	ListIamV2IpGroupsWithResponse(ctx context.Context, params *ListIamV2IpGroupsParams, reqEditors ...RequestEditorFn) (*ListIamV2IpGroupsResponse, error)

	// CreateIamV2IpGroupWithBodyWithResponse Create an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP group.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/ip-groups (the `CreateIamV2IpGroup` operationId).
	CreateIamV2IpGroupWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2IpGroupResponse, error)

	// CreateIamV2IpGroupWithResponse Create an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an IP group.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/ip-groups (the `CreateIamV2IpGroup` operationId).
	CreateIamV2IpGroupWithResponse(ctx context.Context, body CreateIamV2IpGroupJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2IpGroupResponse, error)

	// DeleteIamV2IpGroupWithResponse Delete an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an IP group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/ip-groups/{id} (the `DeleteIamV2IpGroup` operationId).
	DeleteIamV2IpGroupWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2IpGroupResponse, error)

	// GetIamV2IpGroupWithResponse Read an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an IP group.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/ip-groups/{id} (the `GetIamV2IpGroup` operationId).
	GetIamV2IpGroupWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2IpGroupResponse, error)

	// UpdateIamV2IpGroupWithBodyWithResponse Update an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP group.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/ip-groups/{id} (the `UpdateIamV2IpGroup` operationId).
	UpdateIamV2IpGroupWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2IpGroupResponse, error)

	// UpdateIamV2IpGroupWithResponse Update an IP Group
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an IP group.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/ip-groups/{id} (the `UpdateIamV2IpGroup` operationId).
	UpdateIamV2IpGroupWithResponse(ctx context.Context, id string, body UpdateIamV2IpGroupJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2IpGroupResponse, error)

	// ListIamV2RoleBindingsWithResponse List of Role Bindings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all role bindings.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/role-bindings (the `ListIamV2RoleBindings` operationId).
	ListIamV2RoleBindingsWithResponse(ctx context.Context, params *ListIamV2RoleBindingsParams, reqEditors ...RequestEditorFn) (*ListIamV2RoleBindingsResponse, error)

	// CreateIamV2RoleBindingWithBodyWithResponse Create a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a role binding.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/role-bindings (the `CreateIamV2RoleBinding` operationId).
	CreateIamV2RoleBindingWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2RoleBindingResponse, error)

	// CreateIamV2RoleBindingWithResponse Create a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a role binding.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/role-bindings (the `CreateIamV2RoleBinding` operationId).
	CreateIamV2RoleBindingWithResponse(ctx context.Context, body CreateIamV2RoleBindingJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2RoleBindingResponse, error)

	// DeleteIamV2RoleBindingWithResponse Delete a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a role binding.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/role-bindings/{id} (the `DeleteIamV2RoleBinding` operationId).
	DeleteIamV2RoleBindingWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2RoleBindingResponse, error)

	// GetIamV2RoleBindingWithResponse Read a Role Binding
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a role binding.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/role-bindings/{id} (the `GetIamV2RoleBinding` operationId).
	GetIamV2RoleBindingWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2RoleBindingResponse, error)

	// ListIamV2ServiceAccountsWithResponse List of Service Accounts
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all service accounts.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/service-accounts (the `ListIamV2ServiceAccounts` operationId).
	ListIamV2ServiceAccountsWithResponse(ctx context.Context, params *ListIamV2ServiceAccountsParams, reqEditors ...RequestEditorFn) (*ListIamV2ServiceAccountsResponse, error)

	// CreateIamV2ServiceAccountWithBodyWithResponse Create a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a service account.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/service-accounts (the `CreateIamV2ServiceAccount` operationId).
	CreateIamV2ServiceAccountWithBodyWithResponse(ctx context.Context, params *CreateIamV2ServiceAccountParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2ServiceAccountResponse, error)

	// CreateIamV2ServiceAccountWithResponse Create a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a service account.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/service-accounts (the `CreateIamV2ServiceAccount` operationId).
	CreateIamV2ServiceAccountWithResponse(ctx context.Context, params *CreateIamV2ServiceAccountParams, body CreateIamV2ServiceAccountJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2ServiceAccountResponse, error)

	// DeleteIamV2ServiceAccountWithResponse Delete a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a service account.
	//
	// If successful, this request will also recursively delete all of the service account's associated resources,
	// including its cloud and cluster API keys.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/service-accounts/{id} (the `DeleteIamV2ServiceAccount` operationId).
	DeleteIamV2ServiceAccountWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2ServiceAccountResponse, error)

	// GetIamV2ServiceAccountWithResponse Read a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a service account.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/service-accounts/{id} (the `GetIamV2ServiceAccount` operationId).
	GetIamV2ServiceAccountWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2ServiceAccountResponse, error)

	// UpdateIamV2ServiceAccountWithBodyWithResponse Update a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a service account.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/service-accounts/{id} (the `UpdateIamV2ServiceAccount` operationId).
	UpdateIamV2ServiceAccountWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2ServiceAccountResponse, error)

	// UpdateIamV2ServiceAccountWithResponse Update a Service Account
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a service account.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/service-accounts/{id} (the `UpdateIamV2ServiceAccount` operationId).
	UpdateIamV2ServiceAccountWithResponse(ctx context.Context, id string, body UpdateIamV2ServiceAccountJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2ServiceAccountResponse, error)

	// ListIamV2SsoGroupMappingsWithResponse List of Group Mappings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all group mappings.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/sso/group-mappings (the `ListIamV2SsoGroupMappings` operationId).
	ListIamV2SsoGroupMappingsWithResponse(ctx context.Context, params *ListIamV2SsoGroupMappingsParams, reqEditors ...RequestEditorFn) (*ListIamV2SsoGroupMappingsResponse, error)

	// CreateIamV2SsoGroupMappingWithBodyWithResponse Create a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a group mapping.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/sso/group-mappings (the `CreateIamV2SsoGroupMapping` operationId).
	CreateIamV2SsoGroupMappingWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateIamV2SsoGroupMappingResponse, error)

	// CreateIamV2SsoGroupMappingWithResponse Create a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a group mapping.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /iam/v2/sso/group-mappings (the `CreateIamV2SsoGroupMapping` operationId).
	CreateIamV2SsoGroupMappingWithResponse(ctx context.Context, body CreateIamV2SsoGroupMappingJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateIamV2SsoGroupMappingResponse, error)

	// DeleteIamV2SsoGroupMappingWithResponse Delete a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a group mapping.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/sso/group-mappings/{id} (the `DeleteIamV2SsoGroupMapping` operationId).
	DeleteIamV2SsoGroupMappingWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2SsoGroupMappingResponse, error)

	// GetIamV2SsoGroupMappingWithResponse Read a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a group mapping.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/sso/group-mappings/{id} (the `GetIamV2SsoGroupMapping` operationId).
	GetIamV2SsoGroupMappingWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2SsoGroupMappingResponse, error)

	// UpdateIamV2SsoGroupMappingWithBodyWithResponse Update a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a group mapping.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/sso/group-mappings/{id} (the `UpdateIamV2SsoGroupMapping` operationId).
	UpdateIamV2SsoGroupMappingWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2SsoGroupMappingResponse, error)

	// UpdateIamV2SsoGroupMappingWithResponse Update a Group Mapping
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a group mapping.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/sso/group-mappings/{id} (the `UpdateIamV2SsoGroupMapping` operationId).
	UpdateIamV2SsoGroupMappingWithResponse(ctx context.Context, id string, body UpdateIamV2SsoGroupMappingJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2SsoGroupMappingResponse, error)

	// ListIamV2UsersWithResponse List of Users
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all users.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/users (the `ListIamV2Users` operationId).
	ListIamV2UsersWithResponse(ctx context.Context, params *ListIamV2UsersParams, reqEditors ...RequestEditorFn) (*ListIamV2UsersResponse, error)

	// DeleteIamV2UserWithResponse Delete a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a user.
	//
	// If successful, this request will also recursively delete all of the user's associated resources,
	// including its cloud and cluster API keys.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /iam/v2/users/{id} (the `DeleteIamV2User` operationId).
	DeleteIamV2UserWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteIamV2UserResponse, error)

	// GetIamV2UserWithResponse Read a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a user.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /iam/v2/users/{id} (the `GetIamV2User` operationId).
	GetIamV2UserWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetIamV2UserResponse, error)

	// UpdateIamV2UserWithBodyWithResponse Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/users/{id} (the `UpdateIamV2User` operationId).
	UpdateIamV2UserWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateIamV2UserResponse, error)

	// UpdateIamV2UserWithResponse Update a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a user.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/users/{id} (the `UpdateIamV2User` operationId).
	UpdateIamV2UserWithResponse(ctx context.Context, id string, body UpdateIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateIamV2UserResponse, error)

	// UpdateAuthTypeIamV2UserWithBodyWithResponse Update Auth Type of a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the auth type of a user
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/users/{id}/auth (the `UpdateAuthTypeIamV2User` operationId).
	UpdateAuthTypeIamV2UserWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateAuthTypeIamV2UserResponse, error)

	// UpdateAuthTypeIamV2UserWithResponse Update Auth Type of a User
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Update the auth type of a user
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /iam/v2/users/{id}/auth (the `UpdateAuthTypeIamV2User` operationId).
	UpdateAuthTypeIamV2UserWithResponse(ctx context.Context, id string, body UpdateAuthTypeIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateAuthTypeIamV2UserResponse, error)
}

func (r ListIamV2ApiKeysResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListIamV2ApiKeys200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Owner    interface{} `json:"owner,omitempty"`
			Resource interface{} `json:"resource,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListIamV2ApiKeys200JSONResponseBodyKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
} {
	return r.JSON200
}
func (r ListIamV2ApiKeysResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2ApiKeysResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2ApiKeysResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2ApiKeysResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2ApiKeysResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2ApiKeysResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2ApiKeysResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2ApiKeysResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2ApiKeyResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2ApiKey202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2ApiKey202JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
	Spec struct {
		Owner    interface{} `json:"owner,omitempty"`
		Resource interface{} `json:"resource,omitempty"`
	} `json:"spec"`
} {
	return r.JSON202
}
func (r CreateIamV2ApiKeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2ApiKeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2ApiKeyResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2ApiKeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2ApiKeyResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2ApiKeyResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2ApiKeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2ApiKeyResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2ApiKeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2ApiKeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2ApiKeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2ApiKeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2ApiKeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2ApiKeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2ApiKeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2ApiKeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2ApiKeyResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2ApiKeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2ApiKeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2ApiKeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2ApiKeyResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2ApiKey200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2ApiKey200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
	Spec struct {
		Owner    interface{} `json:"owner,omitempty"`
		Resource interface{} `json:"resource,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r GetIamV2ApiKeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2ApiKeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2ApiKeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2ApiKeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2ApiKeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2ApiKeyResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2ApiKeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2ApiKeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2ApiKeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2ApiKeyResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2ApiKey200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2ApiKey200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
	Spec struct {
		Owner    interface{} `json:"owner,omitempty"`
		Resource interface{} `json:"resource,omitempty"`
	} `json:"spec"`
} {
	return r.JSON200
}
func (r UpdateIamV2ApiKeyResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2ApiKeyResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2ApiKeyResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2ApiKeyResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2ApiKeyResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2ApiKeyResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2ApiKeyResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2ApiKeyResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2ApiKeyResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2ApiKeyResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2ApiKeyResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2ApiKeyResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2CertificateAuthoritiesResponse) GetJSON200() *IamV2CertificateAuthorityList {
	return r.JSON200
}
func (r ListIamV2CertificateAuthoritiesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2CertificateAuthoritiesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2CertificateAuthoritiesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2CertificateAuthoritiesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2CertificateAuthoritiesResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2CertificateAuthoritiesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2CertificateAuthoritiesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2CertificateAuthoritiesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2CertificateAuthority201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
	CertificateChainFilename *string `json:"certificate_chain_filename,omitempty"`

	// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
	// either local file uploaded (LOCAL) or from url of CRL (URL).
	CrlSource *string `json:"crl_source,omitempty"`

	// CrlUpdatedAt The timestamp for when CRL was last updated.
	CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description string `json:"description"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName string `json:"display_name"`

	// ExpirationDates The expiration dates of certificates in the chain.
	ExpirationDates *[]time.Time `json:"expiration_dates,omitempty"`

	// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
	// strings that act as unique identifiers for the certificates in the chain.
	Fingerprints *[]string `json:"fingerprints,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2CertificateAuthority201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate bool `json:"require_crl_on_client_certificate"`

	// SerialNumbers The serial numbers for each certificate in the certificate chain.
	SerialNumbers *[]string `json:"serial_numbers,omitempty"`

	// State The current state of the certificate authority.
	State *string `json:"state,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2CertificateAuthorityResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2CertificateAuthorityResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2CertificateAuthorityResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2CertificateAuthorityResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2CertificateAuthorityResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetJSON200() *IamV2CertificateIdentityPoolList {
	return r.JSON200
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2CertificateIdentityPoolsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2CertificateIdentityPoolsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2CertificateIdentityPoolsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2CertificateIdentityPoolsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2CertificateIdentityPool201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// ExternalIdentifier The certificate field that will be used to represent the
	// pool's external identifier for audit logging.
	ExternalIdentifier string `json:"external_identifier"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2CertificateIdentityPool201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the identity pool
	State *string `json:"state,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2CertificateIdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2CertificateIdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2CertificateIdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2CertificateIdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion DeleteIamV2CertificateIdentityPool200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// ExternalIdentifier The certificate field that will be used to represent the
	// pool's external identifier for audit logging.
	ExternalIdentifier string `json:"external_identifier"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     DeleteIamV2CertificateIdentityPool200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal string `json:"principal"`

	// State The current state of the identity pool
	State string `json:"state"`
} {
	return r.JSON200
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2CertificateIdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2CertificateIdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2CertificateIdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2CertificateIdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2CertificateIdentityPool200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// ExternalIdentifier The certificate field that will be used to represent the
	// pool's external identifier for audit logging.
	ExternalIdentifier string `json:"external_identifier"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2CertificateIdentityPool200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal string `json:"principal"`

	// State The current state of the identity pool
	State string `json:"state"`
} {
	return r.JSON200
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2CertificateIdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2CertificateIdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2CertificateIdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2CertificateIdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2CertificateIdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2CertificateIdentityPool200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// ExternalIdentifier The certificate field that will be used to represent the
	// pool's external identifier for audit logging.
	ExternalIdentifier string `json:"external_identifier"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) that specifies which identities can authenticate using your certificate identity pool (see [CEL filter for mTLS](https://docs.confluent.io/cloud/current/access-management/authenticate/mtls/cel-filters.html) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2CertificateIdentityPool200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal string `json:"principal"`

	// State The current state of the identity pool
	State string `json:"state"`
} {
	return r.JSON200
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2CertificateIdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2CertificateIdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2CertificateIdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2CertificateIdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion DeleteIamV2CertificateAuthority200JSONResponseBodyApiVersion `json:"api_version"`

	// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
	CertificateChainFilename string `json:"certificate_chain_filename"`

	// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
	// either local file uploaded (LOCAL) or from url of CRL (URL).
	CrlSource *string `json:"crl_source,omitempty"`

	// CrlUpdatedAt The timestamp for when CRL was last updated.
	CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description string `json:"description"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName string `json:"display_name"`

	// ExpirationDates The expiration dates of certificates in the chain.
	ExpirationDates []time.Time `json:"expiration_dates"`

	// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
	// strings that act as unique identifiers for the certificates in the chain.
	Fingerprints []string `json:"fingerprints"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     DeleteIamV2CertificateAuthority200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate bool `json:"require_crl_on_client_certificate"`

	// SerialNumbers The serial numbers for each certificate in the certificate chain.
	SerialNumbers []string `json:"serial_numbers"`

	// State The current state of the certificate authority.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2CertificateAuthorityResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2CertificateAuthorityResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2CertificateAuthorityResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2CertificateAuthorityResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2CertificateAuthorityResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2CertificateAuthority200JSONResponseBodyApiVersion `json:"api_version"`

	// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
	CertificateChainFilename string `json:"certificate_chain_filename"`

	// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
	// either local file uploaded (LOCAL) or from url of CRL (URL).
	CrlSource *string `json:"crl_source,omitempty"`

	// CrlUpdatedAt The timestamp for when CRL was last updated.
	CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description string `json:"description"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName string `json:"display_name"`

	// ExpirationDates The expiration dates of certificates in the chain.
	ExpirationDates []time.Time `json:"expiration_dates"`

	// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
	// strings that act as unique identifiers for the certificates in the chain.
	Fingerprints []string `json:"fingerprints"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2CertificateAuthority200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate bool `json:"require_crl_on_client_certificate"`

	// SerialNumbers The serial numbers for each certificate in the certificate chain.
	SerialNumbers []string `json:"serial_numbers"`

	// State The current state of the certificate authority.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2CertificateAuthorityResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2CertificateAuthorityResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2CertificateAuthorityResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2CertificateAuthorityResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2CertificateAuthorityResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2CertificateAuthority200JSONResponseBodyApiVersion `json:"api_version"`

	// CertificateChainFilename The file name of the uploaded pem file for this certificate authority.
	CertificateChainFilename string `json:"certificate_chain_filename"`

	// CrlSource The source specifies whether the Certificate Revocation List (CRL) is updated from
	// either local file uploaded (LOCAL) or from url of CRL (URL).
	CrlSource *string `json:"crl_source,omitempty"`

	// CrlUpdatedAt The timestamp for when CRL was last updated.
	CrlUpdatedAt *time.Time `json:"crl_updated_at,omitempty"`

	// CrlUrl The url from which to fetch the CRL for the certificate authority if crl_source is URL.
	CrlUrl *string `json:"crl_url,omitempty"`

	// Description A description of the certificate authority.
	Description string `json:"description"`

	// DisplayName The human-readable name of the certificate authority.
	DisplayName string `json:"display_name"`

	// ExpirationDates The expiration dates of certificates in the chain.
	ExpirationDates []time.Time `json:"expiration_dates"`

	// Fingerprints The fingerprints for each certificate in the certificate chain. These are SHA-1 encoded
	// strings that act as unique identifiers for the certificates in the chain.
	Fingerprints []string `json:"fingerprints"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2CertificateAuthority200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// RequireCrlOnClientCertificate Whether to require CRL validation on client certificates.
	// If `require_crl_on_client_certificate` is true, then a CRL must be configured. At time of mTLS auth,
	// if the client certificate is revoked in the CRL or the client issuer does not match the CRL issuer,
	// certificate verification will fail even if TLS handshake is successful (OpenSSL -crl_check default behavior).
	// If `require_crl_on_client_certificate` is false, this mTLS identity provider cannot configure a new CRL.
	RequireCrlOnClientCertificate bool `json:"require_crl_on_client_certificate"`

	// SerialNumbers The serial numbers for each certificate in the certificate chain.
	SerialNumbers []string `json:"serial_numbers"`

	// State The current state of the certificate authority.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2CertificateAuthorityResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2CertificateAuthorityResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2CertificateAuthorityResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2CertificateAuthorityResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2CertificateAuthorityResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2IdentityProvidersResponse) GetJSON200() *IamV2IdentityProviderList {
	return r.JSON200
}
func (r ListIamV2IdentityProvidersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2IdentityProvidersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2IdentityProvidersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2IdentityProvidersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2IdentityProvidersResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2IdentityProvidersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2IdentityProvidersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2IdentityProvidersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2IdentityProviderResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2IdentityProvider201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A description of the identity provider.
	Description string `json:"description"`

	// DisplayName The human-readable name of the OAuth identity provider.
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1). This appears
	// in audit log records. Note: if the client specifies mapping to one identity pool ID, the identity
	// claim configured with that pool will be used instead.
	// Note - The attribute is in an [Early Access lifecycle stage]
	// (https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	IdentityClaim *string `json:"identity_claim,omitempty"`

	// Issuer A publicly accessible URL uniquely identifying the OAuth
	// identity provider authorized to issue access tokens.
	Issuer string `json:"issuer"`

	// JwksUri A publicly accessible JSON Web Key Set (JWKS) URI for the OAuth
	// identity provider. JWKS provides a set of crypotgraphic keys
	// used to verify the authenticity and integrity of JSON Web
	// Tokens (JWTs) issued by the OAuth identity provider.
	JwksUri string `json:"jwks_uri"`

	// Keys The JWKS issued by the OAuth identity provider. Only `kid` (key ID)
	// and `alg` (algorithm) properties for each key set are included.
	Keys *[]IamV2JwksObject `json:"keys,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2IdentityProvider201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// State The current state of the identity provider.
	State *string `json:"state,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2IdentityProviderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2IdentityProviderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2IdentityProviderResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2IdentityProviderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2IdentityProviderResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2IdentityProviderResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2IdentityProviderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2IdentityProviderResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2IdentityProviderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2IdentityProviderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2IdentityProviderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2IdentityProviderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2IdentityProviderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2IdentityProviderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2IdentityProviderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2IdentityProviderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2IdentityProviderResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2IdentityProviderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2IdentityProviderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2IdentityProviderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2IdentityProviderResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2IdentityProvider200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of the identity provider.
	Description string `json:"description"`

	// DisplayName The human-readable name of the OAuth identity provider.
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1). This appears
	// in audit log records. Note: if the client specifies mapping to one identity pool ID, the identity
	// claim configured with that pool will be used instead.
	// Note - The attribute is in an [Early Access lifecycle stage]
	// (https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	IdentityClaim *string `json:"identity_claim,omitempty"`

	// Issuer A publicly accessible URL uniquely identifying the OAuth
	// identity provider authorized to issue access tokens.
	Issuer string `json:"issuer"`

	// JwksUri A publicly accessible JSON Web Key Set (JWKS) URI for the OAuth
	// identity provider. JWKS provides a set of crypotgraphic keys
	// used to verify the authenticity and integrity of JSON Web
	// Tokens (JWTs) issued by the OAuth identity provider.
	JwksUri string `json:"jwks_uri"`

	// Keys The JWKS issued by the OAuth identity provider. Only `kid` (key ID)
	// and `alg` (algorithm) properties for each key set are included.
	Keys *[]IamV2JwksObject `json:"keys,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2IdentityProvider200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// State The current state of the identity provider.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r GetIamV2IdentityProviderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2IdentityProviderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2IdentityProviderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2IdentityProviderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2IdentityProviderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2IdentityProviderResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2IdentityProviderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2IdentityProviderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2IdentityProviderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2IdentityProvider200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of the identity provider.
	Description string `json:"description"`

	// DisplayName The human-readable name of the OAuth identity provider.
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1). This appears
	// in audit log records. Note: if the client specifies mapping to one identity pool ID, the identity
	// claim configured with that pool will be used instead.
	// Note - The attribute is in an [Early Access lifecycle stage]
	// (https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	IdentityClaim *string `json:"identity_claim,omitempty"`

	// Issuer A publicly accessible URL uniquely identifying the OAuth
	// identity provider authorized to issue access tokens.
	Issuer string `json:"issuer"`

	// JwksUri A publicly accessible JSON Web Key Set (JWKS) URI for the OAuth
	// identity provider. JWKS provides a set of crypotgraphic keys
	// used to verify the authenticity and integrity of JSON Web
	// Tokens (JWTs) issued by the OAuth identity provider.
	JwksUri string `json:"jwks_uri"`

	// Keys The JWKS issued by the OAuth identity provider. Only `kid` (key ID)
	// and `alg` (algorithm) properties for each key set are included.
	Keys *[]IamV2JwksObject `json:"keys,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2IdentityProvider200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// State The current state of the identity provider.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2IdentityProviderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2IdentityProviderResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2IdentityProviderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2IdentityProviderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2IdentityProviderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2IdentityPoolsResponse) GetJSON200() *IamV2IdentityPoolList {
	return r.JSON200
}
func (r ListIamV2IdentityPoolsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2IdentityPoolsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2IdentityPoolsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2IdentityPoolsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2IdentityPoolsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2IdentityPoolsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2IdentityPoolsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2IdentityPoolsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2IdentityPoolResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2IdentityPool201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#supported-common-expression-language-cel-filters) that specifies which identities can authenticate using your identity pool (see [Set identity pool filters](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#set-identity-pool-filters) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// (see [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1) for more details).
	// This appears in the audit log records, showing, for example, that "identity Z used identity pool X to access
	// topic A".
	IdentityClaim string `json:"identity_claim"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2IdentityPool201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the identity pool
	State *string `json:"state,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2IdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2IdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2IdentityPoolResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2IdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2IdentityPoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2IdentityPoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2IdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2IdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2IdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2IdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2IdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2IdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2IdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2IdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2IdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2IdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2IdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2IdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2IdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2IdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2IdentityPoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2IdentityPool200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#supported-common-expression-language-cel-filters) that specifies which identities can authenticate using your identity pool (see [Set identity pool filters](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#set-identity-pool-filters) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// (see [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1) for more details).
	// This appears in the audit log records, showing, for example, that "identity Z used identity pool X to access
	// topic A".
	IdentityClaim string `json:"identity_claim"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2IdentityPool200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal string `json:"principal"`

	// State The current state of the identity pool
	State string `json:"state"`
} {
	return r.JSON200
}
func (r GetIamV2IdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2IdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2IdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2IdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2IdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2IdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2IdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2IdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2IdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2IdentityPool200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description of how this `IdentityPool` is used
	Description string `json:"description"`

	// DisplayName The name of the `IdentityPool`.
	DisplayName string `json:"display_name"`

	// Filter A filter expression in [Supported Common Expression Language (CEL)](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#supported-common-expression-language-cel-filters) that specifies which identities can authenticate using your identity pool (see [Set identity pool filters](https://docs.confluent.io/cloud/current/access-management/authenticate/oauth/identity-pools.html#set-identity-pool-filters) for more details).
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IdentityClaim The JSON Web Token (JWT) claim to extract the authenticating identity to Confluent resources from
	// (see [Registered Claim Names](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1) for more details).
	// This appears in the audit log records, showing, for example, that "identity Z used identity pool X to access
	// topic A".
	IdentityClaim string `json:"identity_claim"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2IdentityPool200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal Represents the federated identity associated with this pool.
	Principal string `json:"principal"`

	// State The current state of the identity pool
	State string `json:"state"`
} {
	return r.JSON200
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2IdentityPoolResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2IdentityPoolResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2IdentityPoolResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2IdentityPoolResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2IdentityPoolResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion RefreshIamV2JsonWebKeySet200JSONResponseBodyApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind RefreshIamV2JsonWebKeySet200JSONResponseBodyKind `json:"kind"`
	Spec map[string]interface{}                           `json:"spec"`

	// Status The status of the Jwks
	Status *IamV2JwksStatus `json:"status,omitempty"`
} {
	return r.JSON200
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r RefreshIamV2JsonWebKeySetResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r RefreshIamV2JsonWebKeySetResponse) GetBody() []byte {
	return r.Body
}
func (r RefreshIamV2JsonWebKeySetResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r RefreshIamV2JsonWebKeySetResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r RefreshIamV2JsonWebKeySetResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2InvitationsResponse) GetJSON200() *IamV2InvitationList {
	return r.JSON200
}
func (r ListIamV2InvitationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2InvitationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2InvitationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2InvitationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2InvitationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2InvitationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2InvitationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2InvitationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2InvitationResponse) GetJSON201() *struct {
	// AcceptedAt The timestamp that the invitation was accepted
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2Invitation201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// AuthType The user/invitee's authentication type. Note that only the [OrganizationAdmin role](https://docs.confluent.io/cloud/current/access-management/access-control/cloud-rbac.html#organizationadmin)
	// can invite AUTH_TYPE_LOCAL users to SSO organizations.
	// The user's auth_type is set as AUTH_TYPE_SSO by default if the organization has SSO enabled.
	// Otherwise, the user's auth_type is AUTH_TYPE_LOCAL by default.
	AuthType *string `json:"auth_type,omitempty"`

	// Creator The invitation creator
	Creator *GlobalObjectReference `json:"creator,omitempty"`

	// Email The user/invitee's email address
	Email openapi_types.Email `json:"email"`

	// ExpiresAt The timestamp that the invitation will expire
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2Invitation201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Status The status of invitations
	Status *string `json:"status,omitempty"`

	// User The user/invitee
	User *GlobalObjectReference `json:"user,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2InvitationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2InvitationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2InvitationResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2InvitationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2InvitationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2InvitationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2InvitationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2InvitationResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2InvitationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2InvitationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2InvitationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2InvitationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2InvitationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2InvitationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2InvitationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2InvitationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2InvitationResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2InvitationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2InvitationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2InvitationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2InvitationResponse) GetJSON200() *struct {
	// AcceptedAt The timestamp that the invitation was accepted
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2Invitation200JSONResponseBodyApiVersion `json:"api_version"`

	// AuthType The user/invitee's authentication type. Note that only the [OrganizationAdmin role](https://docs.confluent.io/cloud/current/access-management/access-control/cloud-rbac.html#organizationadmin)
	// can invite AUTH_TYPE_LOCAL users to SSO organizations.
	// The user's auth_type is set as AUTH_TYPE_SSO by default if the organization has SSO enabled.
	// Otherwise, the user's auth_type is AUTH_TYPE_LOCAL by default.
	AuthType *string `json:"auth_type,omitempty"`

	// Creator The invitation creator
	Creator *GlobalObjectReference `json:"creator,omitempty"`

	// Email The user/invitee's email address
	Email openapi_types.Email `json:"email"`

	// ExpiresAt The timestamp that the invitation will expire
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2Invitation200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Status The status of invitations
	Status *string `json:"status,omitempty"`

	// User The user/invitee
	User *GlobalObjectReference `json:"user,omitempty"`
} {
	return r.JSON200
}
func (r GetIamV2InvitationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2InvitationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2InvitationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2InvitationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2InvitationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2InvitationResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2InvitationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2InvitationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2InvitationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2IpFilterSummary200JSONResponseBodyApiVersion `json:"api_version"`

	// Categories Summary of the operation groups and IP filters created in those operation groups.
	Categories []struct {
		// Name Name of the category.
		Name *string `json:"name,omitempty"`

		// OperationGroups Operation groups part of this category.
		OperationGroups *[]struct {
			// Name Name of the operation group.
			Name *string `json:"name,omitempty"`

			// Status Open, limited, or no access.
			Status *string `json:"status,omitempty"`
		} `json:"operation_groups,omitempty"`

		// Status Open, limited, or mixed.
		Status *string `json:"status,omitempty"`
	} `json:"categories"`

	// Kind Kind defines the object this REST resource represents.
	Kind GetIamV2IpFilterSummary200JSONResponseBodyKind `json:"kind"`

	// Scope The scope associated with this object.
	Scope string `json:"scope"`
} {
	return r.JSON200
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2IpFilterSummaryResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2IpFilterSummaryResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2IpFilterSummaryResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2IpFilterSummaryResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2IpFilterSummaryResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2IpFiltersResponse) GetJSON200() *IamV2IpFilterList {
	return r.JSON200
}
func (r ListIamV2IpFiltersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2IpFiltersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2IpFiltersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2IpFiltersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2IpFiltersResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2IpFiltersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2IpFiltersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2IpFiltersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2IpFilterResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2IpFilter201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// FilterName A human readable name for an IP Filter. Can contain any unicode letter or number, the ASCII space character,
	// or any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	FilterName string `json:"filter_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IpGroups A list of IP Groups.
	IpGroups []GlobalObjectReference `json:"ip_groups"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2IpFilter201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// OperationGroups Scope of resources covered by this IP filter. Resource group must be set to 'multiple'
	// in order to use this property.During update operations, note that the operation
	// groups passed in will replace the list of existing operation groups
	// (passing in an empty list will remove all operation groups) from the filter
	// (in line with the behavior for ip_groups).
	OperationGroups *[]string `json:"operation_groups,omitempty"`

	// ResourceGroup Scope of resources covered by this IP filter. Available resource groups include "management" and "multiple".
	ResourceGroup string `json:"resource_group"`

	// ResourceScope A CRN that specifies the scope of the ip filter, specifically the organization
	// or environment. Without specifying this property, the ip filter
	// would apply to the whole organization.
	ResourceScope *string `json:"resource_scope,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2IpFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2IpFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2IpFilterResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2IpFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2IpFilterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2IpFilterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2IpFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2IpFilterResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2IpFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2IpFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2IpFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2IpFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2IpFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2IpFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2IpFilterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2IpFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2IpFilterResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2IpFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2IpFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2IpFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2IpFilterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2IpFilter200JSONResponseBodyApiVersion `json:"api_version"`

	// FilterName A human readable name for an IP Filter. Can contain any unicode letter or number, the ASCII space character,
	// or any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	FilterName string `json:"filter_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IpGroups A list of IP Groups.
	IpGroups []GlobalObjectReference `json:"ip_groups"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2IpFilter200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// OperationGroups Scope of resources covered by this IP filter. Resource group must be set to 'multiple'
	// in order to use this property.During update operations, note that the operation
	// groups passed in will replace the list of existing operation groups
	// (passing in an empty list will remove all operation groups) from the filter
	// (in line with the behavior for ip_groups).
	OperationGroups *[]string `json:"operation_groups,omitempty"`

	// ResourceGroup Scope of resources covered by this IP filter. Available resource groups include "management" and "multiple".
	ResourceGroup string `json:"resource_group"`

	// ResourceScope A CRN that specifies the scope of the ip filter, specifically the organization
	// or environment. Without specifying this property, the ip filter
	// would apply to the whole organization.
	ResourceScope *string `json:"resource_scope,omitempty"`
} {
	return r.JSON200
}
func (r GetIamV2IpFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2IpFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2IpFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2IpFilterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2IpFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2IpFilterResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2IpFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2IpFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2IpFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2IpFilterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2IpFilter200JSONResponseBodyApiVersion `json:"api_version"`

	// FilterName A human readable name for an IP Filter. Can contain any unicode letter or number, the ASCII space character,
	// or any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	FilterName string `json:"filter_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IpGroups A list of IP Groups.
	IpGroups []GlobalObjectReference `json:"ip_groups"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2IpFilter200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// OperationGroups Scope of resources covered by this IP filter. Resource group must be set to 'multiple'
	// in order to use this property.During update operations, note that the operation
	// groups passed in will replace the list of existing operation groups
	// (passing in an empty list will remove all operation groups) from the filter
	// (in line with the behavior for ip_groups).
	OperationGroups *[]string `json:"operation_groups,omitempty"`

	// ResourceGroup Scope of resources covered by this IP filter. Available resource groups include "management" and "multiple".
	ResourceGroup string `json:"resource_group"`

	// ResourceScope A CRN that specifies the scope of the ip filter, specifically the organization
	// or environment. Without specifying this property, the ip filter
	// would apply to the whole organization.
	ResourceScope *string `json:"resource_scope,omitempty"`
} {
	return r.JSON200
}
func (r UpdateIamV2IpFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2IpFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2IpFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2IpFilterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2IpFilterResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2IpFilterResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2IpFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2IpFilterResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2IpFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2IpFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2IpFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2IpGroupsResponse) GetJSON200() *IamV2IpGroupList {
	return r.JSON200
}
func (r ListIamV2IpGroupsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2IpGroupsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2IpGroupsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2IpGroupsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2IpGroupsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2IpGroupsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2IpGroupsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2IpGroupsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2IpGroupResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2IpGroup201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CidrBlocks A list of CIDRs.
	CidrBlocks []string `json:"cidr_blocks"`

	// GroupName A human readable name for an IP Group. Can contain any unicode letter or number, the ASCII space character, or
	// any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	GroupName string `json:"group_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2IpGroup201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2IpGroupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2IpGroupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2IpGroupResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2IpGroupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2IpGroupResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2IpGroupResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2IpGroupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2IpGroupResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2IpGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2IpGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2IpGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2IpGroupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2IpGroupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2IpGroupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2IpGroupResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2IpGroupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2IpGroupResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2IpGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2IpGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2IpGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2IpGroupResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2IpGroup200JSONResponseBodyApiVersion `json:"api_version"`

	// CidrBlocks A list of CIDRs.
	CidrBlocks []string `json:"cidr_blocks"`

	// GroupName A human readable name for an IP Group. Can contain any unicode letter or number, the ASCII space character, or
	// any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	GroupName string `json:"group_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2IpGroup200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r GetIamV2IpGroupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2IpGroupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2IpGroupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2IpGroupResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2IpGroupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2IpGroupResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2IpGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2IpGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2IpGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2IpGroupResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2IpGroup200JSONResponseBodyApiVersion `json:"api_version"`

	// CidrBlocks A list of CIDRs.
	CidrBlocks []string `json:"cidr_blocks"`

	// GroupName A human readable name for an IP Group. Can contain any unicode letter or number, the ASCII space character, or
	// any of the following special characters: `[`, `]`, `|`, `&`, `+`, `-`, `_`, `/`, `.`, `,`.
	GroupName string `json:"group_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2IpGroup200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r UpdateIamV2IpGroupResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2IpGroupResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2IpGroupResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2IpGroupResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2IpGroupResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2IpGroupResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2IpGroupResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2IpGroupResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2IpGroupResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2IpGroupResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2IpGroupResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2RoleBindingsResponse) GetJSON200() *IamV2RoleBindingList {
	return r.JSON200
}
func (r ListIamV2RoleBindingsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2RoleBindingsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2RoleBindingsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2RoleBindingsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2RoleBindingsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2RoleBindingsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2RoleBindingsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2RoleBindingsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2RoleBindingResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2RoleBinding201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CrnPattern A CRN that specifies the scope and resource patterns necessary for the role to bind
	CrnPattern string `json:"crn_pattern"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2RoleBinding201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The principal User to bind the role to
	Principal string `json:"principal"`

	// RoleName The name of the role to bind to the principal
	RoleName string `json:"role_name"`
} {
	return r.JSON201
}
func (r CreateIamV2RoleBindingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2RoleBindingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2RoleBindingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2RoleBindingResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2RoleBindingResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2RoleBindingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2RoleBindingResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2RoleBindingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2RoleBindingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2RoleBindingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2RoleBindingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion DeleteIamV2RoleBinding200JSONResponseBodyApiVersion `json:"api_version"`

	// CrnPattern A CRN that specifies the scope and resource patterns necessary for the role to bind
	CrnPattern string `json:"crn_pattern"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     DeleteIamV2RoleBinding200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The principal User to bind the role to
	Principal string `json:"principal"`

	// RoleName The name of the role to bind to the principal
	RoleName string `json:"role_name"`
} {
	return r.JSON200
}
func (r DeleteIamV2RoleBindingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2RoleBindingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2RoleBindingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2RoleBindingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2RoleBindingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2RoleBindingResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2RoleBindingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2RoleBindingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2RoleBindingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2RoleBindingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2RoleBinding200JSONResponseBodyApiVersion `json:"api_version"`

	// CrnPattern A CRN that specifies the scope and resource patterns necessary for the role to bind
	CrnPattern string `json:"crn_pattern"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2RoleBinding200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The principal User to bind the role to
	Principal string `json:"principal"`

	// RoleName The name of the role to bind to the principal
	RoleName string `json:"role_name"`
} {
	return r.JSON200
}
func (r GetIamV2RoleBindingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2RoleBindingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2RoleBindingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2RoleBindingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2RoleBindingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2RoleBindingResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2RoleBindingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2RoleBindingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2RoleBindingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2ServiceAccountsResponse) GetJSON200() *IamV2ServiceAccountList {
	return r.JSON200
}
func (r ListIamV2ServiceAccountsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2ServiceAccountsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2ServiceAccountsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2ServiceAccountsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2ServiceAccountsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2ServiceAccountsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2ServiceAccountsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2ServiceAccountsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2ServiceAccountResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2ServiceAccount201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A free-form description of the Service Account
	Description *string `json:"description,omitempty"`

	// DisplayName A human-readable name for the Service Account
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2ServiceAccount201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2ServiceAccountResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2ServiceAccountResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2ServiceAccountResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2ServiceAccountResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2ServiceAccountResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2ServiceAccountResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2ServiceAccountResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2ServiceAccountResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2ServiceAccountResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2ServiceAccountResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2ServiceAccountResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2ServiceAccountResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2ServiceAccountResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2ServiceAccountResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2ServiceAccountResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2ServiceAccountResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2ServiceAccountResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2ServiceAccountResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2ServiceAccountResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2ServiceAccountResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2ServiceAccountResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2ServiceAccount200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A free-form description of the Service Account
	Description *string `json:"description,omitempty"`

	// DisplayName A human-readable name for the Service Account
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2ServiceAccount200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r GetIamV2ServiceAccountResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2ServiceAccountResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2ServiceAccountResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2ServiceAccountResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2ServiceAccountResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2ServiceAccountResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2ServiceAccountResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2ServiceAccountResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2ServiceAccountResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2ServiceAccount200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A free-form description of the Service Account
	Description *string `json:"description,omitempty"`

	// DisplayName A human-readable name for the Service Account
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2ServiceAccount200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2ServiceAccountResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2ServiceAccountResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2ServiceAccountResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2ServiceAccountResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2ServiceAccountResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2SsoGroupMappingsResponse) GetJSON200() *IamV2SsoGroupMappingList {
	return r.JSON200
}
func (r ListIamV2SsoGroupMappingsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2SsoGroupMappingsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2SsoGroupMappingsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2SsoGroupMappingsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2SsoGroupMappingsResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2SsoGroupMappingsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2SsoGroupMappingsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2SsoGroupMappingsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateIamV2SsoGroupMapping201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A description explaining the purpose and use of the group mapping.
	Description string `json:"description"`

	// DisplayName The name of the group mapping.
	DisplayName string `json:"display_name"`

	// Filter A single group identifier or a condition based on [supported CEL operators](https://docs.confluent.io/cloud/current/access-management/authenticate/sso/group-mapping/overview.html#supported-cel-operators-for-group-mapping) that defines which groups are included.
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateIamV2SsoGroupMapping201JSONResponseBodyKind `json:"kind,omitempty"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The unique federated identity associated with this group mapping.
	Principal *string `json:"principal,omitempty"`

	// State The current state of the group mapping.
	State *string `json:"state,omitempty"`
} {
	return r.JSON201
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateIamV2SsoGroupMappingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateIamV2SsoGroupMappingResponse) GetBody() []byte {
	return r.Body
}
func (r CreateIamV2SsoGroupMappingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateIamV2SsoGroupMappingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateIamV2SsoGroupMappingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2SsoGroupMappingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2SsoGroupMappingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2SsoGroupMappingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2SsoGroupMappingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2SsoGroupMappingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2SsoGroupMappingResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2SsoGroupMappingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2SsoGroupMappingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2SsoGroupMappingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2SsoGroupMapping200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description explaining the purpose and use of the group mapping.
	Description string `json:"description"`

	// DisplayName The name of the group mapping.
	DisplayName string `json:"display_name"`

	// Filter A single group identifier or a condition based on [supported CEL operators](https://docs.confluent.io/cloud/current/access-management/authenticate/sso/group-mapping/overview.html#supported-cel-operators-for-group-mapping) that defines which groups are included.
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2SsoGroupMapping200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The unique federated identity associated with this group mapping.
	Principal string `json:"principal"`

	// State The current state of the group mapping.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2SsoGroupMappingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2SsoGroupMappingResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2SsoGroupMappingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2SsoGroupMappingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2SsoGroupMappingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2SsoGroupMapping200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A description explaining the purpose and use of the group mapping.
	Description string `json:"description"`

	// DisplayName The name of the group mapping.
	DisplayName string `json:"display_name"`

	// Filter A single group identifier or a condition based on [supported CEL operators](https://docs.confluent.io/cloud/current/access-management/authenticate/sso/group-mapping/overview.html#supported-cel-operators-for-group-mapping) that defines which groups are included.
	Filter string `json:"filter"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2SsoGroupMapping200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`

	// Principal The unique federated identity associated with this group mapping.
	Principal string `json:"principal"`

	// State The current state of the group mapping.
	State string `json:"state"`
} {
	return r.JSON200
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2SsoGroupMappingResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2SsoGroupMappingResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2SsoGroupMappingResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2SsoGroupMappingResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2SsoGroupMappingResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListIamV2UsersResponse) GetJSON200() *IamV2UserList {
	return r.JSON200
}
func (r ListIamV2UsersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListIamV2UsersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListIamV2UsersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListIamV2UsersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListIamV2UsersResponse) GetBody() []byte {
	return r.Body
}
func (r ListIamV2UsersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListIamV2UsersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListIamV2UsersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteIamV2UserResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteIamV2UserResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteIamV2UserResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteIamV2UserResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteIamV2UserResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteIamV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteIamV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteIamV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteIamV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetIamV2UserResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetIamV2User200JSONResponseBodyApiVersion `json:"api_version"`

	// AuthType The user's authentication method
	AuthType *string `json:"auth_type,omitempty"`

	// Email The user's email address
	Email openapi_types.Email `json:"email"`

	// FullName The user's full name
	FullName *string `json:"full_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetIamV2User200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r GetIamV2UserResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetIamV2UserResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetIamV2UserResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetIamV2UserResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetIamV2UserResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetIamV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r GetIamV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetIamV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetIamV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateIamV2UserResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateIamV2User200JSONResponseBodyApiVersion `json:"api_version"`

	// AuthType The user's authentication method
	AuthType *string `json:"auth_type,omitempty"`

	// Email The user's email address
	Email openapi_types.Email `json:"email"`

	// FullName The user's full name
	FullName *string `json:"full_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateIamV2User200JSONResponseBodyKind `json:"kind"`
	Metadata *struct {
		// CreatedAt The date and time at which this object was created. It is represented in RFC3339 format and is in UTC.
		CreatedAt *time.Time `json:"created_at,omitempty"`

		// DeletedAt The date and time at which this object was (or will be) deleted. It is represented in RFC3339 format and is in UTC.
		DeletedAt    *time.Time  `json:"deleted_at,omitempty"`
		ResourceName interface{} `json:"resource_name,omitempty"`
		Self         interface{} `json:"self"`

		// UpdatedAt The date and time at which this object was last updated. It is represented in RFC3339 format and is in UTC.
		UpdatedAt *time.Time `json:"updated_at,omitempty"`
	} `json:"metadata,omitempty"`
} {
	return r.JSON200
}
func (r UpdateIamV2UserResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateIamV2UserResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateIamV2UserResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateIamV2UserResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateIamV2UserResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateIamV2UserResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateIamV2UserResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateIamV2UserResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateIamV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateIamV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateIamV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateIamV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}

type UpdateAuthTypeIamV2UserResponse400Headers struct {
	XRequestId *string
}
type UpdateAuthTypeIamV2UserResponse401Headers struct {
	WWWAuthenticate *string
	XRequestId      *string
}
type UpdateAuthTypeIamV2UserResponse403Headers struct {
	XRequestId *string
}
type UpdateAuthTypeIamV2UserResponse404Headers struct {
	XRequestId *string
}
type UpdateAuthTypeIamV2UserResponse429Headers struct {
	RetryAfter          *int
	XRateLimitLimit     *int
	XRateLimitRemaining *int
	XRateLimitReset     *int
	XRequestId          *string
}
type UpdateAuthTypeIamV2UserResponse500Headers struct {
	XRequestId *string
}
type UpdateAuthTypeIamV2UserResponse struct {
	Body         []byte
	HTTPResponse *http.Response
	// JSON400 the response for an HTTP 400 `application/json` response
	JSON400 *BadRequestError
	// JSON401 the response for an HTTP 401 `application/json` response
	JSON401 *UnauthenticatedError
	// JSON403 the response for an HTTP 403 `application/json` response
	JSON403 *UnauthorizedError
	// JSON404 the response for an HTTP 404 `application/json` response
	JSON404 *NotFoundError
	// JSON500 the response for an HTTP 500 `application/json` response
	JSON500 *DefaultSystemError
	// Headers400 the parsed response headers for an HTTP 400 response
	Headers400 *UpdateAuthTypeIamV2UserResponse400Headers
	// Headers401 the parsed response headers for an HTTP 401 response
	Headers401 *UpdateAuthTypeIamV2UserResponse401Headers
	// Headers403 the parsed response headers for an HTTP 403 response
	Headers403 *UpdateAuthTypeIamV2UserResponse403Headers
	// Headers404 the parsed response headers for an HTTP 404 response
	Headers404 *UpdateAuthTypeIamV2UserResponse404Headers
	// Headers429 the parsed response headers for an HTTP 429 response
	Headers429 *UpdateAuthTypeIamV2UserResponse429Headers
	// Headers500 the parsed response headers for an HTTP 500 response
	Headers500 *UpdateAuthTypeIamV2UserResponse500Headers
}

func (r UpdateAuthTypeIamV2UserResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateAuthTypeIamV2UserResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateAuthTypeIamV2UserResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateAuthTypeIamV2UserResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateAuthTypeIamV2UserResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateAuthTypeIamV2UserResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateAuthTypeIamV2UserResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateAuthTypeIamV2UserResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateAuthTypeIamV2UserResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (c *ClientWithResponses) UpdateAuthTypeIamV2UserWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateAuthTypeIamV2UserResponse, error) {
	rsp, err := c.UpdateAuthTypeIamV2UserWithBody(ctx, id, contentType, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseUpdateAuthTypeIamV2UserResponse(rsp)
}
func (c *ClientWithResponses) UpdateAuthTypeIamV2UserWithResponse(ctx context.Context, id string, body UpdateAuthTypeIamV2UserJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateAuthTypeIamV2UserResponse, error) {
	rsp, err := c.UpdateAuthTypeIamV2User(ctx, id, body, reqEditors...)
	if err != nil {
		return nil, err
	}
	return ParseUpdateAuthTypeIamV2UserResponse(rsp)
}
func ParseUpdateAuthTypeIamV2UserResponse(rsp *http.Response) (*UpdateAuthTypeIamV2UserResponse, error) {
	bodyBytes, err := io.ReadAll(rsp.Body)
	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &UpdateAuthTypeIamV2UserResponse{
		Body:         bodyBytes,
		HTTPResponse: rsp,
	}

	switch {
	case rsp.StatusCode == 204:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 400:
		var dest BadRequestError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON400 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 401:
		var dest UnauthenticatedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON401 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 403:
		var dest UnauthorizedError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON403 = &dest

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 404:
		var dest NotFoundError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON404 = &dest

	case rsp.StatusCode == 429:
		break // No content-type

	case strings.Contains(rsp.Header.Get("Content-Type"), "json") && rsp.StatusCode == 500:
		var dest DefaultSystemError
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, err
		}
		response.JSON500 = &dest

	}

	switch {
	case rsp.StatusCode == 400:
		var headers UpdateAuthTypeIamV2UserResponse400Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers400 = &headers
	case rsp.StatusCode == 401:
		var headers UpdateAuthTypeIamV2UserResponse401Headers
		if values := rsp.Header.Values("WWW-Authenticate"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "WWW-Authenticate", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.WWWAuthenticate = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers401 = &headers
	case rsp.StatusCode == 403:
		var headers UpdateAuthTypeIamV2UserResponse403Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers403 = &headers
	case rsp.StatusCode == 404:
		var headers UpdateAuthTypeIamV2UserResponse404Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers404 = &headers
	case rsp.StatusCode == 429:
		var headers UpdateAuthTypeIamV2UserResponse429Headers
		if values := rsp.Header.Values("Retry-After"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "Retry-After", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.RetryAfter = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Limit"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Limit", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitLimit = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Remaining"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Remaining", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitRemaining = &value
		}
		if values := rsp.Header.Values("X-RateLimit-Reset"); len(values) > 0 {
			var value int
			if err := runtime.BindStyledParameterWithOptions("simple", "X-RateLimit-Reset", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "integer", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRateLimitReset = &value
		}
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers429 = &headers
	case rsp.StatusCode == 500:
		var headers UpdateAuthTypeIamV2UserResponse500Headers
		if values := rsp.Header.Values("X-Request-Id"); len(values) > 0 {
			var value string
			if err := runtime.BindStyledParameterWithOptions("simple", "X-Request-Id", values[0], &value, runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationHeader, Explode: false, Required: false, Type: "string", Format: ""}); err != nil {
				return nil, err
			}
			headers.XRequestId = &value
		}
		response.Headers500 = &headers
	}

	return response, nil
}
