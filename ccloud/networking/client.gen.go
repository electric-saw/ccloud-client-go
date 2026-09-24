package networking

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oapi-codegen/runtime"
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
	NetworkingV1AccessPointApiVersionNetworkingv1 NetworkingV1AccessPointApiVersion = "networking/v1"
)

func (e NetworkingV1AccessPointApiVersion) Valid() bool {
	switch e {
	case NetworkingV1AccessPointApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1AccessPointKindAccessPoint NetworkingV1AccessPointKind = "AccessPoint"
)

func (e NetworkingV1AccessPointKind) Valid() bool {
	switch e {
	case NetworkingV1AccessPointKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1AccessPointListApiVersionNetworkingv1 NetworkingV1AccessPointListApiVersion = "networking/v1"
)

func (e NetworkingV1AccessPointListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1AccessPointListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1AccessPointListDataApiVersionNetworkingv1 NetworkingV1AccessPointListDataApiVersion = "networking/v1"
)

func (e NetworkingV1AccessPointListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1AccessPointListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1AccessPointListDataKindAccessPoint NetworkingV1AccessPointListDataKind = "AccessPoint"
)

func (e NetworkingV1AccessPointListDataKind) Valid() bool {
	switch e {
	case NetworkingV1AccessPointListDataKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1AccessPointListKindAccessPointList NetworkingV1AccessPointListKind = "AccessPointList"
)

func (e NetworkingV1AccessPointListKind) Valid() bool {
	switch e {
	case NetworkingV1AccessPointListKindAccessPointList:
		return true
	default:
		return false
	}
}

const (
	AwsEgressPrivateLinkEndpoint NetworkingV1AwsEgressPrivateLinkEndpointKind = "AwsEgressPrivateLinkEndpoint"
)

func (e NetworkingV1AwsEgressPrivateLinkEndpointKind) Valid() bool {
	switch e {
	case AwsEgressPrivateLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	AwsEgressPrivateLinkEndpointStatus NetworkingV1AwsEgressPrivateLinkEndpointStatusKind = "AwsEgressPrivateLinkEndpointStatus"
)

func (e NetworkingV1AwsEgressPrivateLinkEndpointStatusKind) Valid() bool {
	switch e {
	case AwsEgressPrivateLinkEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	AwsEgressPrivateLinkGatewaySpec NetworkingV1AwsEgressPrivateLinkGatewaySpecKind = "AwsEgressPrivateLinkGatewaySpec"
)

func (e NetworkingV1AwsEgressPrivateLinkGatewaySpecKind) Valid() bool {
	switch e {
	case AwsEgressPrivateLinkGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AwsEgressPrivateLinkGatewayStatus NetworkingV1AwsEgressPrivateLinkGatewayStatusKind = "AwsEgressPrivateLinkGatewayStatus"
)

func (e NetworkingV1AwsEgressPrivateLinkGatewayStatusKind) Valid() bool {
	switch e {
	case AwsEgressPrivateLinkGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	AwsIngressPrivateLinkEndpoint NetworkingV1AwsIngressPrivateLinkEndpointKind = "AwsIngressPrivateLinkEndpoint"
)

func (e NetworkingV1AwsIngressPrivateLinkEndpointKind) Valid() bool {
	switch e {
	case AwsIngressPrivateLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	AwsIngressPrivateLinkEndpointStatus NetworkingV1AwsIngressPrivateLinkEndpointStatusKind = "AwsIngressPrivateLinkEndpointStatus"
)

func (e NetworkingV1AwsIngressPrivateLinkEndpointStatusKind) Valid() bool {
	switch e {
	case AwsIngressPrivateLinkEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	AwsIngressPrivateLinkGatewaySpec NetworkingV1AwsIngressPrivateLinkGatewaySpecKind = "AwsIngressPrivateLinkGatewaySpec"
)

func (e NetworkingV1AwsIngressPrivateLinkGatewaySpecKind) Valid() bool {
	switch e {
	case AwsIngressPrivateLinkGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AwsIngressPrivateLinkGatewayStatus NetworkingV1AwsIngressPrivateLinkGatewayStatusKind = "AwsIngressPrivateLinkGatewayStatus"
)

func (e NetworkingV1AwsIngressPrivateLinkGatewayStatusKind) Valid() bool {
	switch e {
	case AwsIngressPrivateLinkGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	AwsNetwork NetworkingV1AwsNetworkKind = "AwsNetwork"
)

func (e NetworkingV1AwsNetworkKind) Valid() bool {
	switch e {
	case AwsNetwork:
		return true
	default:
		return false
	}
}

const (
	AwsPeering NetworkingV1AwsPeeringKind = "AwsPeering"
)

func (e NetworkingV1AwsPeeringKind) Valid() bool {
	switch e {
	case AwsPeering:
		return true
	default:
		return false
	}
}

const (
	AwsPeeringGatewaySpec NetworkingV1AwsPeeringGatewaySpecKind = "AwsPeeringGatewaySpec"
)

func (e NetworkingV1AwsPeeringGatewaySpecKind) Valid() bool {
	switch e {
	case AwsPeeringGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateLinkAccess NetworkingV1AwsPrivateLinkAccessKind = "AwsPrivateLinkAccess"
)

func (e NetworkingV1AwsPrivateLinkAccessKind) Valid() bool {
	switch e {
	case AwsPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateLinkAttachmentConnection NetworkingV1AwsPrivateLinkAttachmentConnectionKind = "AwsPrivateLinkAttachmentConnection"
)

func (e NetworkingV1AwsPrivateLinkAttachmentConnectionKind) Valid() bool {
	switch e {
	case AwsPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateLinkAttachmentConnectionStatus NetworkingV1AwsPrivateLinkAttachmentConnectionStatusKind = "AwsPrivateLinkAttachmentConnectionStatus"
)

func (e NetworkingV1AwsPrivateLinkAttachmentConnectionStatusKind) Valid() bool {
	switch e {
	case AwsPrivateLinkAttachmentConnectionStatus:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateLinkAttachmentStatus NetworkingV1AwsPrivateLinkAttachmentStatusKind = "AwsPrivateLinkAttachmentStatus"
)

func (e NetworkingV1AwsPrivateLinkAttachmentStatusKind) Valid() bool {
	switch e {
	case AwsPrivateLinkAttachmentStatus:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateNetworkInterface NetworkingV1AwsPrivateNetworkInterfaceKind = "AwsPrivateNetworkInterface"
)

func (e NetworkingV1AwsPrivateNetworkInterfaceKind) Valid() bool {
	switch e {
	case AwsPrivateNetworkInterface:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateNetworkInterfaceGatewaySpec NetworkingV1AwsPrivateNetworkInterfaceGatewaySpecKind = "AwsPrivateNetworkInterfaceGatewaySpec"
)

func (e NetworkingV1AwsPrivateNetworkInterfaceGatewaySpecKind) Valid() bool {
	switch e {
	case AwsPrivateNetworkInterfaceGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AwsPrivateNetworkInterfaceGatewayStatus NetworkingV1AwsPrivateNetworkInterfaceGatewayStatusKind = "AwsPrivateNetworkInterfaceGatewayStatus"
)

func (e NetworkingV1AwsPrivateNetworkInterfaceGatewayStatusKind) Valid() bool {
	switch e {
	case AwsPrivateNetworkInterfaceGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	AwsTransitGatewayAttachment NetworkingV1AwsTransitGatewayAttachmentKind = "AwsTransitGatewayAttachment"
)

func (e NetworkingV1AwsTransitGatewayAttachmentKind) Valid() bool {
	switch e {
	case AwsTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	AwsTransitGatewayAttachmentStatus NetworkingV1AwsTransitGatewayAttachmentStatusKind = "AwsTransitGatewayAttachmentStatus"
)

func (e NetworkingV1AwsTransitGatewayAttachmentStatusKind) Valid() bool {
	switch e {
	case AwsTransitGatewayAttachmentStatus:
		return true
	default:
		return false
	}
}

const (
	AzureEgressPrivateLinkEndpoint NetworkingV1AzureEgressPrivateLinkEndpointKind = "AzureEgressPrivateLinkEndpoint"
)

func (e NetworkingV1AzureEgressPrivateLinkEndpointKind) Valid() bool {
	switch e {
	case AzureEgressPrivateLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	AzureEgressPrivateLinkEndpointStatus NetworkingV1AzureEgressPrivateLinkEndpointStatusKind = "AzureEgressPrivateLinkEndpointStatus"
)

func (e NetworkingV1AzureEgressPrivateLinkEndpointStatusKind) Valid() bool {
	switch e {
	case AzureEgressPrivateLinkEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	AzureEgressPrivateLinkGatewaySpec NetworkingV1AzureEgressPrivateLinkGatewaySpecKind = "AzureEgressPrivateLinkGatewaySpec"
)

func (e NetworkingV1AzureEgressPrivateLinkGatewaySpecKind) Valid() bool {
	switch e {
	case AzureEgressPrivateLinkGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AzureEgressPrivateLinkGatewayStatus NetworkingV1AzureEgressPrivateLinkGatewayStatusKind = "AzureEgressPrivateLinkGatewayStatus"
)

func (e NetworkingV1AzureEgressPrivateLinkGatewayStatusKind) Valid() bool {
	switch e {
	case AzureEgressPrivateLinkGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	AzureIngressPrivateLinkEndpoint NetworkingV1AzureIngressPrivateLinkEndpointKind = "AzureIngressPrivateLinkEndpoint"
)

func (e NetworkingV1AzureIngressPrivateLinkEndpointKind) Valid() bool {
	switch e {
	case AzureIngressPrivateLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	AzureIngressPrivateLinkEndpointStatus NetworkingV1AzureIngressPrivateLinkEndpointStatusKind = "AzureIngressPrivateLinkEndpointStatus"
)

func (e NetworkingV1AzureIngressPrivateLinkEndpointStatusKind) Valid() bool {
	switch e {
	case AzureIngressPrivateLinkEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	AzureIngressPrivateLinkGatewaySpec NetworkingV1AzureIngressPrivateLinkGatewaySpecKind = "AzureIngressPrivateLinkGatewaySpec"
)

func (e NetworkingV1AzureIngressPrivateLinkGatewaySpecKind) Valid() bool {
	switch e {
	case AzureIngressPrivateLinkGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AzureIngressPrivateLinkGatewayStatus NetworkingV1AzureIngressPrivateLinkGatewayStatusKind = "AzureIngressPrivateLinkGatewayStatus"
)

func (e NetworkingV1AzureIngressPrivateLinkGatewayStatusKind) Valid() bool {
	switch e {
	case AzureIngressPrivateLinkGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	AzureNetwork NetworkingV1AzureNetworkKind = "AzureNetwork"
)

func (e NetworkingV1AzureNetworkKind) Valid() bool {
	switch e {
	case AzureNetwork:
		return true
	default:
		return false
	}
}

const (
	AzurePeering NetworkingV1AzurePeeringKind = "AzurePeering"
)

func (e NetworkingV1AzurePeeringKind) Valid() bool {
	switch e {
	case AzurePeering:
		return true
	default:
		return false
	}
}

const (
	AzurePeeringGatewaySpec NetworkingV1AzurePeeringGatewaySpecKind = "AzurePeeringGatewaySpec"
)

func (e NetworkingV1AzurePeeringGatewaySpecKind) Valid() bool {
	switch e {
	case AzurePeeringGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	AzurePrivateLinkAccess NetworkingV1AzurePrivateLinkAccessKind = "AzurePrivateLinkAccess"
)

func (e NetworkingV1AzurePrivateLinkAccessKind) Valid() bool {
	switch e {
	case AzurePrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	AzurePrivateLinkAttachmentConnection NetworkingV1AzurePrivateLinkAttachmentConnectionKind = "AzurePrivateLinkAttachmentConnection"
)

func (e NetworkingV1AzurePrivateLinkAttachmentConnectionKind) Valid() bool {
	switch e {
	case AzurePrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	AzurePrivateLinkAttachmentConnectionStatus NetworkingV1AzurePrivateLinkAttachmentConnectionStatusKind = "AzurePrivateLinkAttachmentConnectionStatus"
)

func (e NetworkingV1AzurePrivateLinkAttachmentConnectionStatusKind) Valid() bool {
	switch e {
	case AzurePrivateLinkAttachmentConnectionStatus:
		return true
	default:
		return false
	}
}

const (
	AzurePrivateLinkAttachmentStatus NetworkingV1AzurePrivateLinkAttachmentStatusKind = "AzurePrivateLinkAttachmentStatus"
)

func (e NetworkingV1AzurePrivateLinkAttachmentStatusKind) Valid() bool {
	switch e {
	case AzurePrivateLinkAttachmentStatus:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderApiVersionNetworkingv1 NetworkingV1DnsForwarderApiVersion = "networking/v1"
)

func (e NetworkingV1DnsForwarderApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderKindDnsForwarder NetworkingV1DnsForwarderKind = "DnsForwarder"
)

func (e NetworkingV1DnsForwarderKind) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderListApiVersionNetworkingv1 NetworkingV1DnsForwarderListApiVersion = "networking/v1"
)

func (e NetworkingV1DnsForwarderListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderListDataApiVersionNetworkingv1 NetworkingV1DnsForwarderListDataApiVersion = "networking/v1"
)

func (e NetworkingV1DnsForwarderListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderListDataKindDnsForwarder NetworkingV1DnsForwarderListDataKind = "DnsForwarder"
)

func (e NetworkingV1DnsForwarderListDataKind) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderListDataKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsForwarderListKindDnsForwarderList NetworkingV1DnsForwarderListKind = "DnsForwarderList"
)

func (e NetworkingV1DnsForwarderListKind) Valid() bool {
	switch e {
	case NetworkingV1DnsForwarderListKindDnsForwarderList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordApiVersionNetworkingv1 NetworkingV1DnsRecordApiVersion = "networking/v1"
)

func (e NetworkingV1DnsRecordApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordKindDnsRecord NetworkingV1DnsRecordKind = "DnsRecord"
)

func (e NetworkingV1DnsRecordKind) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordListApiVersionNetworkingv1 NetworkingV1DnsRecordListApiVersion = "networking/v1"
)

func (e NetworkingV1DnsRecordListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordListDataApiVersionNetworkingv1 NetworkingV1DnsRecordListDataApiVersion = "networking/v1"
)

func (e NetworkingV1DnsRecordListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordListDataKindDnsRecord NetworkingV1DnsRecordListDataKind = "DnsRecord"
)

func (e NetworkingV1DnsRecordListDataKind) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordListDataKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1DnsRecordListKindDnsRecordList NetworkingV1DnsRecordListKind = "DnsRecordList"
)

func (e NetworkingV1DnsRecordListKind) Valid() bool {
	switch e {
	case NetworkingV1DnsRecordListKindDnsRecordList:
		return true
	default:
		return false
	}
}

const (
	ForwardViaIp NetworkingV1ForwardViaIpKind = "ForwardViaIp"
)

func (e NetworkingV1ForwardViaIpKind) Valid() bool {
	switch e {
	case ForwardViaIp:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayApiVersionNetworkingv1 NetworkingV1GatewayApiVersion = "networking/v1"
)

func (e NetworkingV1GatewayApiVersion) Valid() bool {
	switch e {
	case NetworkingV1GatewayApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayKindGateway NetworkingV1GatewayKind = "Gateway"
)

func (e NetworkingV1GatewayKind) Valid() bool {
	switch e {
	case NetworkingV1GatewayKindGateway:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayListApiVersionNetworkingv1 NetworkingV1GatewayListApiVersion = "networking/v1"
)

func (e NetworkingV1GatewayListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1GatewayListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayListDataApiVersionNetworkingv1 NetworkingV1GatewayListDataApiVersion = "networking/v1"
)

func (e NetworkingV1GatewayListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1GatewayListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayListDataKindGateway NetworkingV1GatewayListDataKind = "Gateway"
)

func (e NetworkingV1GatewayListDataKind) Valid() bool {
	switch e {
	case NetworkingV1GatewayListDataKindGateway:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1GatewayListKindGatewayList NetworkingV1GatewayListKind = "GatewayList"
)

func (e NetworkingV1GatewayListKind) Valid() bool {
	switch e {
	case NetworkingV1GatewayListKindGatewayList:
		return true
	default:
		return false
	}
}

const (
	GcpEgressPrivateServiceConnectEndpoint NetworkingV1GcpEgressPrivateServiceConnectEndpointKind = "GcpEgressPrivateServiceConnectEndpoint"
)

func (e NetworkingV1GcpEgressPrivateServiceConnectEndpointKind) Valid() bool {
	switch e {
	case GcpEgressPrivateServiceConnectEndpoint:
		return true
	default:
		return false
	}
}

const (
	GcpEgressPrivateServiceConnectEndpointStatus NetworkingV1GcpEgressPrivateServiceConnectEndpointStatusKind = "GcpEgressPrivateServiceConnectEndpointStatus"
)

func (e NetworkingV1GcpEgressPrivateServiceConnectEndpointStatusKind) Valid() bool {
	switch e {
	case GcpEgressPrivateServiceConnectEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	GcpEgressPrivateServiceConnectGatewaySpec NetworkingV1GcpEgressPrivateServiceConnectGatewaySpecKind = "GcpEgressPrivateServiceConnectGatewaySpec"
)

func (e NetworkingV1GcpEgressPrivateServiceConnectGatewaySpecKind) Valid() bool {
	switch e {
	case GcpEgressPrivateServiceConnectGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	GcpEgressPrivateServiceConnectGatewayStatus NetworkingV1GcpEgressPrivateServiceConnectGatewayStatusKind = "GcpEgressPrivateServiceConnectGatewayStatus"
)

func (e NetworkingV1GcpEgressPrivateServiceConnectGatewayStatusKind) Valid() bool {
	switch e {
	case GcpEgressPrivateServiceConnectGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	GcpIngressPrivateServiceConnectEndpoint NetworkingV1GcpIngressPrivateServiceConnectEndpointKind = "GcpIngressPrivateServiceConnectEndpoint"
)

func (e NetworkingV1GcpIngressPrivateServiceConnectEndpointKind) Valid() bool {
	switch e {
	case GcpIngressPrivateServiceConnectEndpoint:
		return true
	default:
		return false
	}
}

const (
	GcpIngressPrivateServiceConnectEndpointStatus NetworkingV1GcpIngressPrivateServiceConnectEndpointStatusKind = "GcpIngressPrivateServiceConnectEndpointStatus"
)

func (e NetworkingV1GcpIngressPrivateServiceConnectEndpointStatusKind) Valid() bool {
	switch e {
	case GcpIngressPrivateServiceConnectEndpointStatus:
		return true
	default:
		return false
	}
}

const (
	GcpIngressPrivateServiceConnectGatewaySpec NetworkingV1GcpIngressPrivateServiceConnectGatewaySpecKind = "GcpIngressPrivateServiceConnectGatewaySpec"
)

func (e NetworkingV1GcpIngressPrivateServiceConnectGatewaySpecKind) Valid() bool {
	switch e {
	case GcpIngressPrivateServiceConnectGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	GcpIngressPrivateServiceConnectGatewayStatus NetworkingV1GcpIngressPrivateServiceConnectGatewayStatusKind = "GcpIngressPrivateServiceConnectGatewayStatus"
)

func (e NetworkingV1GcpIngressPrivateServiceConnectGatewayStatusKind) Valid() bool {
	switch e {
	case GcpIngressPrivateServiceConnectGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	GcpNetwork NetworkingV1GcpNetworkKind = "GcpNetwork"
)

func (e NetworkingV1GcpNetworkKind) Valid() bool {
	switch e {
	case GcpNetwork:
		return true
	default:
		return false
	}
}

const (
	GcpPeering NetworkingV1GcpPeeringKind = "GcpPeering"
)

func (e NetworkingV1GcpPeeringKind) Valid() bool {
	switch e {
	case GcpPeering:
		return true
	default:
		return false
	}
}

const (
	GcpPeeringGatewaySpec NetworkingV1GcpPeeringGatewaySpecKind = "GcpPeeringGatewaySpec"
)

func (e NetworkingV1GcpPeeringGatewaySpecKind) Valid() bool {
	switch e {
	case GcpPeeringGatewaySpec:
		return true
	default:
		return false
	}
}

const (
	GcpPeeringGatewayStatus NetworkingV1GcpPeeringGatewayStatusKind = "GcpPeeringGatewayStatus"
)

func (e NetworkingV1GcpPeeringGatewayStatusKind) Valid() bool {
	switch e {
	case GcpPeeringGatewayStatus:
		return true
	default:
		return false
	}
}

const (
	GcpPrivateLinkAttachmentConnection NetworkingV1GcpPrivateLinkAttachmentConnectionKind = "GcpPrivateLinkAttachmentConnection"
)

func (e NetworkingV1GcpPrivateLinkAttachmentConnectionKind) Valid() bool {
	switch e {
	case GcpPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	GcpPrivateLinkAttachmentConnectionStatus NetworkingV1GcpPrivateLinkAttachmentConnectionStatusKind = "GcpPrivateLinkAttachmentConnectionStatus"
)

func (e NetworkingV1GcpPrivateLinkAttachmentConnectionStatusKind) Valid() bool {
	switch e {
	case GcpPrivateLinkAttachmentConnectionStatus:
		return true
	default:
		return false
	}
}

const (
	GcpPrivateLinkAttachmentStatus NetworkingV1GcpPrivateLinkAttachmentStatusKind = "GcpPrivateLinkAttachmentStatus"
)

func (e NetworkingV1GcpPrivateLinkAttachmentStatusKind) Valid() bool {
	switch e {
	case GcpPrivateLinkAttachmentStatus:
		return true
	default:
		return false
	}
}

const (
	GcpPrivateServiceConnectAccess NetworkingV1GcpPrivateServiceConnectAccessKind = "GcpPrivateServiceConnectAccess"
)

func (e NetworkingV1GcpPrivateServiceConnectAccessKind) Valid() bool {
	switch e {
	case GcpPrivateServiceConnectAccess:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1IpAddressApiVersionNetworkingv1 NetworkingV1IpAddressApiVersion = "networking/v1"
)

func (e NetworkingV1IpAddressApiVersion) Valid() bool {
	switch e {
	case NetworkingV1IpAddressApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1IpAddressKindIpAddress NetworkingV1IpAddressKind = "IpAddress"
)

func (e NetworkingV1IpAddressKind) Valid() bool {
	switch e {
	case NetworkingV1IpAddressKindIpAddress:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1IpAddressListApiVersionNetworkingv1 NetworkingV1IpAddressListApiVersion = "networking/v1"
)

func (e NetworkingV1IpAddressListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1IpAddressListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1IpAddressListDataApiVersionNetworkingv1 NetworkingV1IpAddressListDataApiVersion = "networking/v1"
)

func (e NetworkingV1IpAddressListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1IpAddressListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1IpAddressListDataKindIpAddress NetworkingV1IpAddressListDataKind = "IpAddress"
)

func (e NetworkingV1IpAddressListDataKind) Valid() bool {
	switch e {
	case NetworkingV1IpAddressListDataKindIpAddress:
		return true
	default:
		return false
	}
}

const (
	IpAddressList NetworkingV1IpAddressListKind = "IpAddressList"
)

func (e NetworkingV1IpAddressListKind) Valid() bool {
	switch e {
	case IpAddressList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkApiVersionNetworkingv1 NetworkingV1NetworkApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkKindNetwork NetworkingV1NetworkKind = "Network"
)

func (e NetworkingV1NetworkKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkKindNetwork:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointApiVersionNetworkingv1 NetworkingV1NetworkLinkEndpointApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkEndpointApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointKindNetworkLinkEndpoint NetworkingV1NetworkLinkEndpointKind = "NetworkLinkEndpoint"
)

func (e NetworkingV1NetworkLinkEndpointKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointListApiVersionNetworkingv1 NetworkingV1NetworkLinkEndpointListApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkEndpointListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointListDataApiVersionNetworkingv1 NetworkingV1NetworkLinkEndpointListDataApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkEndpointListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointListDataKindNetworkLinkEndpoint NetworkingV1NetworkLinkEndpointListDataKind = "NetworkLinkEndpoint"
)

func (e NetworkingV1NetworkLinkEndpointListDataKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointListDataKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkEndpointListKindNetworkLinkEndpointList NetworkingV1NetworkLinkEndpointListKind = "NetworkLinkEndpointList"
)

func (e NetworkingV1NetworkLinkEndpointListKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkEndpointListKindNetworkLinkEndpointList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceKindNetworkLinkService NetworkingV1NetworkLinkServiceKind = "NetworkLinkService"
)

func (e NetworkingV1NetworkLinkServiceKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceAssociationApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceAssociationApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationKindNetworkLinkServiceAssociation NetworkingV1NetworkLinkServiceAssociationKind = "NetworkLinkServiceAssociation"
)

func (e NetworkingV1NetworkLinkServiceAssociationKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationKindNetworkLinkServiceAssociation:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationListApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceAssociationListApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceAssociationListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationListDataApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceAssociationListDataApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceAssociationListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationListDataKindNetworkLinkServiceAssociation NetworkingV1NetworkLinkServiceAssociationListDataKind = "NetworkLinkServiceAssociation"
)

func (e NetworkingV1NetworkLinkServiceAssociationListDataKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationListDataKindNetworkLinkServiceAssociation:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceAssociationListKindNetworkLinkServiceAssociationList NetworkingV1NetworkLinkServiceAssociationListKind = "NetworkLinkServiceAssociationList"
)

func (e NetworkingV1NetworkLinkServiceAssociationListKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceAssociationListKindNetworkLinkServiceAssociationList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceListApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceListApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceListDataApiVersionNetworkingv1 NetworkingV1NetworkLinkServiceListDataApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkLinkServiceListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceListDataKindNetworkLinkService NetworkingV1NetworkLinkServiceListDataKind = "NetworkLinkService"
)

func (e NetworkingV1NetworkLinkServiceListDataKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceListDataKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkLinkServiceListKindNetworkLinkServiceList NetworkingV1NetworkLinkServiceListKind = "NetworkLinkServiceList"
)

func (e NetworkingV1NetworkLinkServiceListKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkLinkServiceListKindNetworkLinkServiceList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkListApiVersionNetworkingv1 NetworkingV1NetworkListApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkListDataApiVersionNetworkingv1 NetworkingV1NetworkListDataApiVersion = "networking/v1"
)

func (e NetworkingV1NetworkListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1NetworkListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkListDataKindNetwork NetworkingV1NetworkListDataKind = "Network"
)

func (e NetworkingV1NetworkListDataKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkListDataKindNetwork:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1NetworkListKindNetworkList NetworkingV1NetworkListKind = "NetworkList"
)

func (e NetworkingV1NetworkListKind) Valid() bool {
	switch e {
	case NetworkingV1NetworkListKindNetworkList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringApiVersionNetworkingv1 NetworkingV1PeeringApiVersion = "networking/v1"
)

func (e NetworkingV1PeeringApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PeeringApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringKindPeering NetworkingV1PeeringKind = "Peering"
)

func (e NetworkingV1PeeringKind) Valid() bool {
	switch e {
	case NetworkingV1PeeringKindPeering:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringListApiVersionNetworkingv1 NetworkingV1PeeringListApiVersion = "networking/v1"
)

func (e NetworkingV1PeeringListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PeeringListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringListDataApiVersionNetworkingv1 NetworkingV1PeeringListDataApiVersion = "networking/v1"
)

func (e NetworkingV1PeeringListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PeeringListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringListDataKindPeering NetworkingV1PeeringListDataKind = "Peering"
)

func (e NetworkingV1PeeringListDataKind) Valid() bool {
	switch e {
	case NetworkingV1PeeringListDataKindPeering:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PeeringListKindPeeringList NetworkingV1PeeringListKind = "PeeringList"
)

func (e NetworkingV1PeeringListKind) Valid() bool {
	switch e {
	case NetworkingV1PeeringListKindPeeringList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessApiVersionNetworkingv1 NetworkingV1PrivateLinkAccessApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAccessApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessKindPrivateLinkAccess NetworkingV1PrivateLinkAccessKind = "PrivateLinkAccess"
)

func (e NetworkingV1PrivateLinkAccessKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessListApiVersionNetworkingv1 NetworkingV1PrivateLinkAccessListApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAccessListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessListDataApiVersionNetworkingv1 NetworkingV1PrivateLinkAccessListDataApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAccessListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessListDataKindPrivateLinkAccess NetworkingV1PrivateLinkAccessListDataKind = "PrivateLinkAccess"
)

func (e NetworkingV1PrivateLinkAccessListDataKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessListDataKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAccessListKindPrivateLinkAccessList NetworkingV1PrivateLinkAccessListKind = "PrivateLinkAccessList"
)

func (e NetworkingV1PrivateLinkAccessListKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAccessListKindPrivateLinkAccessList:
		return true
	default:
		return false
	}
}

const (
	PrivateLinkAccessPoint NetworkingV1PrivateLinkAccessPointKind = "PrivateLinkAccessPoint"
)

func (e NetworkingV1PrivateLinkAccessPointKind) Valid() bool {
	switch e {
	case PrivateLinkAccessPoint:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentKindPrivateLinkAttachment NetworkingV1PrivateLinkAttachmentKind = "PrivateLinkAttachment"
)

func (e NetworkingV1PrivateLinkAttachmentKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentConnectionApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionKindPrivateLinkAttachmentConnection NetworkingV1PrivateLinkAttachmentConnectionKind = "PrivateLinkAttachmentConnection"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionListApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentConnectionListApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionListDataKindPrivateLinkAttachmentConnection NetworkingV1PrivateLinkAttachmentConnectionListDataKind = "PrivateLinkAttachmentConnection"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionListDataKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionListDataKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentConnectionListKindPrivateLinkAttachmentConnectionList NetworkingV1PrivateLinkAttachmentConnectionListKind = "PrivateLinkAttachmentConnectionList"
)

func (e NetworkingV1PrivateLinkAttachmentConnectionListKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentConnectionListKindPrivateLinkAttachmentConnectionList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentListApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentListApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentListDataApiVersionNetworkingv1 NetworkingV1PrivateLinkAttachmentListDataApiVersion = "networking/v1"
)

func (e NetworkingV1PrivateLinkAttachmentListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentListDataKindPrivateLinkAttachment NetworkingV1PrivateLinkAttachmentListDataKind = "PrivateLinkAttachment"
)

func (e NetworkingV1PrivateLinkAttachmentListDataKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentListDataKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1PrivateLinkAttachmentListKindPrivateLinkAttachmentList NetworkingV1PrivateLinkAttachmentListKind = "PrivateLinkAttachmentList"
)

func (e NetworkingV1PrivateLinkAttachmentListKind) Valid() bool {
	switch e {
	case NetworkingV1PrivateLinkAttachmentListKindPrivateLinkAttachmentList:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentApiVersionNetworkingv1 NetworkingV1TransitGatewayAttachmentApiVersion = "networking/v1"
)

func (e NetworkingV1TransitGatewayAttachmentApiVersion) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentKindTransitGatewayAttachment NetworkingV1TransitGatewayAttachmentKind = "TransitGatewayAttachment"
)

func (e NetworkingV1TransitGatewayAttachmentKind) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentListApiVersionNetworkingv1 NetworkingV1TransitGatewayAttachmentListApiVersion = "networking/v1"
)

func (e NetworkingV1TransitGatewayAttachmentListApiVersion) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentListApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentListDataApiVersionNetworkingv1 NetworkingV1TransitGatewayAttachmentListDataApiVersion = "networking/v1"
)

func (e NetworkingV1TransitGatewayAttachmentListDataApiVersion) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentListDataApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentListDataKindTransitGatewayAttachment NetworkingV1TransitGatewayAttachmentListDataKind = "TransitGatewayAttachment"
)

func (e NetworkingV1TransitGatewayAttachmentListDataKind) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentListDataKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	NetworkingV1TransitGatewayAttachmentListKindTransitGatewayAttachmentList NetworkingV1TransitGatewayAttachmentListKind = "TransitGatewayAttachmentList"
)

func (e NetworkingV1TransitGatewayAttachmentListKind) Valid() bool {
	switch e {
	case NetworkingV1TransitGatewayAttachmentListKindTransitGatewayAttachmentList:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1AccessPoints200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1AccessPoints200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1AccessPoints200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1AccessPoints200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1AccessPoints200JSONResponseBodyKindAccessPointList ListNetworkingV1AccessPoints200JSONResponseBodyKind = "AccessPointList"
)

func (e ListNetworkingV1AccessPoints200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1AccessPoints200JSONResponseBodyKindAccessPointList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1AccessPointJSONBodyApiVersionNetworkingv1 CreateNetworkingV1AccessPointJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1AccessPointJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1AccessPointJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1AccessPointJSONBodyKindAccessPoint CreateNetworkingV1AccessPointJSONBodyKind = "AccessPoint"
)

func (e CreateNetworkingV1AccessPointJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1AccessPointJSONBodyKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1AccessPoint202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1AccessPoint202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1AccessPoint202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1AccessPoint202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1AccessPoint202JSONResponseBodyKindAccessPoint CreateNetworkingV1AccessPoint202JSONResponseBodyKind = "AccessPoint"
)

func (e CreateNetworkingV1AccessPoint202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1AccessPoint202JSONResponseBodyKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1AccessPoint200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1AccessPoint200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1AccessPoint200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1AccessPoint200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1AccessPoint200JSONResponseBodyKindAccessPoint GetNetworkingV1AccessPoint200JSONResponseBodyKind = "AccessPoint"
)

func (e GetNetworkingV1AccessPoint200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1AccessPoint200JSONResponseBodyKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1AccessPointJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1AccessPointJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1AccessPointJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1AccessPointJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1AccessPointJSONBodyKindAccessPoint UpdateNetworkingV1AccessPointJSONBodyKind = "AccessPoint"
)

func (e UpdateNetworkingV1AccessPointJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1AccessPointJSONBodyKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1AccessPoint200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1AccessPoint200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1AccessPoint200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1AccessPoint200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1AccessPoint200JSONResponseBodyKindAccessPoint UpdateNetworkingV1AccessPoint200JSONResponseBodyKind = "AccessPoint"
)

func (e UpdateNetworkingV1AccessPoint200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1AccessPoint200JSONResponseBodyKindAccessPoint:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1DnsForwarders200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1DnsForwarders200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1DnsForwarders200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1DnsForwarders200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1DnsForwarders200JSONResponseBodyKindDnsForwarderList ListNetworkingV1DnsForwarders200JSONResponseBodyKind = "DnsForwarderList"
)

func (e ListNetworkingV1DnsForwarders200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1DnsForwarders200JSONResponseBodyKindDnsForwarderList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsForwarderJSONBodyApiVersionNetworkingv1 CreateNetworkingV1DnsForwarderJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1DnsForwarderJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsForwarderJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsForwarderJSONBodyKindDnsForwarder CreateNetworkingV1DnsForwarderJSONBodyKind = "DnsForwarder"
)

func (e CreateNetworkingV1DnsForwarderJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsForwarderJSONBodyKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsForwarder202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1DnsForwarder202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1DnsForwarder202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsForwarder202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsForwarder202JSONResponseBodyKindDnsForwarder CreateNetworkingV1DnsForwarder202JSONResponseBodyKind = "DnsForwarder"
)

func (e CreateNetworkingV1DnsForwarder202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsForwarder202JSONResponseBodyKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1DnsForwarder200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1DnsForwarder200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1DnsForwarder200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1DnsForwarder200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1DnsForwarder200JSONResponseBodyKindDnsForwarder GetNetworkingV1DnsForwarder200JSONResponseBodyKind = "DnsForwarder"
)

func (e GetNetworkingV1DnsForwarder200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1DnsForwarder200JSONResponseBodyKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsForwarderJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1DnsForwarderJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1DnsForwarderJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsForwarderJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsForwarderJSONBodyKindDnsForwarder UpdateNetworkingV1DnsForwarderJSONBodyKind = "DnsForwarder"
)

func (e UpdateNetworkingV1DnsForwarderJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsForwarderJSONBodyKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsForwarder200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1DnsForwarder200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1DnsForwarder200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsForwarder200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsForwarder200JSONResponseBodyKindDnsForwarder UpdateNetworkingV1DnsForwarder200JSONResponseBodyKind = "DnsForwarder"
)

func (e UpdateNetworkingV1DnsForwarder200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsForwarder200JSONResponseBodyKindDnsForwarder:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1DnsRecords200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1DnsRecords200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1DnsRecords200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1DnsRecords200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1DnsRecords200JSONResponseBodyKindDnsRecordList ListNetworkingV1DnsRecords200JSONResponseBodyKind = "DnsRecordList"
)

func (e ListNetworkingV1DnsRecords200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1DnsRecords200JSONResponseBodyKindDnsRecordList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsRecordJSONBodyApiVersionNetworkingv1 CreateNetworkingV1DnsRecordJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1DnsRecordJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsRecordJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsRecordJSONBodyKindDnsRecord CreateNetworkingV1DnsRecordJSONBodyKind = "DnsRecord"
)

func (e CreateNetworkingV1DnsRecordJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsRecordJSONBodyKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsRecord202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1DnsRecord202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1DnsRecord202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsRecord202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1DnsRecord202JSONResponseBodyKindDnsRecord CreateNetworkingV1DnsRecord202JSONResponseBodyKind = "DnsRecord"
)

func (e CreateNetworkingV1DnsRecord202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1DnsRecord202JSONResponseBodyKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1DnsRecord200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1DnsRecord200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1DnsRecord200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1DnsRecord200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1DnsRecord200JSONResponseBodyKindDnsRecord GetNetworkingV1DnsRecord200JSONResponseBodyKind = "DnsRecord"
)

func (e GetNetworkingV1DnsRecord200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1DnsRecord200JSONResponseBodyKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsRecordJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1DnsRecordJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1DnsRecordJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsRecordJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsRecordJSONBodyKindDnsRecord UpdateNetworkingV1DnsRecordJSONBodyKind = "DnsRecord"
)

func (e UpdateNetworkingV1DnsRecordJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsRecordJSONBodyKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsRecord200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1DnsRecord200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1DnsRecord200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsRecord200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1DnsRecord200JSONResponseBodyKindDnsRecord UpdateNetworkingV1DnsRecord200JSONResponseBodyKind = "DnsRecord"
)

func (e UpdateNetworkingV1DnsRecord200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1DnsRecord200JSONResponseBodyKindDnsRecord:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Gateways200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1Gateways200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1Gateways200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1Gateways200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Gateways200JSONResponseBodyKindGatewayList ListNetworkingV1Gateways200JSONResponseBodyKind = "GatewayList"
)

func (e ListNetworkingV1Gateways200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1Gateways200JSONResponseBodyKindGatewayList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1GatewayJSONBodyApiVersionNetworkingv1 CreateNetworkingV1GatewayJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1GatewayJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1GatewayJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1GatewayJSONBodyKindGateway CreateNetworkingV1GatewayJSONBodyKind = "Gateway"
)

func (e CreateNetworkingV1GatewayJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1GatewayJSONBodyKindGateway:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Gateway202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1Gateway202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1Gateway202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1Gateway202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Gateway202JSONResponseBodyKindGateway CreateNetworkingV1Gateway202JSONResponseBodyKind = "Gateway"
)

func (e CreateNetworkingV1Gateway202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1Gateway202JSONResponseBodyKindGateway:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Gateway200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1Gateway200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1Gateway200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1Gateway200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Gateway200JSONResponseBodyKindGateway GetNetworkingV1Gateway200JSONResponseBodyKind = "Gateway"
)

func (e GetNetworkingV1Gateway200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1Gateway200JSONResponseBodyKindGateway:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1GatewayJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1GatewayJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1GatewayJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1GatewayJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1GatewayJSONBodyKindGateway UpdateNetworkingV1GatewayJSONBodyKind = "Gateway"
)

func (e UpdateNetworkingV1GatewayJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1GatewayJSONBodyKindGateway:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Gateway200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1Gateway200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1Gateway200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1Gateway200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Gateway200JSONResponseBodyKindGateway UpdateNetworkingV1Gateway200JSONResponseBodyKind = "Gateway"
)

func (e UpdateNetworkingV1Gateway200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1Gateway200JSONResponseBodyKindGateway:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyKindNetworkLinkEndpointList ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyKind = "NetworkLinkEndpointList"
)

func (e ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyKindNetworkLinkEndpointList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkEndpointJSONBodyApiVersionNetworkingv1 CreateNetworkingV1NetworkLinkEndpointJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1NetworkLinkEndpointJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkEndpointJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkEndpointJSONBodyKindNetworkLinkEndpoint CreateNetworkingV1NetworkLinkEndpointJSONBodyKind = "NetworkLinkEndpoint"
)

func (e CreateNetworkingV1NetworkLinkEndpointJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkEndpointJSONBodyKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyKindNetworkLinkEndpoint CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyKind = "NetworkLinkEndpoint"
)

func (e CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKindNetworkLinkEndpoint GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind = "NetworkLinkEndpoint"
)

func (e GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkEndpointJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1NetworkLinkEndpointJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1NetworkLinkEndpointJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkEndpointJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkEndpointJSONBodyKindNetworkLinkEndpoint UpdateNetworkingV1NetworkLinkEndpointJSONBodyKind = "NetworkLinkEndpoint"
)

func (e UpdateNetworkingV1NetworkLinkEndpointJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkEndpointJSONBodyKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKindNetworkLinkEndpoint UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind = "NetworkLinkEndpoint"
)

func (e UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKindNetworkLinkEndpoint:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyKindNetworkLinkServiceAssociationList ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyKind = "NetworkLinkServiceAssociationList"
)

func (e ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyKindNetworkLinkServiceAssociationList:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyKindNetworkLinkServiceAssociation GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyKind = "NetworkLinkServiceAssociation"
)

func (e GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyKindNetworkLinkServiceAssociation:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkServices200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1NetworkLinkServices200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1NetworkLinkServices200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkServices200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1NetworkLinkServices200JSONResponseBodyKindNetworkLinkServiceList ListNetworkingV1NetworkLinkServices200JSONResponseBodyKind = "NetworkLinkServiceList"
)

func (e ListNetworkingV1NetworkLinkServices200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1NetworkLinkServices200JSONResponseBodyKindNetworkLinkServiceList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkServiceJSONBodyApiVersionNetworkingv1 CreateNetworkingV1NetworkLinkServiceJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1NetworkLinkServiceJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkServiceJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkServiceJSONBodyKindNetworkLinkService CreateNetworkingV1NetworkLinkServiceJSONBodyKind = "NetworkLinkService"
)

func (e CreateNetworkingV1NetworkLinkServiceJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkServiceJSONBodyKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkService202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1NetworkLinkService202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1NetworkLinkService202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkService202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkLinkService202JSONResponseBodyKindNetworkLinkService CreateNetworkingV1NetworkLinkService202JSONResponseBodyKind = "NetworkLinkService"
)

func (e CreateNetworkingV1NetworkLinkService202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkLinkService202JSONResponseBodyKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkService200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkService200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1NetworkLinkService200JSONResponseBodyKindNetworkLinkService GetNetworkingV1NetworkLinkService200JSONResponseBodyKind = "NetworkLinkService"
)

func (e GetNetworkingV1NetworkLinkService200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1NetworkLinkService200JSONResponseBodyKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkServiceJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1NetworkLinkServiceJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1NetworkLinkServiceJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkServiceJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkServiceJSONBodyKindNetworkLinkService UpdateNetworkingV1NetworkLinkServiceJSONBodyKind = "NetworkLinkService"
)

func (e UpdateNetworkingV1NetworkLinkServiceJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkServiceJSONBodyKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkService200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkService200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkLinkService200JSONResponseBodyKindNetworkLinkService UpdateNetworkingV1NetworkLinkService200JSONResponseBodyKind = "NetworkLinkService"
)

func (e UpdateNetworkingV1NetworkLinkService200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkLinkService200JSONResponseBodyKindNetworkLinkService:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Networks200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1Networks200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1Networks200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1Networks200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Networks200JSONResponseBodyKindNetworkList ListNetworkingV1Networks200JSONResponseBodyKind = "NetworkList"
)

func (e ListNetworkingV1Networks200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1Networks200JSONResponseBodyKindNetworkList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkJSONBodyApiVersionNetworkingv1 CreateNetworkingV1NetworkJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1NetworkJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1NetworkJSONBodyKindNetwork CreateNetworkingV1NetworkJSONBodyKind = "Network"
)

func (e CreateNetworkingV1NetworkJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1NetworkJSONBodyKindNetwork:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Network202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1Network202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1Network202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1Network202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Network202JSONResponseBodyKindNetwork CreateNetworkingV1Network202JSONResponseBodyKind = "Network"
)

func (e CreateNetworkingV1Network202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1Network202JSONResponseBodyKindNetwork:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Network200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1Network200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1Network200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1Network200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Network200JSONResponseBodyKindNetwork GetNetworkingV1Network200JSONResponseBodyKind = "Network"
)

func (e GetNetworkingV1Network200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1Network200JSONResponseBodyKindNetwork:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1NetworkJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1NetworkJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1NetworkJSONBodyKindNetwork UpdateNetworkingV1NetworkJSONBodyKind = "Network"
)

func (e UpdateNetworkingV1NetworkJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1NetworkJSONBodyKindNetwork:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Network200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1Network200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1Network200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1Network200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Network200JSONResponseBodyKindNetwork UpdateNetworkingV1Network200JSONResponseBodyKind = "Network"
)

func (e UpdateNetworkingV1Network200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1Network200JSONResponseBodyKindNetwork:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Peerings200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1Peerings200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1Peerings200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1Peerings200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1Peerings200JSONResponseBodyKindPeeringList ListNetworkingV1Peerings200JSONResponseBodyKind = "PeeringList"
)

func (e ListNetworkingV1Peerings200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1Peerings200JSONResponseBodyKindPeeringList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PeeringJSONBodyApiVersionNetworkingv1 CreateNetworkingV1PeeringJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PeeringJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PeeringJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PeeringJSONBodyKindPeering CreateNetworkingV1PeeringJSONBodyKind = "Peering"
)

func (e CreateNetworkingV1PeeringJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PeeringJSONBodyKindPeering:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Peering202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1Peering202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1Peering202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1Peering202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1Peering202JSONResponseBodyKindPeering CreateNetworkingV1Peering202JSONResponseBodyKind = "Peering"
)

func (e CreateNetworkingV1Peering202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1Peering202JSONResponseBodyKindPeering:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Peering200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1Peering200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1Peering200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1Peering200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1Peering200JSONResponseBodyKindPeering GetNetworkingV1Peering200JSONResponseBodyKind = "Peering"
)

func (e GetNetworkingV1Peering200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1Peering200JSONResponseBodyKindPeering:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PeeringJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1PeeringJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PeeringJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PeeringJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PeeringJSONBodyKindPeering UpdateNetworkingV1PeeringJSONBodyKind = "Peering"
)

func (e UpdateNetworkingV1PeeringJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PeeringJSONBodyKindPeering:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Peering200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1Peering200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1Peering200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1Peering200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1Peering200JSONResponseBodyKindPeering UpdateNetworkingV1Peering200JSONResponseBodyKind = "Peering"
)

func (e UpdateNetworkingV1Peering200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1Peering200JSONResponseBodyKindPeering:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyKindPrivateLinkAccessList ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyKind = "PrivateLinkAccessList"
)

func (e ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyKindPrivateLinkAccessList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAccessJSONBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAccessJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAccessJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAccessJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAccessJSONBodyKindPrivateLinkAccess CreateNetworkingV1PrivateLinkAccessJSONBodyKind = "PrivateLinkAccess"
)

func (e CreateNetworkingV1PrivateLinkAccessJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAccessJSONBodyKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyKindPrivateLinkAccess CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyKind = "PrivateLinkAccess"
)

func (e CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAccess200JSONResponseBodyKindPrivateLinkAccess GetNetworkingV1PrivateLinkAccess200JSONResponseBodyKind = "PrivateLinkAccess"
)

func (e GetNetworkingV1PrivateLinkAccess200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAccess200JSONResponseBodyKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAccessJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAccessJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAccessJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAccessJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAccessJSONBodyKindPrivateLinkAccess UpdateNetworkingV1PrivateLinkAccessJSONBodyKind = "PrivateLinkAccess"
)

func (e UpdateNetworkingV1PrivateLinkAccessJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAccessJSONBodyKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyKindPrivateLinkAccess UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyKind = "PrivateLinkAccess"
)

func (e UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyKindPrivateLinkAccess:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyKindPrivateLinkAttachmentConnectionList ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyKind = "PrivateLinkAttachmentConnectionList"
)

func (e ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyKindPrivateLinkAttachmentConnectionList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKindPrivateLinkAttachmentConnection CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKind = "PrivateLinkAttachmentConnection"
)

func (e CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyKindPrivateLinkAttachmentConnection CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyKind = "PrivateLinkAttachmentConnection"
)

func (e CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKindPrivateLinkAttachmentConnection GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind = "PrivateLinkAttachmentConnection"
)

func (e GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKindPrivateLinkAttachmentConnection UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKind = "PrivateLinkAttachmentConnection"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONBodyKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKindPrivateLinkAttachmentConnection UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind = "PrivateLinkAttachmentConnection"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKindPrivateLinkAttachmentConnection:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyKindPrivateLinkAttachmentList ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyKind = "PrivateLinkAttachmentList"
)

func (e ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyKindPrivateLinkAttachmentList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachmentJSONBodyKindPrivateLinkAttachment CreateNetworkingV1PrivateLinkAttachmentJSONBodyKind = "PrivateLinkAttachment"
)

func (e CreateNetworkingV1PrivateLinkAttachmentJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachmentJSONBodyKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyKindPrivateLinkAttachment CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyKind = "PrivateLinkAttachment"
)

func (e CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyKindPrivateLinkAttachment GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind = "PrivateLinkAttachment"
)

func (e GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachmentJSONBodyKindPrivateLinkAttachment UpdateNetworkingV1PrivateLinkAttachmentJSONBodyKind = "PrivateLinkAttachment"
)

func (e UpdateNetworkingV1PrivateLinkAttachmentJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachmentJSONBodyKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyKindPrivateLinkAttachment UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind = "PrivateLinkAttachment"
)

func (e UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyKindPrivateLinkAttachment:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyApiVersionNetworkingv1 ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyApiVersion = "networking/v1"
)

func (e ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyKindTransitGatewayAttachmentList ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyKind = "TransitGatewayAttachmentList"
)

func (e ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyKind) Valid() bool {
	switch e {
	case ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyKindTransitGatewayAttachmentList:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersionNetworkingv1 CreateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1TransitGatewayAttachmentJSONBodyKindTransitGatewayAttachment CreateNetworkingV1TransitGatewayAttachmentJSONBodyKind = "TransitGatewayAttachment"
)

func (e CreateNetworkingV1TransitGatewayAttachmentJSONBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1TransitGatewayAttachmentJSONBodyKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyApiVersionNetworkingv1 CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyApiVersion = "networking/v1"
)

func (e CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyKindTransitGatewayAttachment CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyKind = "TransitGatewayAttachment"
)

func (e CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersionNetworkingv1 GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion = "networking/v1"
)

func (e GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyKindTransitGatewayAttachment GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind = "TransitGatewayAttachment"
)

func (e GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersionNetworkingv1 UpdateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1TransitGatewayAttachmentJSONBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1TransitGatewayAttachmentJSONBodyKindTransitGatewayAttachment UpdateNetworkingV1TransitGatewayAttachmentJSONBodyKind = "TransitGatewayAttachment"
)

func (e UpdateNetworkingV1TransitGatewayAttachmentJSONBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1TransitGatewayAttachmentJSONBodyKindTransitGatewayAttachment:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersionNetworkingv1 UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion = "networking/v1"
)

func (e UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersionNetworkingv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyKindTransitGatewayAttachment UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind = "TransitGatewayAttachment"
)

func (e UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyKindTransitGatewayAttachment:
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
type EnvScopedObjectReference struct {
	// Environment Environment of the referred resource, if env-scoped
	Environment *string `json:"environment,omitempty"`

	// Id ID of the referred resource
	Id string `json:"id"`

	// Related API URL for accessing or modifying the referred object
	Related string `json:"related"`

	// ResourceName CRN reference to the referred resource
	ResourceName string `json:"resource_name"`
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
type ObjectReference struct {
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
type NetworkingV1AccessPoint struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1AccessPointApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1AccessPointKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Access Point
	Spec *NetworkingV1AccessPointSpec `json:"spec,omitempty"`

	// Status The status of the Access Point
	Status *NetworkingV1AccessPointStatus `json:"status,omitempty"`
}
type NetworkingV1AccessPointApiVersion string
type NetworkingV1AccessPointKind string
type NetworkingV1AccessPointList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1AccessPointListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1AccessPointListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1AccessPointListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Access Point
		Status NetworkingV1AccessPointStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1AccessPointListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1AccessPointListApiVersion string
type NetworkingV1AccessPointListDataApiVersion string
type NetworkingV1AccessPointListDataKind string
type NetworkingV1AccessPointListKind string
type NetworkingV1AccessPointSpec struct {
	// Config The specific details of the different access point configurations.
	Config *NetworkingV1AccessPointSpec_Config `json:"config,omitempty"`

	// DisplayName The name of the access point.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Gateway The gateway to which this belongs.
	Gateway *ObjectReference `json:"gateway,omitempty"`
}
type NetworkingV1AccessPointSpec_Config struct {
	union json.RawMessage
}
type NetworkingV1AccessPointStatus struct {
	// Config Cloud specific status of the access point.
	Config *NetworkingV1AccessPointStatus_Config `json:"config,omitempty"`

	// ErrorCode Error code if access point is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if access point is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the access point:
	//
	//   PROVISIONING: Access point provisioning is in progress;
	//
	//   PENDING_ACCEPT: Access point connection request is pending acceptance by the customer;
	//
	//   READY:  Access point is ready;
	//
	//   FAILED: Access point is in a failed state;
	//
	//   DEPROVISIONING: Access point deprovisioning is in progress;
	//
	//   DISCONNECTED: Access Point has been disconnected in the cloud provider by the customer;
	//
	//   DEGRADED: Access Point is experiencing reduced performance or partial failure;
	//
	//   ERROR: Invalid customer input during Access Point creation;
	Phase string `json:"phase"`
}
type NetworkingV1AccessPointStatus_Config struct {
	union json.RawMessage
}
type NetworkingV1AwsEgressPrivateLinkEndpoint struct {
	// EnableHighAvailability Whether a resource should be provisioned with high availability. Endpoints deployed with high availability have network interfaces deployed in multiple AZs.
	EnableHighAvailability *bool `json:"enable_high_availability,omitempty"`

	// Kind AwsEgressPrivateLinkEndpoint kind.
	Kind NetworkingV1AwsEgressPrivateLinkEndpointKind `json:"kind"`

	// TargetSystem [Used by the Confluent Cloud Console] The target system or service that the PrivateLink Endpoint connects to (e.g. "MONGODB" or "SNOWFLAKE").
	TargetSystem *string `json:"target_system,omitempty"`

	// VpcEndpointServiceName ID of the VPC Endpoint service used for PrivateLink.
	VpcEndpointServiceName string `json:"vpc_endpoint_service_name"`
}
type NetworkingV1AwsEgressPrivateLinkEndpointKind string
type NetworkingV1AwsEgressPrivateLinkEndpointStatus struct {
	// Kind AwsEgressPrivateLinkEndpointStatus kind.
	Kind NetworkingV1AwsEgressPrivateLinkEndpointStatusKind `json:"kind"`

	// VpcEndpointDnsName DNS name of a VPC Endpoint (if any) that is connected to the VPC Endpoint service.
	VpcEndpointDnsName string `json:"vpc_endpoint_dns_name"`

	// VpcEndpointId ID of a VPC Endpoint (if any) that is connected to the VPC Endpoint service.
	VpcEndpointId string `json:"vpc_endpoint_id"`
}
type NetworkingV1AwsEgressPrivateLinkEndpointStatusKind string
type NetworkingV1AwsEgressPrivateLinkGatewaySpec struct {
	// Kind AWS Egress Private Link Gateway Spec kind type.
	Kind NetworkingV1AwsEgressPrivateLinkGatewaySpecKind `json:"kind"`

	// Region AWS region of the Egress Private Link Gateway.
	Region string `json:"region"`
}
type NetworkingV1AwsEgressPrivateLinkGatewaySpecKind string
type NetworkingV1AwsEgressPrivateLinkGatewayStatus struct {
	// Kind AWS Egress Private Link Gateway Status kind type.
	Kind NetworkingV1AwsEgressPrivateLinkGatewayStatusKind `json:"kind"`

	// PrincipalArn The principal ARN used by the AWS Egress Private Link Gateway.
	PrincipalArn *string `json:"principal_arn,omitempty"`
}
type NetworkingV1AwsEgressPrivateLinkGatewayStatusKind string
type NetworkingV1AwsIngressPrivateLinkEndpoint struct {
	// Kind AwsIngressPrivateLinkEndpoint kind.
	Kind NetworkingV1AwsIngressPrivateLinkEndpointKind `json:"kind"`

	// VpcEndpointId ID of a VPC Endpoint that will be connected to the VPC Endpoint service.
	VpcEndpointId string `json:"vpc_endpoint_id"`
}
type NetworkingV1AwsIngressPrivateLinkEndpointKind string
type NetworkingV1AwsIngressPrivateLinkEndpointStatus struct {
	// DnsDomain DNS domain name used to configure the Private Hosted Zone for the Access Point.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// Kind AwsIngressPrivateLinkEndpointStatus kind.
	Kind NetworkingV1AwsIngressPrivateLinkEndpointStatusKind `json:"kind"`

	// VpcEndpointId ID of the VPC Endpoint used for connecting to the VPC Endpoint service.
	VpcEndpointId string `json:"vpc_endpoint_id"`

	// VpcEndpointServiceName ID of the Confluent Cloud VPC Endpoint service used for PrivateLink.
	VpcEndpointServiceName string `json:"vpc_endpoint_service_name"`
}
type NetworkingV1AwsIngressPrivateLinkEndpointStatusKind string
type NetworkingV1AwsIngressPrivateLinkGatewaySpec struct {
	// Kind AWS Ingress Private Link Gateway Spec kind type.
	Kind NetworkingV1AwsIngressPrivateLinkGatewaySpecKind `json:"kind"`

	// Region AWS region of the Ingress Private Link Gateway.
	Region string `json:"region"`
}
type NetworkingV1AwsIngressPrivateLinkGatewaySpecKind string
type NetworkingV1AwsIngressPrivateLinkGatewayStatus struct {
	// Kind AWS Ingress Private Link Gateway Status kind type.
	Kind NetworkingV1AwsIngressPrivateLinkGatewayStatusKind `json:"kind"`

	// VpcEndpointServiceName The ID of the AWS VPC Endpoint Service that can be used to establish connections for all zones.
	VpcEndpointServiceName *string `json:"vpc_endpoint_service_name,omitempty"`
}
type NetworkingV1AwsIngressPrivateLinkGatewayStatusKind string
type NetworkingV1AwsNetwork struct {
	// Account The AWS account ID associated with the Confluent Cloud VPC.
	Account string `json:"account"`

	// Kind Network kind type.
	Kind NetworkingV1AwsNetworkKind `json:"kind"`

	// PrivateLinkEndpointService The endpoint service of the Confluent Cloud VPC. (used for PrivateLink) if available.
	PrivateLinkEndpointService *string `json:"private_link_endpoint_service,omitempty"`

	// Vpc The Confluent Cloud VPC ID.
	Vpc string `json:"vpc"`
}
type NetworkingV1AwsNetworkKind string
type NetworkingV1AwsPeering struct {
	// Account The AWS account ID associated with the VPC you are peering with Confluent Cloud network.
	Account string `json:"account"`

	// CustomerRegion The region of the VPC you are peering with Confluent Cloud network.
	CustomerRegion string `json:"customer_region"`

	// Kind Peering kind type.
	Kind NetworkingV1AwsPeeringKind `json:"kind"`

	// Routes The [CIDR blocks](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) of the VPC you are peering
	// with Confluent Cloud network. This is used by Confluent Cloud network to route traffic back to your network.
	// The CIDR block must be a private range and cannot overlap with the Confluent Cloud CIDR block.
	Routes []NetworkingV1Cidr `json:"routes"`

	// Vpc The VPC ID you are peering with Confluent Cloud network.
	Vpc string `json:"vpc"`
}
type NetworkingV1AwsPeeringKind string
type NetworkingV1AwsPeeringGatewaySpec struct {
	// Kind AWS Peering Gateway Spec kind type.
	Kind NetworkingV1AwsPeeringGatewaySpecKind `json:"kind"`

	// Region AWS region of the Peering Gateway.
	Region string `json:"region"`
}
type NetworkingV1AwsPeeringGatewaySpecKind string
type NetworkingV1AwsPrivateLinkAccess struct {
	// Account The AWS account ID for the account containing the VPCs you want to connect from using AWS PrivateLink.
	// You can find your AWS account ID [here](https://console.aws.amazon.com/billing/home?#/account)
	// under **My Account** in your AWS Management Console. Must be a **12 character string**.
	Account string `json:"account"`

	// Kind PrivateLink kind type.
	Kind NetworkingV1AwsPrivateLinkAccessKind `json:"kind"`
}
type NetworkingV1AwsPrivateLinkAccessKind string
type NetworkingV1AwsPrivateLinkAttachmentConnection struct {
	// Kind PrivateLinkAttachmentConnection kind.
	Kind NetworkingV1AwsPrivateLinkAttachmentConnectionKind `json:"kind"`

	// VpcEndpointId Id of a VPC Endpoint that is connected to the VPC Endpoint service.
	VpcEndpointId string `json:"vpc_endpoint_id"`
}
type NetworkingV1AwsPrivateLinkAttachmentConnectionKind string
type NetworkingV1AwsPrivateLinkAttachmentConnectionStatus struct {
	// Kind PrivateLinkAttachmentConnectionStatus kind.
	Kind NetworkingV1AwsPrivateLinkAttachmentConnectionStatusKind `json:"kind"`

	// VpcEndpointId Id of the VPC Endpoint (if any) that is connected to the VPC Endpoint service.
	VpcEndpointId string `json:"vpc_endpoint_id"`

	// VpcEndpointServiceName Id of the VPC Endpoint service used for PrivateLink.
	VpcEndpointServiceName string `json:"vpc_endpoint_service_name"`
}
type NetworkingV1AwsPrivateLinkAttachmentConnectionStatusKind string
type NetworkingV1AwsPrivateLinkAttachmentStatus struct {
	// Kind PrivateLinkAttachmentStatus kind.
	Kind NetworkingV1AwsPrivateLinkAttachmentStatusKind `json:"kind"`

	// VpcEndpointService AWS VPC Endpoint Service that can be used to establish connections for all zones.
	VpcEndpointService NetworkingV1AwsVpcEndpointService `json:"vpc_endpoint_service"`
}
type NetworkingV1AwsPrivateLinkAttachmentStatusKind string
type NetworkingV1AwsPrivateNetworkInterface struct {
	// Account The AWS account ID associated with the ENIs you are using for the Confluent Private Network Interface.
	Account *string `json:"account,omitempty"`

	// EgressRoutes List of egress CIDRs (IPv4) for egress PNI.
	EgressRoutes *[]string `json:"egress_routes,omitempty"`

	// Kind AwsPrivateNetworkInterface kind.
	Kind NetworkingV1AwsPrivateNetworkInterfaceKind `json:"kind"`

	// NetworkInterfaces List of the IDs of the Elastic Network Interfaces.
	NetworkInterfaces *[]string `json:"network_interfaces,omitempty"`
}
type NetworkingV1AwsPrivateNetworkInterfaceKind string
type NetworkingV1AwsPrivateNetworkInterfaceGatewaySpec struct {
	// Kind AWS Private Network Interface Gateway Spec kind type.
	Kind NetworkingV1AwsPrivateNetworkInterfaceGatewaySpecKind `json:"kind"`

	// Region AWS region of the Private Network Interface Gateway.
	Region string `json:"region"`

	// Zones AWS availability zone ids of the Private Network Interface Gateway.
	Zones []string `json:"zones"`
}
type NetworkingV1AwsPrivateNetworkInterfaceGatewaySpecKind string
type NetworkingV1AwsPrivateNetworkInterfaceGatewayStatus struct {
	// Account The AWS account ID associated with the Private Network Interface Gateway.
	Account *string `json:"account,omitempty"`

	// Kind AWS Private Network Interface Gateway Status kind type.
	Kind NetworkingV1AwsPrivateNetworkInterfaceGatewayStatusKind `json:"kind"`
}
type NetworkingV1AwsPrivateNetworkInterfaceGatewayStatusKind string
type NetworkingV1AwsTransitGatewayAttachment struct {
	// Kind AWS Transit Gateway Attachment kind type.
	Kind NetworkingV1AwsTransitGatewayAttachmentKind `json:"kind"`

	// RamShareArn The full AWS Resource Name (ARN) for the AWS Resource Access Manager (RAM) Share of the Transit Gateways that you want Confluent Cloud to be attached to.
	RamShareArn string `json:"ram_share_arn"`

	// Routes List of destination routes.
	Routes []NetworkingV1Cidr `json:"routes"`

	// TransitGatewayId The ID of the AWS Transit Gateway that you want Confluent CLoud to be attached to.
	TransitGatewayId string `json:"transit_gateway_id"`
}
type NetworkingV1AwsTransitGatewayAttachmentKind string
type NetworkingV1AwsTransitGatewayAttachmentStatus struct {
	// Kind AWS Transit Gateway Attachment Status kind type.
	Kind *NetworkingV1AwsTransitGatewayAttachmentStatusKind `json:"kind,omitempty"`

	// TransitGatewayAttachmentId The ID of the AWS Transit Gateway VPC Attachment that attaches Confluent VPC to Transit Gateway.
	TransitGatewayAttachmentId string `json:"transit_gateway_attachment_id"`
}
type NetworkingV1AwsTransitGatewayAttachmentStatusKind string
type NetworkingV1AwsVpcEndpointService struct {
	// VpcEndpointServiceName Id of the VPC Endpoint service.
	VpcEndpointServiceName string `json:"vpc_endpoint_service_name"`
}
type NetworkingV1AzureEgressPrivateLinkEndpoint struct {
	// Kind AzureEgressPrivateLinkEndpoint kind.
	Kind NetworkingV1AzureEgressPrivateLinkEndpointKind `json:"kind"`

	// PrivateLinkServiceResourceId Resource ID of the Azure Private Link service.
	PrivateLinkServiceResourceId string `json:"private_link_service_resource_id"`

	// PrivateLinkSubresourceName Name of the subresource for the Private Endpoint to connect to.
	PrivateLinkSubresourceName *string `json:"private_link_subresource_name,omitempty"`

	// TargetSystem [Used by the Confluent Cloud Console] The target system or service that the PrivateLink Endpoint connects to (e.g. "MONGODB" or "SNOWFLAKE").
	TargetSystem *string `json:"target_system,omitempty"`
}
type NetworkingV1AzureEgressPrivateLinkEndpointKind string
type NetworkingV1AzureEgressPrivateLinkEndpointStatus struct {
	// Kind AzureEgressPrivateLinkEndpointStatus kind.
	Kind NetworkingV1AzureEgressPrivateLinkEndpointStatusKind `json:"kind"`

	// PrivateEndpointCustomDnsConfigDomains Domains of the Private Endpoint (if any) based off FQDNs in Azure custom DNS configs, which are required in your private DNS setup.
	PrivateEndpointCustomDnsConfigDomains *[]string `json:"private_endpoint_custom_dns_config_domains,omitempty"`

	// PrivateEndpointDomain Domain of the Private Endpoint (if any) that is connected to the Private Link service.
	PrivateEndpointDomain *string `json:"private_endpoint_domain,omitempty"`

	// PrivateEndpointIpAddress IP address of the Private Endpoint (if any) that is connected to the Private Link service.
	PrivateEndpointIpAddress string `json:"private_endpoint_ip_address"`

	// PrivateEndpointResourceId Resource ID of the Private Endpoint (if any) that is connected to the Private Link service.
	PrivateEndpointResourceId string `json:"private_endpoint_resource_id"`
}
type NetworkingV1AzureEgressPrivateLinkEndpointStatusKind string
type NetworkingV1AzureEgressPrivateLinkGatewaySpec struct {
	// Kind Azure Egress Private Link Gateway Spec kind type.
	Kind NetworkingV1AzureEgressPrivateLinkGatewaySpecKind `json:"kind"`

	// Region Azure region of the Egress Private Link Gateway.
	Region string `json:"region"`
}
type NetworkingV1AzureEgressPrivateLinkGatewaySpecKind string
type NetworkingV1AzureEgressPrivateLinkGatewayStatus struct {
	// Kind Azure Egress Private Link Gateway Status kind type.
	Kind NetworkingV1AzureEgressPrivateLinkGatewayStatusKind `json:"kind"`

	// Subscription The Azure Subscription ID associated with the Confluent Cloud VPC.
	Subscription *string `json:"subscription,omitempty"`
}
type NetworkingV1AzureEgressPrivateLinkGatewayStatusKind string
type NetworkingV1AzureIngressPrivateLinkEndpoint struct {
	// Kind AzureIngressPrivateLinkEndpoint kind.
	Kind NetworkingV1AzureIngressPrivateLinkEndpointKind `json:"kind"`

	// PrivateEndpointResourceId Resource ID of a Private Endpoint that will be connected to the Private Link service.
	PrivateEndpointResourceId string `json:"private_endpoint_resource_id"`
}
type NetworkingV1AzureIngressPrivateLinkEndpointKind string
type NetworkingV1AzureIngressPrivateLinkEndpointStatus struct {
	// DnsDomain DNS domain name used to configure the Private DNS Zone for the Access Point.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// Kind AzureIngressPrivateLinkEndpointStatus kind.
	Kind NetworkingV1AzureIngressPrivateLinkEndpointStatusKind `json:"kind"`

	// PrivateEndpointResourceId Resource ID of the Private Endpoint used for connecting to the Private Link Service.
	PrivateEndpointResourceId string `json:"private_endpoint_resource_id"`

	// PrivateLinkServiceAlias Alias of the Confluent Cloud Private Link Service.
	PrivateLinkServiceAlias string `json:"private_link_service_alias"`

	// PrivateLinkServiceResourceId Resource ID of the Confluent Cloud Private Link Service.
	PrivateLinkServiceResourceId string `json:"private_link_service_resource_id"`
}
type NetworkingV1AzureIngressPrivateLinkEndpointStatusKind string
type NetworkingV1AzureIngressPrivateLinkGatewaySpec struct {
	// Kind Azure Ingress Private Link Gateway Spec kind type.
	Kind NetworkingV1AzureIngressPrivateLinkGatewaySpecKind `json:"kind"`

	// Region Azure region of the Ingress Private Link Gateway.
	Region string `json:"region"`
}
type NetworkingV1AzureIngressPrivateLinkGatewaySpecKind string
type NetworkingV1AzureIngressPrivateLinkGatewayStatus struct {
	// Kind Azure Ingress Private Link Gateway Status kind type.
	Kind NetworkingV1AzureIngressPrivateLinkGatewayStatusKind `json:"kind"`

	// PrivateLinkServiceAlias Alias of the Confluent Cloud Private Link Service.
	PrivateLinkServiceAlias *string `json:"private_link_service_alias,omitempty"`

	// PrivateLinkServiceResourceId Resource ID of the Confluent Cloud Private Link Service.
	PrivateLinkServiceResourceId *string `json:"private_link_service_resource_id,omitempty"`
}
type NetworkingV1AzureIngressPrivateLinkGatewayStatusKind string
type NetworkingV1AzureNetwork struct {
	// Kind Network kind type.
	Kind NetworkingV1AzureNetworkKind `json:"kind"`

	// PrivateLinkServiceAliases The mapping of zones to Private Link Service Aliases if available. Keys are zones
	// and values are [Azure Private Link Service
	// Aliases](https://docs.microsoft.com/en-us/azure/private-link/private-link-service-overview#share-your-service).
	PrivateLinkServiceAliases *map[string]string `json:"private_link_service_aliases,omitempty"`

	// PrivateLinkServiceResourceIds The mapping of zones to Private Link Service Resource IDs if available. Keys are zones
	// and values are [Azure Private Link Service Resource
	// IDs](https://docs.microsoft.com/en-us/azure/private-link/private-link-service-overview#share-your-service).
	PrivateLinkServiceResourceIds *map[string]string `json:"private_link_service_resource_ids,omitempty"`

	// Subscription The Azure Subscription ID associated with the Confluent Cloud VPC.
	Subscription string `json:"subscription"`

	// Vnet The resource ID of the Confluent Cloud VNet.
	Vnet string `json:"vnet"`
}
type NetworkingV1AzureNetworkKind string
type NetworkingV1AzurePeering struct {
	// CustomerRegion The region of the VNet you are peering with Confluent Cloud network.
	CustomerRegion string `json:"customer_region"`

	// Kind Peering kind type.
	Kind NetworkingV1AzurePeeringKind `json:"kind"`

	// Tenant The Azure Tenant ID in which your Azure Subscription exists.
	// Represents an organization in Azure Active Directory. You can find your Azure Tenant ID in the Azure Portal
	// under
	// [Azure Active Directory](https://portal.azure.com/#blade/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/Overview).
	// Must be a valid **32 character UUID string**.
	Tenant string `json:"tenant"`

	// Vnet The resource ID of the VNet that you are peering with Confluent Cloud. You can find the name of your Azure VNet in the [Azure Portal on the Overview tab of your Azure Virtual Network](https://portal.azure.com/#blade/HubsExtension/BrowseResource/resourceType/Microsoft.Network%2FvirtualNetworks).
	Vnet string `json:"vnet"`
}
type NetworkingV1AzurePeeringKind string
type NetworkingV1AzurePeeringGatewaySpec struct {
	// Kind Azure Peering Gateway Spec kind type.
	Kind NetworkingV1AzurePeeringGatewaySpecKind `json:"kind"`

	// Region Azure region of the Peering Gateway.
	Region string `json:"region"`
}
type NetworkingV1AzurePeeringGatewaySpecKind string
type NetworkingV1AzurePrivateLinkAccess struct {
	// Kind PrivateLink kind type.
	Kind NetworkingV1AzurePrivateLinkAccessKind `json:"kind"`

	// Subscription The Azure subscription ID for the account containing the VNets you want to connect from using
	// Azure Private Link. You can find your Azure subscription ID in the subscription section of your
	// [Microsoft Azure Portal](https://portal.azure.com/#blade/Microsoft_Azure_Billing/SubscriptionsBlade).
	// Must be a valid **32 character UUID string**.
	Subscription string `json:"subscription"`
}
type NetworkingV1AzurePrivateLinkAccessKind string
type NetworkingV1AzurePrivateLinkAttachmentConnection struct {
	// Kind PrivateLinkAttachmentConnection kind.
	Kind NetworkingV1AzurePrivateLinkAttachmentConnectionKind `json:"kind"`

	// PrivateEndpointResourceId Resource Id of the PrivateEndpoint that is connected to the PrivateLink service.
	PrivateEndpointResourceId string `json:"private_endpoint_resource_id"`
}
type NetworkingV1AzurePrivateLinkAttachmentConnectionKind string
type NetworkingV1AzurePrivateLinkAttachmentConnectionStatus struct {
	// Kind PrivateLinkAttachmentConnectionStatus kind.
	Kind NetworkingV1AzurePrivateLinkAttachmentConnectionStatusKind `json:"kind"`

	// PrivateEndpointResourceId Resource Id of the PrivateEndpoint (if any) that is connected to
	// the PrivateLink service.
	PrivateEndpointResourceId string `json:"private_endpoint_resource_id"`

	// PrivateLinkServiceAlias Azure PrivateLink service alias.
	PrivateLinkServiceAlias string `json:"private_link_service_alias"`

	// PrivateLinkServiceResourceId Azure PrivateLink service resource id.
	PrivateLinkServiceResourceId string `json:"private_link_service_resource_id"`
}
type NetworkingV1AzurePrivateLinkAttachmentConnectionStatusKind string
type NetworkingV1AzurePrivateLinkAttachmentStatus struct {
	// Kind PrivateLinkAttachmentStatus kind.
	Kind NetworkingV1AzurePrivateLinkAttachmentStatusKind `json:"kind"`

	// PrivateLinkService Azure PrivateLink service that can be used to connect to a PrivateEndpoint.
	PrivateLinkService NetworkingV1AzurePrivateLinkService `json:"private_link_service"`
}
type NetworkingV1AzurePrivateLinkAttachmentStatusKind string
type NetworkingV1AzurePrivateLinkService struct {
	// PrivateLinkServiceAlias Azure PrivateLink service alias.
	PrivateLinkServiceAlias string `json:"private_link_service_alias"`

	// PrivateLinkServiceResourceId Azure PrivateLink service resource id.
	PrivateLinkServiceResourceId string `json:"private_link_service_resource_id"`
}
type NetworkingV1Cidr = string
type NetworkingV1ConnectionType = string
type NetworkingV1DnsConfig struct {
	// Resolution Network DNS resolution type.
	Resolution string `json:"resolution"`
}
type NetworkingV1DnsForwarder struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1DnsForwarderApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1DnsForwarderKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Dns Forwarder
	Spec *NetworkingV1DnsForwarderSpec `json:"spec,omitempty"`

	// Status The status of the Dns Forwarder
	Status *NetworkingV1DnsForwarderStatus `json:"status,omitempty"`
}
type NetworkingV1DnsForwarderApiVersion string
type NetworkingV1DnsForwarderKind string
type NetworkingV1DnsForwarderList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1DnsForwarderListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1DnsForwarderListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1DnsForwarderListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Dns Forwarder
		Status NetworkingV1DnsForwarderStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1DnsForwarderListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1DnsForwarderListApiVersion string
type NetworkingV1DnsForwarderListDataApiVersion string
type NetworkingV1DnsForwarderListDataKind string
type NetworkingV1DnsForwarderListKind string
type NetworkingV1DnsForwarderSpec struct {
	// Config The specific details of different kinds of configuration for DNS Forwarder.
	Config *NetworkingV1DnsForwarderSpec_Config `json:"config,omitempty"`

	// DisplayName The name of the DNS forwarder
	DisplayName *string `json:"display_name,omitempty"`

	// Domains List of domains for the DNS forwarder to use
	Domains *[]string `json:"domains,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Gateway The gateway to which this belongs.
	Gateway *ObjectReference `json:"gateway,omitempty"`
}
type NetworkingV1DnsForwarderSpec_Config struct {
	union json.RawMessage
}
type NetworkingV1DnsForwarderStatus struct {
	// ErrorCode Error code if dns forwarder is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if dns forwarder is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the DNS forwarder:
	//
	//   PROVISIONING: DNS forwarder provisioning is in progress;
	//
	//   CREATED: DNS forwarder is created. It will automatically become ready once a Kafka cluster is provisioned;
	//
	//   READY: DNS forwarder is ready;
	//
	//   FAILED: DNS forwarder is in a failed state;
	//
	//   DEGRADED: DNS forwarder is in a degraded state, transitioning from 'READY' due to unreachable DNS resolvers;
	//
	//   DEPROVISIONING: DNS forwarder deprovisioning is in progress;
	Phase string `json:"phase"`
}
type NetworkingV1DnsRecord struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1DnsRecordApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1DnsRecordKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Dns Record
	Spec *NetworkingV1DnsRecordSpec `json:"spec,omitempty"`

	// Status The status of the Dns Record
	Status *NetworkingV1DnsRecordStatus `json:"status,omitempty"`
}
type NetworkingV1DnsRecordApiVersion string
type NetworkingV1DnsRecordKind string
type NetworkingV1DnsRecordList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1DnsRecordListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1DnsRecordListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1DnsRecordListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Dns Record
		Status NetworkingV1DnsRecordStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1DnsRecordListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1DnsRecordListApiVersion string
type NetworkingV1DnsRecordListDataApiVersion string
type NetworkingV1DnsRecordListDataKind string
type NetworkingV1DnsRecordListKind string
type NetworkingV1DnsRecordSpec struct {
	// Config The config of the DNS record.
	Config *NetworkingV1DnsRecordSpec_Config `json:"config,omitempty"`

	// DisplayName The name of the DNS record.
	DisplayName *string `json:"display_name,omitempty"`

	// Domain The fully qualified domain name of the DNS record.
	Domain *string `json:"domain,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Gateway The gateway to which this belongs.
	Gateway *TypedEnvScopedObjectReference `json:"gateway,omitempty"`
}
type NetworkingV1DnsRecordSpec_Config struct {
	union json.RawMessage
}
type NetworkingV1DnsRecordStatus struct {
	// ErrorCode Error code if the DNS record is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if the DNS record is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the DNS record:
	//
	//   PROVISIONING: DNS record provisioning is in progress;
	//
	//   CREATED: DNS record is created. It will automatically become ready once a Kafka cluster is provisioned;
	//
	//   READY: DNS record is ready;
	//
	//   FAILED: DNS record is in a failed state;
	//
	//   DEPROVISIONING: DNS record deprovisioning is in progress;
	Phase string `json:"phase"`
}
type NetworkingV1ForwardViaIp struct {
	// DnsServerIps List of IP addresses of the DNS server
	DnsServerIps []NetworkingV1Ip `json:"dns_server_ips"`

	// Kind DNS Forwarder Configured via DNS Server IPs kind type.
	Kind NetworkingV1ForwardViaIpKind `json:"kind"`
}
type NetworkingV1ForwardViaIpKind string
type NetworkingV1Gateway struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1GatewayApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1GatewayKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Gateway
	Spec *NetworkingV1GatewaySpec `json:"spec,omitempty"`

	// Status The status of the Gateway
	Status *NetworkingV1GatewayStatus `json:"status,omitempty"`
}
type NetworkingV1GatewayApiVersion string
type NetworkingV1GatewayKind string
type NetworkingV1GatewayList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1GatewayListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1GatewayListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1GatewayListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Gateway
		Status NetworkingV1GatewayStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1GatewayListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1GatewayListApiVersion string
type NetworkingV1GatewayListDataApiVersion string
type NetworkingV1GatewayListDataKind string
type NetworkingV1GatewayListKind string
type NetworkingV1GatewaySpec struct {
	// Config Gateway type specific configuration. Please note that Peering configs are not supported in Create requests.
	Config *NetworkingV1GatewaySpec_Config `json:"config,omitempty"`

	// DisplayName The name of the gateway.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`
}
type NetworkingV1GatewaySpec_Config struct {
	union json.RawMessage
}
type NetworkingV1GatewayStatus struct {
	// CloudGateway Gateway type specific status.
	CloudGateway *NetworkingV1GatewayStatus_CloudGateway `json:"cloud_gateway,omitempty"`

	// ErrorCode Error code if gateway is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if gateway is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the gateway:
	//
	//   CREATED: gateway exists without an Access Point.
	//
	//   PROVISIONING: gateway provisioning is in progress;
	//
	//   READY:  gateway is ready;
	//
	//   FAILED: gateway is in a failed state;
	//
	//   DEPROVISIONING: gateway deprovisioning is in progress;
	//
	//   EXPIRED: gateway has timed out waiting for connections, can only be deleted;
	Phase string `json:"phase"`
}
type NetworkingV1GatewayStatus_CloudGateway struct {
	union json.RawMessage
}
type NetworkingV1GcpEgressPrivateServiceConnectEndpoint struct {
	// Kind GcpEgressPrivateServiceConnectEndpoint kind.
	Kind NetworkingV1GcpEgressPrivateServiceConnectEndpointKind `json:"kind"`

	// PrivateServiceConnectEndpointTarget URI of the service attachment for the published service that the Private Service Connect Endpoint connects to or "ALL_GOOGLE_APIS" for global Google APIs.
	PrivateServiceConnectEndpointTarget string `json:"private_service_connect_endpoint_target"`

	// TargetSystem [Used by the Confluent Cloud Console] The target system or service that the PrivateLink Endpoint connects to (e.g. "GCS" or "SNOWFLAKE").
	TargetSystem *string `json:"target_system,omitempty"`
}
type NetworkingV1GcpEgressPrivateServiceConnectEndpointKind string
type NetworkingV1GcpEgressPrivateServiceConnectEndpointStatus struct {
	// Kind GcpEgressPrivateServiceConnectEndpointStatus kind.
	Kind NetworkingV1GcpEgressPrivateServiceConnectEndpointStatusKind `json:"kind"`

	// PrivateServiceConnectEndpointConnectionId Connection ID of the Private Service Connect Endpoint (if any) that is connected to the endpoint target.
	PrivateServiceConnectEndpointConnectionId string `json:"private_service_connect_endpoint_connection_id"`

	// PrivateServiceConnectEndpointIpAddress IP address of the Private Service Connect Endpoint (if any) that is connected to the endpoint target.
	PrivateServiceConnectEndpointIpAddress string `json:"private_service_connect_endpoint_ip_address"`

	// PrivateServiceConnectEndpointName Name of the Private Service Connect Endpoint (if any) that is connected to the endpoint target.
	PrivateServiceConnectEndpointName string `json:"private_service_connect_endpoint_name"`
}
type NetworkingV1GcpEgressPrivateServiceConnectEndpointStatusKind string
type NetworkingV1GcpEgressPrivateServiceConnectGatewaySpec struct {
	// Kind GCP Private Service Connect Gateway Spec kind type.
	Kind NetworkingV1GcpEgressPrivateServiceConnectGatewaySpecKind `json:"kind"`

	// Region GCP region of the Egress Private Service Connect Gateway.
	Region string `json:"region"`
}
type NetworkingV1GcpEgressPrivateServiceConnectGatewaySpecKind string
type NetworkingV1GcpEgressPrivateServiceConnectGatewayStatus struct {
	// Kind GCP Private Service Connect Gateway Status kind type.
	Kind NetworkingV1GcpEgressPrivateServiceConnectGatewayStatusKind `json:"kind"`

	// Project The GCP project used by the GCP Private Service Connect Gateway.
	Project *string `json:"project,omitempty"`
}
type NetworkingV1GcpEgressPrivateServiceConnectGatewayStatusKind string
type NetworkingV1GcpIngressPrivateServiceConnectEndpoint struct {
	// Kind GcpIngressPrivateServiceConnectEndpoint kind.
	Kind NetworkingV1GcpIngressPrivateServiceConnectEndpointKind `json:"kind"`

	// PrivateServiceConnectConnectionId The ID of the Private Service Connect connection.
	PrivateServiceConnectConnectionId string `json:"private_service_connect_connection_id"`
}
type NetworkingV1GcpIngressPrivateServiceConnectEndpointKind string
type NetworkingV1GcpIngressPrivateServiceConnectEndpointStatus struct {
	// DnsDomain DNS domain name used to configure the DNS Zone for the Access Point.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// Kind GcpIngressPrivateServiceConnectEndpointStatus kind.
	Kind NetworkingV1GcpIngressPrivateServiceConnectEndpointStatusKind `json:"kind"`

	// PrivateServiceConnectConnectionId The ID of the Private Service Connect connection.
	PrivateServiceConnectConnectionId string `json:"private_service_connect_connection_id"`

	// PrivateServiceConnectServiceAttachment URI of the Private Service Connect Service Attachment in Confluent Cloud.
	PrivateServiceConnectServiceAttachment string `json:"private_service_connect_service_attachment"`
}
type NetworkingV1GcpIngressPrivateServiceConnectEndpointStatusKind string
type NetworkingV1GcpIngressPrivateServiceConnectGatewaySpec struct {
	// Kind GCP Ingress Private Service Connect Gateway Spec kind type.
	Kind NetworkingV1GcpIngressPrivateServiceConnectGatewaySpecKind `json:"kind"`

	// Region GCP region of the Ingress Private Service Connect Gateway.
	Region string `json:"region"`
}
type NetworkingV1GcpIngressPrivateServiceConnectGatewaySpecKind string
type NetworkingV1GcpIngressPrivateServiceConnectGatewayStatus struct {
	// Kind GCP Ingress Private Service Connect Gateway Status kind type.
	Kind NetworkingV1GcpIngressPrivateServiceConnectGatewayStatusKind `json:"kind"`

	// PrivateServiceConnectServiceAttachment URI of the Private Service Connect Service Attachment in Confluent Cloud.
	PrivateServiceConnectServiceAttachment *string `json:"private_service_connect_service_attachment,omitempty"`
}
type NetworkingV1GcpIngressPrivateServiceConnectGatewayStatusKind string
type NetworkingV1GcpNetwork struct {
	// Kind Network kind type.
	Kind NetworkingV1GcpNetworkKind `json:"kind"`

	// PrivateServiceConnectServiceAttachments The mapping of zones to Private Service Connect Service
	// Attachments if available. Keys are zones and values are
	// [GCP Private Service Connect Service
	// Attachment](https://cloud.google.com/vpc/docs/configure-private-service-connect-producer#api_7)
	PrivateServiceConnectServiceAttachments *map[string]string `json:"private_service_connect_service_attachments,omitempty"`

	// Project The GCP Project ID associated with the Confluent Cloud VPC.
	Project string `json:"project"`

	// VpcNetwork The network name of the Confluent Cloud VPC.
	VpcNetwork string `json:"vpc_network"`
}
type NetworkingV1GcpNetworkKind string
type NetworkingV1GcpPeering struct {
	// ImportCustomRoutes Enable customer route import. For more information, see
	// [Importing custom routes](https://cloud.google.com/vpc/docs/vpc-peering#importing-exporting-routes).
	ImportCustomRoutes *bool `json:"import_custom_routes,omitempty"`

	// Kind Peering kind type.
	Kind NetworkingV1GcpPeeringKind `json:"kind"`

	// Project The Google Cloud project ID associated with the VPC that you are peering with Confluent Cloud network.
	Project string `json:"project"`

	// VpcNetwork The name of the VPC that you are peering with Confluent Cloud network.
	VpcNetwork string `json:"vpc_network"`
}
type NetworkingV1GcpPeeringKind string
type NetworkingV1GcpPeeringGatewaySpec struct {
	// Kind GCP Peering Gateway Spec kind type.
	Kind NetworkingV1GcpPeeringGatewaySpecKind `json:"kind"`

	// Region GCP region of the Peering Gateway.
	Region string `json:"region"`
}
type NetworkingV1GcpPeeringGatewaySpecKind string
type NetworkingV1GcpPeeringGatewayStatus struct {
	// IamPrincipal The IAM principal email used by the GCP Peering Gateway.
	IamPrincipal *string `json:"iam_principal,omitempty"`

	// Kind GCP Peering Gateway Status kind type.
	Kind NetworkingV1GcpPeeringGatewayStatusKind `json:"kind"`
}
type NetworkingV1GcpPeeringGatewayStatusKind string
type NetworkingV1GcpPrivateLinkAttachmentConnection struct {
	// Kind PrivateLinkAttachmentConnection kind.
	Kind NetworkingV1GcpPrivateLinkAttachmentConnectionKind `json:"kind"`

	// PrivateServiceConnectConnectionId Id of the Private Service connection.
	PrivateServiceConnectConnectionId string `json:"private_service_connect_connection_id"`
}
type NetworkingV1GcpPrivateLinkAttachmentConnectionKind string
type NetworkingV1GcpPrivateLinkAttachmentConnectionStatus struct {
	// Kind PrivateLinkAttachmentConnectionStatus kind.
	Kind NetworkingV1GcpPrivateLinkAttachmentConnectionStatusKind `json:"kind"`

	// PrivateServiceConnectConnectionId Id of the Private Service connection.
	PrivateServiceConnectConnectionId string `json:"private_service_connect_connection_id"`

	// PrivateServiceConnectServiceAttachment GCP Private Service Connect ServiceAttachment.
	PrivateServiceConnectServiceAttachment string `json:"private_service_connect_service_attachment"`
}
type NetworkingV1GcpPrivateLinkAttachmentConnectionStatusKind string
type NetworkingV1GcpPrivateLinkAttachmentStatus struct {
	// Kind PrivateLinkAttachmentStatus kind.
	Kind NetworkingV1GcpPrivateLinkAttachmentStatusKind `json:"kind"`

	// ServiceAttachment GCP PSC Service attachment that can be used to connect
	// to a PSC Endpoint.
	ServiceAttachment NetworkingV1GcpPscServiceAttachment `json:"service_attachment"`
}
type NetworkingV1GcpPrivateLinkAttachmentStatusKind string
type NetworkingV1GcpPrivateServiceConnectAccess struct {
	// Kind PrivateLink kind type.
	Kind NetworkingV1GcpPrivateServiceConnectAccessKind `json:"kind"`

	// Project The GCP project ID for the account containing the VPCs that you want to connect from
	// using Private Service Connect. You can find your Google Cloud Project ID under **Project ID** section of
	// your [Google Cloud Console dashboard](https://console.cloud.google.com/home/dashboard).
	Project string `json:"project"`
}
type NetworkingV1GcpPrivateServiceConnectAccessKind string
type NetworkingV1GcpPscServiceAttachment struct {
	// PrivateServiceConnectServiceAttachment Id of a Private Service Connect Service Attachment in Confluent Cloud.
	PrivateServiceConnectServiceAttachment string `json:"private_service_connect_service_attachment"`
}
type NetworkingV1Ip = string
type NetworkingV1IpAddress struct {
	// AddressType Whether the address is used for egress or ingress.
	AddressType *string `json:"address_type,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1IpAddressApiVersion `json:"api_version,omitempty"`

	// Cloud The cloud service provider in which the address exists.
	Cloud *string `json:"cloud,omitempty"`

	// IpPrefix The IP Address range.
	IpPrefix *string `json:"ip_prefix,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind *NetworkingV1IpAddressKind `json:"kind,omitempty"`

	// Region The region/location where the IP Address is in use.
	Region *string `json:"region,omitempty"`

	// Services The service types that will use the address.
	Services *[]string `json:"services,omitempty"`
}
type NetworkingV1IpAddressApiVersion string
type NetworkingV1IpAddressKind string
type NetworkingV1IpAddressList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1IpAddressListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// AddressType Whether the address is used for egress or ingress.
		AddressType *string `json:"address_type,omitempty"`

		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1IpAddressListDataApiVersion `json:"api_version,omitempty"`

		// Cloud The cloud service provider in which the address exists.
		Cloud *string `json:"cloud,omitempty"`

		// IpPrefix The IP Address range.
		IpPrefix *string `json:"ip_prefix,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind *NetworkingV1IpAddressListDataKind `json:"kind,omitempty"`

		// Region The region/location where the IP Address is in use.
		Region *string `json:"region,omitempty"`

		// Services The service types that will use the address.
		Services *[]string `json:"services,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1IpAddressListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1IpAddressListApiVersion string
type NetworkingV1IpAddressListDataApiVersion string
type NetworkingV1IpAddressListDataKind string
type NetworkingV1IpAddressListKind string
type NetworkingV1Network struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1NetworkApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1NetworkKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Network
	Spec *NetworkingV1NetworkSpec `json:"spec,omitempty"`

	// Status The status of the Network
	Status *NetworkingV1NetworkStatus `json:"status,omitempty"`
}
type NetworkingV1NetworkApiVersion string
type NetworkingV1NetworkKind string
type NetworkingV1NetworkLinkEndpoint struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1NetworkLinkEndpointApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1NetworkLinkEndpointKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Network Link Endpoint
	Spec *NetworkingV1NetworkLinkEndpointSpec `json:"spec,omitempty"`

	// Status The status of the Network Link Endpoint
	Status *NetworkingV1NetworkLinkEndpointStatus `json:"status,omitempty"`
}
type NetworkingV1NetworkLinkEndpointApiVersion string
type NetworkingV1NetworkLinkEndpointKind string
type NetworkingV1NetworkLinkEndpointList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1NetworkLinkEndpointListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1NetworkLinkEndpointListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1NetworkLinkEndpointListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Network Link Endpoint
		Status NetworkingV1NetworkLinkEndpointStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1NetworkLinkEndpointListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1NetworkLinkEndpointListApiVersion string
type NetworkingV1NetworkLinkEndpointListDataApiVersion string
type NetworkingV1NetworkLinkEndpointListDataKind string
type NetworkingV1NetworkLinkEndpointListKind string
type NetworkingV1NetworkLinkEndpointSpec struct {
	// Description The description of the network link endpoint
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the network link endpoint
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Network The network to which this belongs.
	Network *EnvScopedObjectReference `json:"network,omitempty"`

	// NetworkLinkService The network_link_service to which this belongs.
	NetworkLinkService *EnvScopedObjectReference `json:"network_link_service,omitempty"`
}
type NetworkingV1NetworkLinkEndpointStatus struct {
	// ErrorCode Error code if network link is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if network link is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// ExpiresAt The date and time when the request expires if it is not accepted by the target network admin.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Phase The lifecycle phase of the network link endpoint:
	//
	//   PROVISIONING: network link endpoint provisioning is in progress;
	//
	//   PENDING_ACCEPT: network link endpoint request is pending acceptance by the the owner of the target;
	//
	//   READY:  network link endpoint is ready;
	//
	//   FAILED: network link endpoint is in a failed state;
	//
	//   DEPROVISIONING: network link endpoint deprovisioning is in progress;
	//
	//   EXPIRED: network link endpoint request is expired, can only be deleted;
	//
	//   DISCONNECTED: network link endpoint is in a disconnected state, target owner has removed the permissions;
	//
	//   DISCONNECTING: network link endpoint disconnection is in progress;
	//
	//   INACTIVE: network link endpoint is created, but not active since there are no clusters in the network;
	Phase string `json:"phase"`
}
type NetworkingV1NetworkLinkService struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1NetworkLinkServiceApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1NetworkLinkServiceKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Network Link Service
	Spec *NetworkingV1NetworkLinkServiceSpec `json:"spec,omitempty"`

	// Status The status of the Network Link Service
	Status *NetworkingV1NetworkLinkServiceStatus `json:"status,omitempty"`
}
type NetworkingV1NetworkLinkServiceApiVersion string
type NetworkingV1NetworkLinkServiceKind string
type NetworkingV1NetworkLinkServiceAcceptPolicy struct {
	// Environments List of environments from which connections can be accepted.
	// All networks win the list of environment will be allowed.
	Environments *[]string `json:"environments,omitempty"`

	// Networks List of networks from which connections can be accepted.
	Networks *[]string `json:"networks,omitempty"`
}
type NetworkingV1NetworkLinkServiceAssociation struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1NetworkLinkServiceAssociationApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1NetworkLinkServiceAssociationKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Network Link Service Association
	Spec *NetworkingV1NetworkLinkServiceAssociationSpec `json:"spec,omitempty"`

	// Status The status of the Network Link Service Association
	Status *NetworkingV1NetworkLinkServiceAssociationStatus `json:"status,omitempty"`
}
type NetworkingV1NetworkLinkServiceAssociationApiVersion string
type NetworkingV1NetworkLinkServiceAssociationKind string
type NetworkingV1NetworkLinkServiceAssociationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1NetworkLinkServiceAssociationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1NetworkLinkServiceAssociationListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1NetworkLinkServiceAssociationListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Network Link Service Association
		Status NetworkingV1NetworkLinkServiceAssociationStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1NetworkLinkServiceAssociationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1NetworkLinkServiceAssociationListApiVersion string
type NetworkingV1NetworkLinkServiceAssociationListDataApiVersion string
type NetworkingV1NetworkLinkServiceAssociationListDataKind string
type NetworkingV1NetworkLinkServiceAssociationListKind string
type NetworkingV1NetworkLinkServiceAssociationSpec struct {
	// Description The description of the network link endpoint
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the network link endpoint
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// NetworkLinkEndpoint ID of the Network link endpoint.
	NetworkLinkEndpoint *string `json:"network_link_endpoint,omitempty"`

	// NetworkLinkService The network_link_service to which this belongs.
	NetworkLinkService *EnvScopedObjectReference `json:"network_link_service,omitempty"`
}
type NetworkingV1NetworkLinkServiceAssociationStatus struct {
	// ErrorCode Error code if network link is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if network link is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// ExpiresAt The date and time when the request expires if it is not accepted by the target network admin.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Phase The lifecycle phase of the network link endpoint:
	//
	//   PROVISIONING: network link endpoint provisioning is in progress;
	//
	//   PENDING_ACCEPT: network link endpoint request is pending acceptance by the the owner of the target;
	//
	//   READY:  network link endpoint is ready;
	//
	//   FAILED: network link endpoint is in a failed state;
	//
	//   DEPROVISIONING: network link endpoint deprovisioning is in progress;
	//
	//   EXPIRED: network link endpoint request is expired, can only be deleted;
	//
	//   DISCONNECTED: network link endpoint is in a disconnected state, target owner has removed the permissions;
	//
	//   DISCONNECTING: network link endpoint disconnection is in progress;
	//
	//   INACTIVE: network link endpoint is created, but not active since there are no clusters in the network;
	Phase string `json:"phase"`
}
type NetworkingV1NetworkLinkServiceList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1NetworkLinkServiceListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1NetworkLinkServiceListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1NetworkLinkServiceListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Network Link Service
		Status NetworkingV1NetworkLinkServiceStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1NetworkLinkServiceListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1NetworkLinkServiceListApiVersion string
type NetworkingV1NetworkLinkServiceListDataApiVersion string
type NetworkingV1NetworkLinkServiceListDataKind string
type NetworkingV1NetworkLinkServiceListKind string
type NetworkingV1NetworkLinkServiceSpec struct {
	// Accept Network Link Service Accept policy
	Accept *NetworkingV1NetworkLinkServiceAcceptPolicy `json:"accept,omitempty"`

	// Description The description of the network link service
	Description *string `json:"description,omitempty"`

	// DisplayName The name of the network link service
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *GlobalObjectReference `json:"environment,omitempty"`

	// Network The network to which this belongs.
	Network *EnvScopedObjectReference `json:"network,omitempty"`
}
type NetworkingV1NetworkLinkServiceStatus struct {
	// ErrorCode Error code if network link service is in a failed state.
	// May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if network link service is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the network link service:
	//
	// READY:  network link service is ready;
	Phase string `json:"phase"`
}
type NetworkingV1NetworkList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1NetworkListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1NetworkListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1NetworkListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Network
		Status NetworkingV1NetworkStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1NetworkListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1NetworkListApiVersion string
type NetworkingV1NetworkListDataApiVersion string
type NetworkingV1NetworkListDataKind string
type NetworkingV1NetworkListKind string
type NetworkingV1NetworkSpec struct {
	// Cidr The IPv4 [CIDR block](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) to used for this network.
	// Must be `/16`. Required for VPC peering and AWS TransitGateway.
	Cidr *string `json:"cidr,omitempty"`

	// Cloud The cloud service provider in which the network exists.
	Cloud *string `json:"cloud,omitempty"`

	// ConnectionTypes The connection types requested for use with the network.
	ConnectionTypes *[]NetworkingV1ConnectionType `json:"connection_types,omitempty"`

	// DisplayName The name of the network
	DisplayName *string `json:"display_name,omitempty"`

	// DnsConfig DNS config only applies to PrivateLink network connection type.
	//
	// When resolution is CHASED_PRIVATE, clusters in this network require both public and private DNS
	//  to resolve cluster endpoints.
	//
	// When resolution is PRIVATE, clusters in this network only require private DNS
	//  to resolve cluster endpoints.
	DnsConfig *NetworkingV1DnsConfig `json:"dns_config,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Gateway The gateway associated with this object. The gateway can be one of networking.v1.Gateway. May be `null` or omitted if not associated with a gateway.
	Gateway *TypedEnvScopedObjectReference `json:"gateway,omitempty"`

	// Region The cloud service provider region in which the network exists.
	Region *string `json:"region,omitempty"`

	// ReservedCidr The reserved CIDR config is used only by AWS networks with connection_types = Vpc_Peering or Transit_Gateway
	//
	// An IPv4 [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing)
	//   reserved for Confluent Cloud Network. Must be \24.
	//   If not specified, Confluent Cloud Network uses 172.20.255.0/24
	//
	// Note - The attribute is in a [Limited Availability lifecycle stage](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	ReservedCidr *string `json:"reserved_cidr,omitempty"`

	// Zones The 3 availability zones for this network. They can optionally be specified for AWS networks
	// used with PrivateLink, for GCP networks used with Private Service Connect, and for AWS and GCP
	// networks used with Peering.
	// Otherwise, they are automatically chosen by Confluent Cloud.
	//
	// On AWS, zones are AWS [AZ IDs](https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html)
	//  (e.g. use1-az3)
	//
	// On GCP, zones are GCP [zones](https://cloud.google.com/compute/docs/regions-zones)
	//  (e.g. us-central1-c).
	//
	// On Azure, zones are Confluent-chosen names (e.g. 1, 2, 3) since Azure does not
	//  have universal zone identifiers.
	Zones *[]string `json:"zones,omitempty"`

	// ZonesInfo Each item represents information related to a single zone.
	//
	// Note - The attribute is in a [Limited Availability lifecycle stage](https://docs.confluent.io/cloud/current/api.html#section/Versioning/API-Lifecycle-Policy)
	ZonesInfo *[]NetworkingV1ZoneInfo `json:"zones_info,omitempty"`
}
type NetworkingV1NetworkStatus struct {
	// ActiveConnectionTypes The connection types requested for use with the network.
	ActiveConnectionTypes []NetworkingV1ConnectionType `json:"active_connection_types"`

	// Cloud The cloud-specific network details. These will be populated when the network reaches the READY state.
	Cloud *NetworkingV1NetworkStatus_Cloud `json:"cloud,omitempty"`

	// DnsDomain The root DNS domain for the network if applicable. Present on networks that support PrivateLink.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// EndpointSuffix The endpoint suffix for the network, if applicable. Full service endpoints can be constructed by appending
	// the service identifier to the beginning of the endpoint suffix. For example, the Flink REST endpoint can be
	// constructed by adding "flink" - 'https://flink' + 'endpoint_suffix'.
	EndpointSuffix *string `json:"endpoint_suffix,omitempty"`

	// ErrorCode Error code if network is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if network is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// IdleSince The date and time when the network becomes idle
	IdleSince *time.Time `json:"idle_since,omitempty"`

	// Phase The lifecyle phase of the network:
	//
	// PROVISIONING:  network provisioning is in progress;
	//
	// READY:  network is ready;
	//
	// FAILED: provisioning failed;
	//
	// DEPROVISIONING: network deprovisioning is in progress;
	Phase string `json:"phase"`

	// SupportedConnectionTypes The connection types this network supports.
	SupportedConnectionTypes []NetworkingV1ConnectionType `json:"supported_connection_types"`

	// ZonalSubdomains The DNS subdomain for each zone. Present on networks that support PrivateLink. Keys are zones and
	// values are DNS domains.
	ZonalSubdomains *map[string]string `json:"zonal_subdomains,omitempty"`
}
type NetworkingV1NetworkStatus_Cloud struct {
	union json.RawMessage
}
type NetworkingV1Peering struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1PeeringApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1PeeringKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Peering
	Spec *NetworkingV1PeeringSpec `json:"spec,omitempty"`

	// Status The status of the Peering
	Status *NetworkingV1PeeringStatus `json:"status,omitempty"`
}
type NetworkingV1PeeringApiVersion string
type NetworkingV1PeeringKind string
type NetworkingV1PeeringList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1PeeringListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1PeeringListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1PeeringListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Peering
		Status NetworkingV1PeeringStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1PeeringListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1PeeringListApiVersion string
type NetworkingV1PeeringListDataApiVersion string
type NetworkingV1PeeringListDataKind string
type NetworkingV1PeeringListKind string
type NetworkingV1PeeringSpec struct {
	// Cloud The cloud-specific peering details.
	Cloud *NetworkingV1PeeringSpec_Cloud `json:"cloud,omitempty"`

	// DisplayName The name of the peering
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Network The network to which this belongs.
	Network *ObjectReference `json:"network,omitempty"`
}
type NetworkingV1PeeringSpec_Cloud struct {
	union json.RawMessage
}
type NetworkingV1PeeringStatus struct {
	// ErrorCode Error code if peering is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if peering is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the peering:
	//
	//   PROVISIONING: peering provisioning is in progress;
	//
	//   PENDING_ACCEPT: peering connection request is pending acceptance by the customer;
	//
	//   READY:  peering is ready;
	//
	//   FAILED: peering is in a failed state;
	//
	//   DEPROVISIONING: peering deprovisioning is in progress;
	//
	//   DISCONNECTED: peering has been disconnected in the cloud provider by the customer;
	Phase string `json:"phase"`
}
type NetworkingV1PrivateLinkAccess struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1PrivateLinkAccessApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1PrivateLinkAccessKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Private Link Access
	Spec *NetworkingV1PrivateLinkAccessSpec `json:"spec,omitempty"`

	// Status The status of the Private Link Access
	Status *NetworkingV1PrivateLinkAccessStatus `json:"status,omitempty"`
}
type NetworkingV1PrivateLinkAccessApiVersion string
type NetworkingV1PrivateLinkAccessKind string
type NetworkingV1PrivateLinkAccessList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1PrivateLinkAccessListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1PrivateLinkAccessListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1PrivateLinkAccessListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Private Link Access
		Status NetworkingV1PrivateLinkAccessStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1PrivateLinkAccessListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1PrivateLinkAccessListApiVersion string
type NetworkingV1PrivateLinkAccessListDataApiVersion string
type NetworkingV1PrivateLinkAccessListDataKind string
type NetworkingV1PrivateLinkAccessListKind string
type NetworkingV1PrivateLinkAccessPoint struct {
	// Kind PrivateLinkAccessPoint kind.
	Kind NetworkingV1PrivateLinkAccessPointKind `json:"kind"`

	// ResourceId ID of the target resource.
	ResourceId string `json:"resource_id"`
}
type NetworkingV1PrivateLinkAccessPointKind string
type NetworkingV1PrivateLinkAccessSpec struct {
	// Cloud The cloud-specific PrivateLink details.
	Cloud *NetworkingV1PrivateLinkAccessSpec_Cloud `json:"cloud,omitempty"`

	// DisplayName The name of the PrivateLink access
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Network The network to which this belongs.
	Network *ObjectReference `json:"network,omitempty"`
}
type NetworkingV1PrivateLinkAccessSpec_Cloud struct {
	union json.RawMessage
}
type NetworkingV1PrivateLinkAccessStatus struct {
	// ErrorCode Error code if PrivateLink access is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if PrivateLink access is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the PrivateLink access configuration:
	//
	//   PROVISIONING: PrivateLink access provisioning is in progress;
	//
	//   READY:  PrivateLink access is ready;
	//
	//   FAILED: PrivateLink access is in a failed state;
	//
	//   DEPROVISIONING: PrivateLink access deprovisioning is in progress;
	Phase string `json:"phase"`
}
type NetworkingV1PrivateLinkAttachment struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1PrivateLinkAttachmentApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1PrivateLinkAttachmentKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Private Link Attachment
	Spec *NetworkingV1PrivateLinkAttachmentSpec `json:"spec,omitempty"`

	// Status The status of the Private Link Attachment
	Status *NetworkingV1PrivateLinkAttachmentStatus `json:"status,omitempty"`
}
type NetworkingV1PrivateLinkAttachmentApiVersion string
type NetworkingV1PrivateLinkAttachmentKind string
type NetworkingV1PrivateLinkAttachmentConnection struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1PrivateLinkAttachmentConnectionApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1PrivateLinkAttachmentConnectionKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Private Link Attachment Connection
	Spec *NetworkingV1PrivateLinkAttachmentConnectionSpec `json:"spec,omitempty"`

	// Status The status of the Private Link Attachment Connection
	Status *NetworkingV1PrivateLinkAttachmentConnectionStatus `json:"status,omitempty"`
}
type NetworkingV1PrivateLinkAttachmentConnectionApiVersion string
type NetworkingV1PrivateLinkAttachmentConnectionKind string
type NetworkingV1PrivateLinkAttachmentConnectionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1PrivateLinkAttachmentConnectionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1PrivateLinkAttachmentConnectionListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Private Link Attachment Connection
		Status NetworkingV1PrivateLinkAttachmentConnectionStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1PrivateLinkAttachmentConnectionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1PrivateLinkAttachmentConnectionListApiVersion string
type NetworkingV1PrivateLinkAttachmentConnectionListDataApiVersion string
type NetworkingV1PrivateLinkAttachmentConnectionListDataKind string
type NetworkingV1PrivateLinkAttachmentConnectionListKind string
type NetworkingV1PrivateLinkAttachmentConnectionSpec struct {
	// Cloud The cloud-specific PrivateLink attachment connection details.
	Cloud *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud `json:"cloud,omitempty"`

	// DisplayName The name of the PrivateLink attachment connection.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// PrivateLinkAttachment The private_link_attachment to which this belongs.
	PrivateLinkAttachment *ObjectReference `json:"private_link_attachment,omitempty"`
}
type NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud struct {
	union json.RawMessage
}
type NetworkingV1PrivateLinkAttachmentConnectionStatus struct {
	// Cloud The cloud specific status of the PrivateLink attachment connection.
	Cloud *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud `json:"cloud,omitempty"`

	// ErrorCode Error code if PrivateLink attachment connection is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if PrivateLink attachment connection is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the PrivateLink attachment:
	//
	//   PROVISIONING: PrivateLink attachment connection provisioning is in progress;
	//
	//   READY: PrivateLink attachment connection is ready;
	//
	//   FAILED: PrivateLink attachment connection is in a failed state;
	//
	//   DEPROVISIONING: PrivateLink attachment connection deprovisioning is in progress;
	//
	//   DISCONNECTED:|
	//     PrivateLink attachment connection is in a disconnected state. This means the
	//     private endpoint associated with this PrivateLink attachment connection has been deleted;
	Phase string `json:"phase"`
}
type NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud struct {
	union json.RawMessage
}
type NetworkingV1PrivateLinkAttachmentList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1PrivateLinkAttachmentListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1PrivateLinkAttachmentListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1PrivateLinkAttachmentListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Private Link Attachment
		Status NetworkingV1PrivateLinkAttachmentStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1PrivateLinkAttachmentListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1PrivateLinkAttachmentListApiVersion string
type NetworkingV1PrivateLinkAttachmentListDataApiVersion string
type NetworkingV1PrivateLinkAttachmentListDataKind string
type NetworkingV1PrivateLinkAttachmentListKind string
type NetworkingV1PrivateLinkAttachmentSpec struct {
	// Cloud The cloud service provider that hosts the resources to access with the PrivateLink attachment.
	Cloud *string `json:"cloud,omitempty"`

	// DisplayName The name of the PrivateLink attachment.
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Region The cloud service provider region where the resources to be accessed
	// using the PrivateLink attachment are located.
	Region *string `json:"region,omitempty"`
}
type NetworkingV1PrivateLinkAttachmentStatus struct {
	// Cloud The cloud specific status of the PrivateLink attachment. These will be populated when the PrivateLink attachment reaches the WAITING_FOR_CONNECTIONS state.
	Cloud *NetworkingV1PrivateLinkAttachmentStatus_Cloud `json:"cloud,omitempty"`

	// DnsDomain The root DNS domain for the PrivateLink attachment.
	DnsDomain *string `json:"dns_domain,omitempty"`

	// ErrorCode Error code if PrivateLink attachment is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if PrivateLink attachment is in a failed state.
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the PrivateLink attachment:
	//
	//   PROVISIONING: PrivateLink attachment provisioning is in progress;
	//
	//   WAITING_FOR_CONNECTIONS: PrivateLink attachment is waiting for connections;
	//
	//   READY: PrivateLink attachment is ready;
	//
	//   FAILED: PrivateLink attachment is in a failed state;
	//
	//   EXPIRED: PrivateLink attachment has timed out waiting for connections, can only be deleted;
	//
	//   DEPROVISIONING: PrivateLink attachment deprovisioning is in progress;
	Phase string `json:"phase"`
}
type NetworkingV1PrivateLinkAttachmentStatus_Cloud struct {
	union json.RawMessage
}
type NetworkingV1TransitGatewayAttachment struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NetworkingV1TransitGatewayAttachmentApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NetworkingV1TransitGatewayAttachmentKind `json:"kind,omitempty"`
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

	// Spec The desired state of the Transit Gateway Attachment
	Spec *NetworkingV1TransitGatewayAttachmentSpec `json:"spec,omitempty"`

	// Status The status of the Transit Gateway Attachment
	Status *NetworkingV1TransitGatewayAttachmentStatus `json:"status,omitempty"`
}
type NetworkingV1TransitGatewayAttachmentApiVersion string
type NetworkingV1TransitGatewayAttachmentKind string
type NetworkingV1TransitGatewayAttachmentList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NetworkingV1TransitGatewayAttachmentListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NetworkingV1TransitGatewayAttachmentListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NetworkingV1TransitGatewayAttachmentListDataKind `json:"kind,omitempty"`
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

		// Status The status of the Transit Gateway Attachment
		Status NetworkingV1TransitGatewayAttachmentStatus `json:"status"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NetworkingV1TransitGatewayAttachmentListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NetworkingV1TransitGatewayAttachmentListApiVersion string
type NetworkingV1TransitGatewayAttachmentListDataApiVersion string
type NetworkingV1TransitGatewayAttachmentListDataKind string
type NetworkingV1TransitGatewayAttachmentListKind string
type NetworkingV1TransitGatewayAttachmentSpec struct {
	// Cloud The cloud-specific Transit Gateway details.
	Cloud *NetworkingV1TransitGatewayAttachmentSpec_Cloud `json:"cloud,omitempty"`

	// DisplayName The name of the TGW attachment
	DisplayName *string `json:"display_name,omitempty"`

	// Environment The environment to which this belongs.
	Environment *ObjectReference `json:"environment,omitempty"`

	// Network The network to which this belongs.
	Network *ObjectReference `json:"network,omitempty"`
}
type NetworkingV1TransitGatewayAttachmentSpec_Cloud struct {
	union json.RawMessage
}
type NetworkingV1TransitGatewayAttachmentStatus struct {
	// Cloud The cloud-specific TGW attachment details.
	Cloud *NetworkingV1TransitGatewayAttachmentStatus_Cloud `json:"cloud,omitempty"`

	// ErrorCode Error code if TGW attachment is in a failed state. May be used for programmatic error checking.
	ErrorCode *string `json:"error_code,omitempty"`

	// ErrorMessage Displayable error message if TGW attachment is in a failed state
	ErrorMessage *string `json:"error_message,omitempty"`

	// Phase The lifecycle phase of the TGW attachment:
	//
	//   PROVISIONING: attachment provisioning is in progress;
	//
	//   PENDING_ACCEPT: attachment request is pending acceptance by the customer;
	//
	//   READY:  attachment is ready;
	//
	//   FAILED: attachment is in a failed state;
	//
	//   DEPROVISIONING: attachment deprovisioning is in progress;
	//
	//   DISCONNECTED: attachment was manually deleted directly in the cloud provider by the customer;
	//
	//   ERROR: invalid customer input during attachment creation.
	Phase string `json:"phase"`
}
type NetworkingV1TransitGatewayAttachmentStatus_Cloud struct {
	union json.RawMessage
}
type NetworkingV1ZoneInfo struct {
	// Cidr The IPv4 [CIDR block](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) to used for this network.
	// Must be a `/27`. Required for VPC peering and AWS TransitGateway.
	Cidr *string `json:"cidr,omitempty"`

	// ZoneId Cloud provider zone id
	ZoneId *string `json:"zone_id,omitempty"`
}
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
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1AwsEgressPrivateLinkEndpoint() (NetworkingV1AwsEgressPrivateLinkEndpoint, error) {
	var body NetworkingV1AwsEgressPrivateLinkEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1AwsEgressPrivateLinkEndpoint(v NetworkingV1AwsEgressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1AwsEgressPrivateLinkEndpoint(v NetworkingV1AwsEgressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1AwsIngressPrivateLinkEndpoint() (NetworkingV1AwsIngressPrivateLinkEndpoint, error) {
	var body NetworkingV1AwsIngressPrivateLinkEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1AwsIngressPrivateLinkEndpoint(v NetworkingV1AwsIngressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1AwsIngressPrivateLinkEndpoint(v NetworkingV1AwsIngressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1AzureEgressPrivateLinkEndpoint() (NetworkingV1AzureEgressPrivateLinkEndpoint, error) {
	var body NetworkingV1AzureEgressPrivateLinkEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1AzureEgressPrivateLinkEndpoint(v NetworkingV1AzureEgressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1AzureEgressPrivateLinkEndpoint(v NetworkingV1AzureEgressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1AzureIngressPrivateLinkEndpoint() (NetworkingV1AzureIngressPrivateLinkEndpoint, error) {
	var body NetworkingV1AzureIngressPrivateLinkEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1AzureIngressPrivateLinkEndpoint(v NetworkingV1AzureIngressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1AzureIngressPrivateLinkEndpoint(v NetworkingV1AzureIngressPrivateLinkEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1AwsPrivateNetworkInterface() (NetworkingV1AwsPrivateNetworkInterface, error) {
	var body NetworkingV1AwsPrivateNetworkInterface
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1AwsPrivateNetworkInterface(v NetworkingV1AwsPrivateNetworkInterface) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterface"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1AwsPrivateNetworkInterface(v NetworkingV1AwsPrivateNetworkInterface) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterface"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1GcpEgressPrivateServiceConnectEndpoint() (NetworkingV1GcpEgressPrivateServiceConnectEndpoint, error) {
	var body NetworkingV1GcpEgressPrivateServiceConnectEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1GcpEgressPrivateServiceConnectEndpoint(v NetworkingV1GcpEgressPrivateServiceConnectEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1GcpEgressPrivateServiceConnectEndpoint(v NetworkingV1GcpEgressPrivateServiceConnectEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) AsNetworkingV1GcpIngressPrivateServiceConnectEndpoint() (NetworkingV1GcpIngressPrivateServiceConnectEndpoint, error) {
	var body NetworkingV1GcpIngressPrivateServiceConnectEndpoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointSpec_Config) FromNetworkingV1GcpIngressPrivateServiceConnectEndpoint(v NetworkingV1GcpIngressPrivateServiceConnectEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectEndpoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointSpec_Config) MergeNetworkingV1GcpIngressPrivateServiceConnectEndpoint(v NetworkingV1GcpIngressPrivateServiceConnectEndpoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectEndpoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1AccessPointSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsEgressPrivateLinkEndpoint":
		return t.AsNetworkingV1AwsEgressPrivateLinkEndpoint()
	case "AwsIngressPrivateLinkEndpoint":
		return t.AsNetworkingV1AwsIngressPrivateLinkEndpoint()
	case "AwsPrivateNetworkInterface":
		return t.AsNetworkingV1AwsPrivateNetworkInterface()
	case "AzureEgressPrivateLinkEndpoint":
		return t.AsNetworkingV1AzureEgressPrivateLinkEndpoint()
	case "AzureIngressPrivateLinkEndpoint":
		return t.AsNetworkingV1AzureIngressPrivateLinkEndpoint()
	case "GcpEgressPrivateServiceConnectEndpoint":
		return t.AsNetworkingV1GcpEgressPrivateServiceConnectEndpoint()
	case "GcpIngressPrivateServiceConnectEndpoint":
		return t.AsNetworkingV1GcpIngressPrivateServiceConnectEndpoint()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1AccessPointSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1AccessPointSpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1AwsEgressPrivateLinkEndpointStatus() (NetworkingV1AwsEgressPrivateLinkEndpointStatus, error) {
	var body NetworkingV1AwsEgressPrivateLinkEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1AwsEgressPrivateLinkEndpointStatus(v NetworkingV1AwsEgressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1AwsEgressPrivateLinkEndpointStatus(v NetworkingV1AwsEgressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1AwsIngressPrivateLinkEndpointStatus() (NetworkingV1AwsIngressPrivateLinkEndpointStatus, error) {
	var body NetworkingV1AwsIngressPrivateLinkEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1AwsIngressPrivateLinkEndpointStatus(v NetworkingV1AwsIngressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1AwsIngressPrivateLinkEndpointStatus(v NetworkingV1AwsIngressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1AzureEgressPrivateLinkEndpointStatus() (NetworkingV1AzureEgressPrivateLinkEndpointStatus, error) {
	var body NetworkingV1AzureEgressPrivateLinkEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1AzureEgressPrivateLinkEndpointStatus(v NetworkingV1AzureEgressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1AzureEgressPrivateLinkEndpointStatus(v NetworkingV1AzureEgressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1AzureIngressPrivateLinkEndpointStatus() (NetworkingV1AzureIngressPrivateLinkEndpointStatus, error) {
	var body NetworkingV1AzureIngressPrivateLinkEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1AzureIngressPrivateLinkEndpointStatus(v NetworkingV1AzureIngressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1AzureIngressPrivateLinkEndpointStatus(v NetworkingV1AzureIngressPrivateLinkEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1GcpEgressPrivateServiceConnectEndpointStatus() (NetworkingV1GcpEgressPrivateServiceConnectEndpointStatus, error) {
	var body NetworkingV1GcpEgressPrivateServiceConnectEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1GcpEgressPrivateServiceConnectEndpointStatus(v NetworkingV1GcpEgressPrivateServiceConnectEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1GcpEgressPrivateServiceConnectEndpointStatus(v NetworkingV1GcpEgressPrivateServiceConnectEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) AsNetworkingV1GcpIngressPrivateServiceConnectEndpointStatus() (NetworkingV1GcpIngressPrivateServiceConnectEndpointStatus, error) {
	var body NetworkingV1GcpIngressPrivateServiceConnectEndpointStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1AccessPointStatus_Config) FromNetworkingV1GcpIngressPrivateServiceConnectEndpointStatus(v NetworkingV1GcpIngressPrivateServiceConnectEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectEndpointStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1AccessPointStatus_Config) MergeNetworkingV1GcpIngressPrivateServiceConnectEndpointStatus(v NetworkingV1GcpIngressPrivateServiceConnectEndpointStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectEndpointStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1AccessPointStatus_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1AccessPointStatus_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsEgressPrivateLinkEndpointStatus":
		return t.AsNetworkingV1AwsEgressPrivateLinkEndpointStatus()
	case "AwsIngressPrivateLinkEndpointStatus":
		return t.AsNetworkingV1AwsIngressPrivateLinkEndpointStatus()
	case "AzureEgressPrivateLinkEndpointStatus":
		return t.AsNetworkingV1AzureEgressPrivateLinkEndpointStatus()
	case "AzureIngressPrivateLinkEndpointStatus":
		return t.AsNetworkingV1AzureIngressPrivateLinkEndpointStatus()
	case "GcpEgressPrivateServiceConnectEndpointStatus":
		return t.AsNetworkingV1GcpEgressPrivateServiceConnectEndpointStatus()
	case "GcpIngressPrivateServiceConnectEndpointStatus":
		return t.AsNetworkingV1GcpIngressPrivateServiceConnectEndpointStatus()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1AccessPointStatus_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1AccessPointStatus_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1DnsForwarderSpec_Config) AsNetworkingV1ForwardViaIp() (NetworkingV1ForwardViaIp, error) {
	var body NetworkingV1ForwardViaIp
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1DnsForwarderSpec_Config) FromNetworkingV1ForwardViaIp(v NetworkingV1ForwardViaIp) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"ForwardViaIp"}`))
	t.union = b
	return err
}
func (t *NetworkingV1DnsForwarderSpec_Config) MergeNetworkingV1ForwardViaIp(v NetworkingV1ForwardViaIp) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"ForwardViaIp"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1DnsForwarderSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1DnsForwarderSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "ForwardViaIp":
		return t.AsNetworkingV1ForwardViaIp()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1DnsForwarderSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1DnsForwarderSpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1DnsRecordSpec_Config) AsNetworkingV1PrivateLinkAccessPoint() (NetworkingV1PrivateLinkAccessPoint, error) {
	var body NetworkingV1PrivateLinkAccessPoint
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1DnsRecordSpec_Config) FromNetworkingV1PrivateLinkAccessPoint(v NetworkingV1PrivateLinkAccessPoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"PrivateLinkAccessPoint"}`))
	t.union = b
	return err
}
func (t *NetworkingV1DnsRecordSpec_Config) MergeNetworkingV1PrivateLinkAccessPoint(v NetworkingV1PrivateLinkAccessPoint) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"PrivateLinkAccessPoint"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1DnsRecordSpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1DnsRecordSpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "PrivateLinkAccessPoint":
		return t.AsNetworkingV1PrivateLinkAccessPoint()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1DnsRecordSpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1DnsRecordSpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AwsEgressPrivateLinkGatewaySpec() (NetworkingV1AwsEgressPrivateLinkGatewaySpec, error) {
	var body NetworkingV1AwsEgressPrivateLinkGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AwsEgressPrivateLinkGatewaySpec(v NetworkingV1AwsEgressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AwsEgressPrivateLinkGatewaySpec(v NetworkingV1AwsEgressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AwsPrivateNetworkInterfaceGatewaySpec() (NetworkingV1AwsPrivateNetworkInterfaceGatewaySpec, error) {
	var body NetworkingV1AwsPrivateNetworkInterfaceGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AwsPrivateNetworkInterfaceGatewaySpec(v NetworkingV1AwsPrivateNetworkInterfaceGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterfaceGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AwsPrivateNetworkInterfaceGatewaySpec(v NetworkingV1AwsPrivateNetworkInterfaceGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterfaceGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AwsIngressPrivateLinkGatewaySpec() (NetworkingV1AwsIngressPrivateLinkGatewaySpec, error) {
	var body NetworkingV1AwsIngressPrivateLinkGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AwsIngressPrivateLinkGatewaySpec(v NetworkingV1AwsIngressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AwsIngressPrivateLinkGatewaySpec(v NetworkingV1AwsIngressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AwsPeeringGatewaySpec() (NetworkingV1AwsPeeringGatewaySpec, error) {
	var body NetworkingV1AwsPeeringGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AwsPeeringGatewaySpec(v NetworkingV1AwsPeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPeeringGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AwsPeeringGatewaySpec(v NetworkingV1AwsPeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPeeringGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AzurePeeringGatewaySpec() (NetworkingV1AzurePeeringGatewaySpec, error) {
	var body NetworkingV1AzurePeeringGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AzurePeeringGatewaySpec(v NetworkingV1AzurePeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePeeringGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AzurePeeringGatewaySpec(v NetworkingV1AzurePeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePeeringGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AzureEgressPrivateLinkGatewaySpec() (NetworkingV1AzureEgressPrivateLinkGatewaySpec, error) {
	var body NetworkingV1AzureEgressPrivateLinkGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AzureEgressPrivateLinkGatewaySpec(v NetworkingV1AzureEgressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AzureEgressPrivateLinkGatewaySpec(v NetworkingV1AzureEgressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1AzureIngressPrivateLinkGatewaySpec() (NetworkingV1AzureIngressPrivateLinkGatewaySpec, error) {
	var body NetworkingV1AzureIngressPrivateLinkGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1AzureIngressPrivateLinkGatewaySpec(v NetworkingV1AzureIngressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1AzureIngressPrivateLinkGatewaySpec(v NetworkingV1AzureIngressPrivateLinkGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1GcpPeeringGatewaySpec() (NetworkingV1GcpPeeringGatewaySpec, error) {
	var body NetworkingV1GcpPeeringGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1GcpPeeringGatewaySpec(v NetworkingV1GcpPeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeeringGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1GcpPeeringGatewaySpec(v NetworkingV1GcpPeeringGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeeringGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1GcpEgressPrivateServiceConnectGatewaySpec() (NetworkingV1GcpEgressPrivateServiceConnectGatewaySpec, error) {
	var body NetworkingV1GcpEgressPrivateServiceConnectGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1GcpEgressPrivateServiceConnectGatewaySpec(v NetworkingV1GcpEgressPrivateServiceConnectGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1GcpEgressPrivateServiceConnectGatewaySpec(v NetworkingV1GcpEgressPrivateServiceConnectGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) AsNetworkingV1GcpIngressPrivateServiceConnectGatewaySpec() (NetworkingV1GcpIngressPrivateServiceConnectGatewaySpec, error) {
	var body NetworkingV1GcpIngressPrivateServiceConnectGatewaySpec
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewaySpec_Config) FromNetworkingV1GcpIngressPrivateServiceConnectGatewaySpec(v NetworkingV1GcpIngressPrivateServiceConnectGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectGatewaySpec"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewaySpec_Config) MergeNetworkingV1GcpIngressPrivateServiceConnectGatewaySpec(v NetworkingV1GcpIngressPrivateServiceConnectGatewaySpec) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectGatewaySpec"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewaySpec_Config) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1GatewaySpec_Config) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsEgressPrivateLinkGatewaySpec":
		return t.AsNetworkingV1AwsEgressPrivateLinkGatewaySpec()
	case "AwsIngressPrivateLinkGatewaySpec":
		return t.AsNetworkingV1AwsIngressPrivateLinkGatewaySpec()
	case "AwsPeeringGatewaySpec":
		return t.AsNetworkingV1AwsPeeringGatewaySpec()
	case "AwsPrivateNetworkInterfaceGatewaySpec":
		return t.AsNetworkingV1AwsPrivateNetworkInterfaceGatewaySpec()
	case "AzureEgressPrivateLinkGatewaySpec":
		return t.AsNetworkingV1AzureEgressPrivateLinkGatewaySpec()
	case "AzureIngressPrivateLinkGatewaySpec":
		return t.AsNetworkingV1AzureIngressPrivateLinkGatewaySpec()
	case "AzurePeeringGatewaySpec":
		return t.AsNetworkingV1AzurePeeringGatewaySpec()
	case "GcpEgressPrivateServiceConnectGatewaySpec":
		return t.AsNetworkingV1GcpEgressPrivateServiceConnectGatewaySpec()
	case "GcpIngressPrivateServiceConnectGatewaySpec":
		return t.AsNetworkingV1GcpIngressPrivateServiceConnectGatewaySpec()
	case "GcpPeeringGatewaySpec":
		return t.AsNetworkingV1GcpPeeringGatewaySpec()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1GatewaySpec_Config) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1GatewaySpec_Config) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1AwsEgressPrivateLinkGatewayStatus() (NetworkingV1AwsEgressPrivateLinkGatewayStatus, error) {
	var body NetworkingV1AwsEgressPrivateLinkGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1AwsEgressPrivateLinkGatewayStatus(v NetworkingV1AwsEgressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1AwsEgressPrivateLinkGatewayStatus(v NetworkingV1AwsEgressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsEgressPrivateLinkGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1AwsIngressPrivateLinkGatewayStatus() (NetworkingV1AwsIngressPrivateLinkGatewayStatus, error) {
	var body NetworkingV1AwsIngressPrivateLinkGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1AwsIngressPrivateLinkGatewayStatus(v NetworkingV1AwsIngressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1AwsIngressPrivateLinkGatewayStatus(v NetworkingV1AwsIngressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsIngressPrivateLinkGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1AwsPrivateNetworkInterfaceGatewayStatus() (NetworkingV1AwsPrivateNetworkInterfaceGatewayStatus, error) {
	var body NetworkingV1AwsPrivateNetworkInterfaceGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1AwsPrivateNetworkInterfaceGatewayStatus(v NetworkingV1AwsPrivateNetworkInterfaceGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterfaceGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1AwsPrivateNetworkInterfaceGatewayStatus(v NetworkingV1AwsPrivateNetworkInterfaceGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateNetworkInterfaceGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1AzureEgressPrivateLinkGatewayStatus() (NetworkingV1AzureEgressPrivateLinkGatewayStatus, error) {
	var body NetworkingV1AzureEgressPrivateLinkGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1AzureEgressPrivateLinkGatewayStatus(v NetworkingV1AzureEgressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1AzureEgressPrivateLinkGatewayStatus(v NetworkingV1AzureEgressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureEgressPrivateLinkGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1AzureIngressPrivateLinkGatewayStatus() (NetworkingV1AzureIngressPrivateLinkGatewayStatus, error) {
	var body NetworkingV1AzureIngressPrivateLinkGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1AzureIngressPrivateLinkGatewayStatus(v NetworkingV1AzureIngressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1AzureIngressPrivateLinkGatewayStatus(v NetworkingV1AzureIngressPrivateLinkGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureIngressPrivateLinkGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1GcpPeeringGatewayStatus() (NetworkingV1GcpPeeringGatewayStatus, error) {
	var body NetworkingV1GcpPeeringGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1GcpPeeringGatewayStatus(v NetworkingV1GcpPeeringGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeeringGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1GcpPeeringGatewayStatus(v NetworkingV1GcpPeeringGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeeringGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1GcpEgressPrivateServiceConnectGatewayStatus() (NetworkingV1GcpEgressPrivateServiceConnectGatewayStatus, error) {
	var body NetworkingV1GcpEgressPrivateServiceConnectGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1GcpEgressPrivateServiceConnectGatewayStatus(v NetworkingV1GcpEgressPrivateServiceConnectGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1GcpEgressPrivateServiceConnectGatewayStatus(v NetworkingV1GcpEgressPrivateServiceConnectGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpEgressPrivateServiceConnectGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) AsNetworkingV1GcpIngressPrivateServiceConnectGatewayStatus() (NetworkingV1GcpIngressPrivateServiceConnectGatewayStatus, error) {
	var body NetworkingV1GcpIngressPrivateServiceConnectGatewayStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) FromNetworkingV1GcpIngressPrivateServiceConnectGatewayStatus(v NetworkingV1GcpIngressPrivateServiceConnectGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectGatewayStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) MergeNetworkingV1GcpIngressPrivateServiceConnectGatewayStatus(v NetworkingV1GcpIngressPrivateServiceConnectGatewayStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpIngressPrivateServiceConnectGatewayStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1GatewayStatus_CloudGateway) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1GatewayStatus_CloudGateway) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsEgressPrivateLinkGatewayStatus":
		return t.AsNetworkingV1AwsEgressPrivateLinkGatewayStatus()
	case "AwsIngressPrivateLinkGatewayStatus":
		return t.AsNetworkingV1AwsIngressPrivateLinkGatewayStatus()
	case "AwsPrivateNetworkInterfaceGatewayStatus":
		return t.AsNetworkingV1AwsPrivateNetworkInterfaceGatewayStatus()
	case "AzureEgressPrivateLinkGatewayStatus":
		return t.AsNetworkingV1AzureEgressPrivateLinkGatewayStatus()
	case "AzureIngressPrivateLinkGatewayStatus":
		return t.AsNetworkingV1AzureIngressPrivateLinkGatewayStatus()
	case "GcpEgressPrivateServiceConnectGatewayStatus":
		return t.AsNetworkingV1GcpEgressPrivateServiceConnectGatewayStatus()
	case "GcpIngressPrivateServiceConnectGatewayStatus":
		return t.AsNetworkingV1GcpIngressPrivateServiceConnectGatewayStatus()
	case "GcpPeeringGatewayStatus":
		return t.AsNetworkingV1GcpPeeringGatewayStatus()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1GatewayStatus_CloudGateway) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1GatewayStatus_CloudGateway) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1NetworkStatus_Cloud) AsNetworkingV1AwsNetwork() (NetworkingV1AwsNetwork, error) {
	var body NetworkingV1AwsNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1NetworkStatus_Cloud) FromNetworkingV1AwsNetwork(v NetworkingV1AwsNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsNetwork"}`))
	t.union = b
	return err
}
func (t *NetworkingV1NetworkStatus_Cloud) MergeNetworkingV1AwsNetwork(v NetworkingV1AwsNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1NetworkStatus_Cloud) AsNetworkingV1GcpNetwork() (NetworkingV1GcpNetwork, error) {
	var body NetworkingV1GcpNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1NetworkStatus_Cloud) FromNetworkingV1GcpNetwork(v NetworkingV1GcpNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpNetwork"}`))
	t.union = b
	return err
}
func (t *NetworkingV1NetworkStatus_Cloud) MergeNetworkingV1GcpNetwork(v NetworkingV1GcpNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1NetworkStatus_Cloud) AsNetworkingV1AzureNetwork() (NetworkingV1AzureNetwork, error) {
	var body NetworkingV1AzureNetwork
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1NetworkStatus_Cloud) FromNetworkingV1AzureNetwork(v NetworkingV1AzureNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureNetwork"}`))
	t.union = b
	return err
}
func (t *NetworkingV1NetworkStatus_Cloud) MergeNetworkingV1AzureNetwork(v NetworkingV1AzureNetwork) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzureNetwork"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1NetworkStatus_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1NetworkStatus_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsNetwork":
		return t.AsNetworkingV1AwsNetwork()
	case "AzureNetwork":
		return t.AsNetworkingV1AzureNetwork()
	case "GcpNetwork":
		return t.AsNetworkingV1GcpNetwork()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1NetworkStatus_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1NetworkStatus_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1PeeringSpec_Cloud) AsNetworkingV1AwsPeering() (NetworkingV1AwsPeering, error) {
	var body NetworkingV1AwsPeering
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PeeringSpec_Cloud) FromNetworkingV1AwsPeering(v NetworkingV1AwsPeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPeering"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PeeringSpec_Cloud) MergeNetworkingV1AwsPeering(v NetworkingV1AwsPeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPeering"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PeeringSpec_Cloud) AsNetworkingV1GcpPeering() (NetworkingV1GcpPeering, error) {
	var body NetworkingV1GcpPeering
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PeeringSpec_Cloud) FromNetworkingV1GcpPeering(v NetworkingV1GcpPeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeering"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PeeringSpec_Cloud) MergeNetworkingV1GcpPeering(v NetworkingV1GcpPeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPeering"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PeeringSpec_Cloud) AsNetworkingV1AzurePeering() (NetworkingV1AzurePeering, error) {
	var body NetworkingV1AzurePeering
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PeeringSpec_Cloud) FromNetworkingV1AzurePeering(v NetworkingV1AzurePeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePeering"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PeeringSpec_Cloud) MergeNetworkingV1AzurePeering(v NetworkingV1AzurePeering) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePeering"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PeeringSpec_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1PeeringSpec_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsPeering":
		return t.AsNetworkingV1AwsPeering()
	case "AzurePeering":
		return t.AsNetworkingV1AzurePeering()
	case "GcpPeering":
		return t.AsNetworkingV1GcpPeering()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1PeeringSpec_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1PeeringSpec_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) AsNetworkingV1AwsPrivateLinkAccess() (NetworkingV1AwsPrivateLinkAccess, error) {
	var body NetworkingV1AwsPrivateLinkAccess
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) FromNetworkingV1AwsPrivateLinkAccess(v NetworkingV1AwsPrivateLinkAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAccess"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) MergeNetworkingV1AwsPrivateLinkAccess(v NetworkingV1AwsPrivateLinkAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAccess"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) AsNetworkingV1AzurePrivateLinkAccess() (NetworkingV1AzurePrivateLinkAccess, error) {
	var body NetworkingV1AzurePrivateLinkAccess
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) FromNetworkingV1AzurePrivateLinkAccess(v NetworkingV1AzurePrivateLinkAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAccess"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) MergeNetworkingV1AzurePrivateLinkAccess(v NetworkingV1AzurePrivateLinkAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAccess"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) AsNetworkingV1GcpPrivateServiceConnectAccess() (NetworkingV1GcpPrivateServiceConnectAccess, error) {
	var body NetworkingV1GcpPrivateServiceConnectAccess
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) FromNetworkingV1GcpPrivateServiceConnectAccess(v NetworkingV1GcpPrivateServiceConnectAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateServiceConnectAccess"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) MergeNetworkingV1GcpPrivateServiceConnectAccess(v NetworkingV1GcpPrivateServiceConnectAccess) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateServiceConnectAccess"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsPrivateLinkAccess":
		return t.AsNetworkingV1AwsPrivateLinkAccess()
	case "AzurePrivateLinkAccess":
		return t.AsNetworkingV1AzurePrivateLinkAccess()
	case "GcpPrivateServiceConnectAccess":
		return t.AsNetworkingV1GcpPrivateServiceConnectAccess()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1PrivateLinkAccessSpec_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1PrivateLinkAccessSpec_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) AsNetworkingV1AwsPrivateLinkAttachmentConnection() (NetworkingV1AwsPrivateLinkAttachmentConnection, error) {
	var body NetworkingV1AwsPrivateLinkAttachmentConnection
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) FromNetworkingV1AwsPrivateLinkAttachmentConnection(v NetworkingV1AwsPrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentConnection"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) MergeNetworkingV1AwsPrivateLinkAttachmentConnection(v NetworkingV1AwsPrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentConnection"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) AsNetworkingV1AzurePrivateLinkAttachmentConnection() (NetworkingV1AzurePrivateLinkAttachmentConnection, error) {
	var body NetworkingV1AzurePrivateLinkAttachmentConnection
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) FromNetworkingV1AzurePrivateLinkAttachmentConnection(v NetworkingV1AzurePrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentConnection"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) MergeNetworkingV1AzurePrivateLinkAttachmentConnection(v NetworkingV1AzurePrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentConnection"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) AsNetworkingV1GcpPrivateLinkAttachmentConnection() (NetworkingV1GcpPrivateLinkAttachmentConnection, error) {
	var body NetworkingV1GcpPrivateLinkAttachmentConnection
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) FromNetworkingV1GcpPrivateLinkAttachmentConnection(v NetworkingV1GcpPrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentConnection"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) MergeNetworkingV1GcpPrivateLinkAttachmentConnection(v NetworkingV1GcpPrivateLinkAttachmentConnection) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentConnection"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsPrivateLinkAttachmentConnection":
		return t.AsNetworkingV1AwsPrivateLinkAttachmentConnection()
	case "AzurePrivateLinkAttachmentConnection":
		return t.AsNetworkingV1AzurePrivateLinkAttachmentConnection()
	case "GcpPrivateLinkAttachmentConnection":
		return t.AsNetworkingV1GcpPrivateLinkAttachmentConnection()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionSpec_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) AsNetworkingV1AwsPrivateLinkAttachmentConnectionStatus() (NetworkingV1AwsPrivateLinkAttachmentConnectionStatus, error) {
	var body NetworkingV1AwsPrivateLinkAttachmentConnectionStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) FromNetworkingV1AwsPrivateLinkAttachmentConnectionStatus(v NetworkingV1AwsPrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentConnectionStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) MergeNetworkingV1AwsPrivateLinkAttachmentConnectionStatus(v NetworkingV1AwsPrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentConnectionStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) AsNetworkingV1AzurePrivateLinkAttachmentConnectionStatus() (NetworkingV1AzurePrivateLinkAttachmentConnectionStatus, error) {
	var body NetworkingV1AzurePrivateLinkAttachmentConnectionStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) FromNetworkingV1AzurePrivateLinkAttachmentConnectionStatus(v NetworkingV1AzurePrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentConnectionStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) MergeNetworkingV1AzurePrivateLinkAttachmentConnectionStatus(v NetworkingV1AzurePrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentConnectionStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) AsNetworkingV1GcpPrivateLinkAttachmentConnectionStatus() (NetworkingV1GcpPrivateLinkAttachmentConnectionStatus, error) {
	var body NetworkingV1GcpPrivateLinkAttachmentConnectionStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) FromNetworkingV1GcpPrivateLinkAttachmentConnectionStatus(v NetworkingV1GcpPrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentConnectionStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) MergeNetworkingV1GcpPrivateLinkAttachmentConnectionStatus(v NetworkingV1GcpPrivateLinkAttachmentConnectionStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentConnectionStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsPrivateLinkAttachmentConnectionStatus":
		return t.AsNetworkingV1AwsPrivateLinkAttachmentConnectionStatus()
	case "AzurePrivateLinkAttachmentConnectionStatus":
		return t.AsNetworkingV1AzurePrivateLinkAttachmentConnectionStatus()
	case "GcpPrivateLinkAttachmentConnectionStatus":
		return t.AsNetworkingV1GcpPrivateLinkAttachmentConnectionStatus()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1PrivateLinkAttachmentConnectionStatus_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) AsNetworkingV1AwsPrivateLinkAttachmentStatus() (NetworkingV1AwsPrivateLinkAttachmentStatus, error) {
	var body NetworkingV1AwsPrivateLinkAttachmentStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) FromNetworkingV1AwsPrivateLinkAttachmentStatus(v NetworkingV1AwsPrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) MergeNetworkingV1AwsPrivateLinkAttachmentStatus(v NetworkingV1AwsPrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsPrivateLinkAttachmentStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) AsNetworkingV1AzurePrivateLinkAttachmentStatus() (NetworkingV1AzurePrivateLinkAttachmentStatus, error) {
	var body NetworkingV1AzurePrivateLinkAttachmentStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) FromNetworkingV1AzurePrivateLinkAttachmentStatus(v NetworkingV1AzurePrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) MergeNetworkingV1AzurePrivateLinkAttachmentStatus(v NetworkingV1AzurePrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AzurePrivateLinkAttachmentStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) AsNetworkingV1GcpPrivateLinkAttachmentStatus() (NetworkingV1GcpPrivateLinkAttachmentStatus, error) {
	var body NetworkingV1GcpPrivateLinkAttachmentStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) FromNetworkingV1GcpPrivateLinkAttachmentStatus(v NetworkingV1GcpPrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) MergeNetworkingV1GcpPrivateLinkAttachmentStatus(v NetworkingV1GcpPrivateLinkAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"GcpPrivateLinkAttachmentStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsPrivateLinkAttachmentStatus":
		return t.AsNetworkingV1AwsPrivateLinkAttachmentStatus()
	case "AzurePrivateLinkAttachmentStatus":
		return t.AsNetworkingV1AzurePrivateLinkAttachmentStatus()
	case "GcpPrivateLinkAttachmentStatus":
		return t.AsNetworkingV1GcpPrivateLinkAttachmentStatus()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1PrivateLinkAttachmentStatus_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1PrivateLinkAttachmentStatus_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1TransitGatewayAttachmentSpec_Cloud) AsNetworkingV1AwsTransitGatewayAttachment() (NetworkingV1AwsTransitGatewayAttachment, error) {
	var body NetworkingV1AwsTransitGatewayAttachment
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1TransitGatewayAttachmentSpec_Cloud) FromNetworkingV1AwsTransitGatewayAttachment(v NetworkingV1AwsTransitGatewayAttachment) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsTransitGatewayAttachment"}`))
	t.union = b
	return err
}
func (t *NetworkingV1TransitGatewayAttachmentSpec_Cloud) MergeNetworkingV1AwsTransitGatewayAttachment(v NetworkingV1AwsTransitGatewayAttachment) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsTransitGatewayAttachment"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1TransitGatewayAttachmentSpec_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1TransitGatewayAttachmentSpec_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsTransitGatewayAttachment":
		return t.AsNetworkingV1AwsTransitGatewayAttachment()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1TransitGatewayAttachmentSpec_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1TransitGatewayAttachmentSpec_Cloud) UnmarshalJSON(b []byte) error {
	err := t.union.UnmarshalJSON(b)
	return err
}
func (t NetworkingV1TransitGatewayAttachmentStatus_Cloud) AsNetworkingV1AwsTransitGatewayAttachmentStatus() (NetworkingV1AwsTransitGatewayAttachmentStatus, error) {
	var body NetworkingV1AwsTransitGatewayAttachmentStatus
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NetworkingV1TransitGatewayAttachmentStatus_Cloud) FromNetworkingV1AwsTransitGatewayAttachmentStatus(v NetworkingV1AwsTransitGatewayAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsTransitGatewayAttachmentStatus"}`))
	t.union = b
	return err
}
func (t *NetworkingV1TransitGatewayAttachmentStatus_Cloud) MergeNetworkingV1AwsTransitGatewayAttachmentStatus(v NetworkingV1AwsTransitGatewayAttachmentStatus) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"AwsTransitGatewayAttachmentStatus"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NetworkingV1TransitGatewayAttachmentStatus_Cloud) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NetworkingV1TransitGatewayAttachmentStatus_Cloud) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "AwsTransitGatewayAttachmentStatus":
		return t.AsNetworkingV1AwsTransitGatewayAttachmentStatus()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NetworkingV1TransitGatewayAttachmentStatus_Cloud) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NetworkingV1TransitGatewayAttachmentStatus_Cloud) UnmarshalJSON(b []byte) error {
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
type oasClient struct {
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
type ClientOption func(*oasClient) error

func NewClient(server string, opts ...ClientOption) (*oasClient, error) {
	// create a client with sane default values
	client := oasClient{
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
	return func(c *oasClient) error {
		c.Client = doer
		return nil
	}
}
func WithRequestEditorFn(fn RequestEditorFn) ClientOption {
	return func(c *oasClient) error {
		c.RequestEditors = append(c.RequestEditors, fn)
		return nil
	}
}

type ClientInterface interface {

	// ListNetworkingV1AccessPoints List of Access Points
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all access points.
	//
	// Corresponds with GET /networking/v1/access-points (the `ListNetworkingV1AccessPoints` operationId).
	ListNetworkingV1AccessPoints(ctx context.Context, params *ListNetworkingV1AccessPointsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1AccessPointWithBody Create an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an access point.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/access-points (the `CreateNetworkingV1AccessPoint` operationId).
	CreateNetworkingV1AccessPointWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1AccessPoint Create an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an access point.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/access-points (the `CreateNetworkingV1AccessPoint` operationId).
	CreateNetworkingV1AccessPoint(ctx context.Context, body CreateNetworkingV1AccessPointJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1AccessPoint Delete an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an access point.
	//
	// Corresponds with DELETE /networking/v1/access-points/{id} (the `DeleteNetworkingV1AccessPoint` operationId).
	DeleteNetworkingV1AccessPoint(ctx context.Context, id string, params *DeleteNetworkingV1AccessPointParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1AccessPoint Read an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an access point.
	//
	// Corresponds with GET /networking/v1/access-points/{id} (the `GetNetworkingV1AccessPoint` operationId).
	GetNetworkingV1AccessPoint(ctx context.Context, id string, params *GetNetworkingV1AccessPointParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1AccessPointWithBody Update an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an access point.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/access-points/{id} (the `UpdateNetworkingV1AccessPoint` operationId).
	UpdateNetworkingV1AccessPointWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1AccessPoint Update an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an access point.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/access-points/{id} (the `UpdateNetworkingV1AccessPoint` operationId).
	UpdateNetworkingV1AccessPoint(ctx context.Context, id string, body UpdateNetworkingV1AccessPointJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1DnsForwarders List of DNS Forwarders
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all DNS forwarders.
	//
	// Corresponds with GET /networking/v1/dns-forwarders (the `ListNetworkingV1DnsForwarders` operationId).
	ListNetworkingV1DnsForwarders(ctx context.Context, params *ListNetworkingV1DnsForwardersParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1DnsForwarderWithBody Create a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS forwarder.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/dns-forwarders (the `CreateNetworkingV1DnsForwarder` operationId).
	CreateNetworkingV1DnsForwarderWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1DnsForwarder Create a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS forwarder.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/dns-forwarders (the `CreateNetworkingV1DnsForwarder` operationId).
	CreateNetworkingV1DnsForwarder(ctx context.Context, body CreateNetworkingV1DnsForwarderJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1DnsForwarder Delete a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a DNS forwarder.
	//
	// Corresponds with DELETE /networking/v1/dns-forwarders/{id} (the `DeleteNetworkingV1DnsForwarder` operationId).
	DeleteNetworkingV1DnsForwarder(ctx context.Context, id string, params *DeleteNetworkingV1DnsForwarderParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1DnsForwarder Read a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a DNS forwarder.
	//
	// Corresponds with GET /networking/v1/dns-forwarders/{id} (the `GetNetworkingV1DnsForwarder` operationId).
	GetNetworkingV1DnsForwarder(ctx context.Context, id string, params *GetNetworkingV1DnsForwarderParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1DnsForwarderWithBody Update a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS forwarder.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/dns-forwarders/{id} (the `UpdateNetworkingV1DnsForwarder` operationId).
	UpdateNetworkingV1DnsForwarderWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1DnsForwarder Update a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS forwarder.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/dns-forwarders/{id} (the `UpdateNetworkingV1DnsForwarder` operationId).
	UpdateNetworkingV1DnsForwarder(ctx context.Context, id string, body UpdateNetworkingV1DnsForwarderJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1DnsRecords List of DNS Records
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all DNS records.
	//
	// Corresponds with GET /networking/v1/dns-records (the `ListNetworkingV1DnsRecords` operationId).
	ListNetworkingV1DnsRecords(ctx context.Context, params *ListNetworkingV1DnsRecordsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1DnsRecordWithBody Create a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS record.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/dns-records (the `CreateNetworkingV1DnsRecord` operationId).
	CreateNetworkingV1DnsRecordWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1DnsRecord Create a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS record.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/dns-records (the `CreateNetworkingV1DnsRecord` operationId).
	CreateNetworkingV1DnsRecord(ctx context.Context, body CreateNetworkingV1DnsRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1DnsRecord Delete a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a DNS record.
	//
	// Corresponds with DELETE /networking/v1/dns-records/{id} (the `DeleteNetworkingV1DnsRecord` operationId).
	DeleteNetworkingV1DnsRecord(ctx context.Context, id string, params *DeleteNetworkingV1DnsRecordParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1DnsRecord Read a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a DNS record.
	//
	// Corresponds with GET /networking/v1/dns-records/{id} (the `GetNetworkingV1DnsRecord` operationId).
	GetNetworkingV1DnsRecord(ctx context.Context, id string, params *GetNetworkingV1DnsRecordParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1DnsRecordWithBody Update a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS record.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/dns-records/{id} (the `UpdateNetworkingV1DnsRecord` operationId).
	UpdateNetworkingV1DnsRecordWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1DnsRecord Update a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS record.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/dns-records/{id} (the `UpdateNetworkingV1DnsRecord` operationId).
	UpdateNetworkingV1DnsRecord(ctx context.Context, id string, body UpdateNetworkingV1DnsRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1Gateways List of Gateways
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all gateways.
	//
	// Corresponds with GET /networking/v1/gateways (the `ListNetworkingV1Gateways` operationId).
	ListNetworkingV1Gateways(ctx context.Context, params *ListNetworkingV1GatewaysParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1GatewayWithBody Create a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a gateway.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/gateways (the `CreateNetworkingV1Gateway` operationId).
	CreateNetworkingV1GatewayWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1Gateway Create a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a gateway.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/gateways (the `CreateNetworkingV1Gateway` operationId).
	CreateNetworkingV1Gateway(ctx context.Context, body CreateNetworkingV1GatewayJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1Gateway Delete a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a gateway.
	//
	// Corresponds with DELETE /networking/v1/gateways/{id} (the `DeleteNetworkingV1Gateway` operationId).
	DeleteNetworkingV1Gateway(ctx context.Context, id string, params *DeleteNetworkingV1GatewayParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1Gateway Read a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a gateway.
	//
	// Corresponds with GET /networking/v1/gateways/{id} (the `GetNetworkingV1Gateway` operationId).
	GetNetworkingV1Gateway(ctx context.Context, id string, params *GetNetworkingV1GatewayParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1GatewayWithBody Update a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a gateway.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/gateways/{id} (the `UpdateNetworkingV1Gateway` operationId).
	UpdateNetworkingV1GatewayWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1Gateway Update a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a gateway.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/gateways/{id} (the `UpdateNetworkingV1Gateway` operationId).
	UpdateNetworkingV1Gateway(ctx context.Context, id string, body UpdateNetworkingV1GatewayJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1IpAddresses List of IP Addresses
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Related guide: [Use Public Egress IP addresses on Confluent Cloud](https://docs.confluent.io/cloud/current/networking/static-egress-ip-addresses.html)
	//
	// Retrieve a sorted, filtered, paginated list of all IP Addresses.
	//
	// Corresponds with GET /networking/v1/ip-addresses (the `ListNetworkingV1IpAddresses` operationId).
	ListNetworkingV1IpAddresses(ctx context.Context, params *ListNetworkingV1IpAddressesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1NetworkLinkEndpoints List of Network Link Endpoints
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link endpoints.
	//
	// Corresponds with GET /networking/v1/network-link-endpoints (the `ListNetworkingV1NetworkLinkEndpoints` operationId).
	ListNetworkingV1NetworkLinkEndpoints(ctx context.Context, params *ListNetworkingV1NetworkLinkEndpointsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1NetworkLinkEndpointWithBody Create a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link endpoint.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/network-link-endpoints (the `CreateNetworkingV1NetworkLinkEndpoint` operationId).
	CreateNetworkingV1NetworkLinkEndpointWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1NetworkLinkEndpoint Create a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link endpoint.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/network-link-endpoints (the `CreateNetworkingV1NetworkLinkEndpoint` operationId).
	CreateNetworkingV1NetworkLinkEndpoint(ctx context.Context, body CreateNetworkingV1NetworkLinkEndpointJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1NetworkLinkEndpoint Delete a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network link endpoint.
	//
	// Corresponds with DELETE /networking/v1/network-link-endpoints/{id} (the `DeleteNetworkingV1NetworkLinkEndpoint` operationId).
	DeleteNetworkingV1NetworkLinkEndpoint(ctx context.Context, id string, params *DeleteNetworkingV1NetworkLinkEndpointParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1NetworkLinkEndpoint Read a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link endpoint.
	//
	// Corresponds with GET /networking/v1/network-link-endpoints/{id} (the `GetNetworkingV1NetworkLinkEndpoint` operationId).
	GetNetworkingV1NetworkLinkEndpoint(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkEndpointParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1NetworkLinkEndpointWithBody Update a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link endpoint.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/network-link-endpoints/{id} (the `UpdateNetworkingV1NetworkLinkEndpoint` operationId).
	UpdateNetworkingV1NetworkLinkEndpointWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1NetworkLinkEndpoint Update a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link endpoint.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/network-link-endpoints/{id} (the `UpdateNetworkingV1NetworkLinkEndpoint` operationId).
	UpdateNetworkingV1NetworkLinkEndpoint(ctx context.Context, id string, body UpdateNetworkingV1NetworkLinkEndpointJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1NetworkLinkServiceAssociations List of Network Link Service Associations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link service associations.
	//
	// Corresponds with GET /networking/v1/network-link-service-associations (the `ListNetworkingV1NetworkLinkServiceAssociations` operationId).
	ListNetworkingV1NetworkLinkServiceAssociations(ctx context.Context, params *ListNetworkingV1NetworkLinkServiceAssociationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1NetworkLinkServiceAssociation Read a Network Link Service Association
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link service association.
	//
	// Corresponds with GET /networking/v1/network-link-service-associations/{id} (the `GetNetworkingV1NetworkLinkServiceAssociation` operationId).
	GetNetworkingV1NetworkLinkServiceAssociation(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkServiceAssociationParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1NetworkLinkServices List of Network Link Services
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link services.
	//
	// Corresponds with GET /networking/v1/network-link-services (the `ListNetworkingV1NetworkLinkServices` operationId).
	ListNetworkingV1NetworkLinkServices(ctx context.Context, params *ListNetworkingV1NetworkLinkServicesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1NetworkLinkServiceWithBody Create a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link service.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/network-link-services (the `CreateNetworkingV1NetworkLinkService` operationId).
	CreateNetworkingV1NetworkLinkServiceWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1NetworkLinkService Create a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link service.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/network-link-services (the `CreateNetworkingV1NetworkLinkService` operationId).
	CreateNetworkingV1NetworkLinkService(ctx context.Context, body CreateNetworkingV1NetworkLinkServiceJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1NetworkLinkService Delete a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network link service.
	//
	// Corresponds with DELETE /networking/v1/network-link-services/{id} (the `DeleteNetworkingV1NetworkLinkService` operationId).
	DeleteNetworkingV1NetworkLinkService(ctx context.Context, id string, params *DeleteNetworkingV1NetworkLinkServiceParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1NetworkLinkService Read a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link service.
	//
	// Corresponds with GET /networking/v1/network-link-services/{id} (the `GetNetworkingV1NetworkLinkService` operationId).
	GetNetworkingV1NetworkLinkService(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkServiceParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1NetworkLinkServiceWithBody Update a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link service.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/network-link-services/{id} (the `UpdateNetworkingV1NetworkLinkService` operationId).
	UpdateNetworkingV1NetworkLinkServiceWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1NetworkLinkService Update a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link service.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/network-link-services/{id} (the `UpdateNetworkingV1NetworkLinkService` operationId).
	UpdateNetworkingV1NetworkLinkService(ctx context.Context, id string, body UpdateNetworkingV1NetworkLinkServiceJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1Networks List of Networks
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all networks.
	//
	// Corresponds with GET /networking/v1/networks (the `ListNetworkingV1Networks` operationId).
	ListNetworkingV1Networks(ctx context.Context, params *ListNetworkingV1NetworksParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1NetworkWithBody Create a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/networks (the `CreateNetworkingV1Network` operationId).
	CreateNetworkingV1NetworkWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1Network Create a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/networks (the `CreateNetworkingV1Network` operationId).
	CreateNetworkingV1Network(ctx context.Context, body CreateNetworkingV1NetworkJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1Network Delete a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network.
	//
	// Corresponds with DELETE /networking/v1/networks/{id} (the `DeleteNetworkingV1Network` operationId).
	DeleteNetworkingV1Network(ctx context.Context, id string, params *DeleteNetworkingV1NetworkParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1Network Read a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network.
	//
	// Corresponds with GET /networking/v1/networks/{id} (the `GetNetworkingV1Network` operationId).
	GetNetworkingV1Network(ctx context.Context, id string, params *GetNetworkingV1NetworkParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1NetworkWithBody Update a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/networks/{id} (the `UpdateNetworkingV1Network` operationId).
	UpdateNetworkingV1NetworkWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1Network Update a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/networks/{id} (the `UpdateNetworkingV1Network` operationId).
	UpdateNetworkingV1Network(ctx context.Context, id string, body UpdateNetworkingV1NetworkJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1Peerings List of Peerings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all peerings.
	//
	// Corresponds with GET /networking/v1/peerings (the `ListNetworkingV1Peerings` operationId).
	ListNetworkingV1Peerings(ctx context.Context, params *ListNetworkingV1PeeringsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PeeringWithBody Create a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a peering.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/peerings (the `CreateNetworkingV1Peering` operationId).
	CreateNetworkingV1PeeringWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1Peering Create a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a peering.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/peerings (the `CreateNetworkingV1Peering` operationId).
	CreateNetworkingV1Peering(ctx context.Context, body CreateNetworkingV1PeeringJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1Peering Delete a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a peering.
	//
	// Corresponds with DELETE /networking/v1/peerings/{id} (the `DeleteNetworkingV1Peering` operationId).
	DeleteNetworkingV1Peering(ctx context.Context, id string, params *DeleteNetworkingV1PeeringParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1Peering Read a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a peering.
	//
	// Corresponds with GET /networking/v1/peerings/{id} (the `GetNetworkingV1Peering` operationId).
	GetNetworkingV1Peering(ctx context.Context, id string, params *GetNetworkingV1PeeringParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PeeringWithBody Update a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a peering.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/peerings/{id} (the `UpdateNetworkingV1Peering` operationId).
	UpdateNetworkingV1PeeringWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1Peering Update a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a peering.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/peerings/{id} (the `UpdateNetworkingV1Peering` operationId).
	UpdateNetworkingV1Peering(ctx context.Context, id string, body UpdateNetworkingV1PeeringJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1PrivateLinkAccesses List of Private Link Accesses
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link accesses.
	//
	// Corresponds with GET /networking/v1/private-link-accesses (the `ListNetworkingV1PrivateLinkAccesses` operationId).
	ListNetworkingV1PrivateLinkAccesses(ctx context.Context, params *ListNetworkingV1PrivateLinkAccessesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAccessWithBody Create a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link access.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/private-link-accesses (the `CreateNetworkingV1PrivateLinkAccess` operationId).
	CreateNetworkingV1PrivateLinkAccessWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAccess Create a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link access.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/private-link-accesses (the `CreateNetworkingV1PrivateLinkAccess` operationId).
	CreateNetworkingV1PrivateLinkAccess(ctx context.Context, body CreateNetworkingV1PrivateLinkAccessJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1PrivateLinkAccess Delete a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link access.
	//
	// Corresponds with DELETE /networking/v1/private-link-accesses/{id} (the `DeleteNetworkingV1PrivateLinkAccess` operationId).
	DeleteNetworkingV1PrivateLinkAccess(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAccessParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1PrivateLinkAccess Read a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link access.
	//
	// Corresponds with GET /networking/v1/private-link-accesses/{id} (the `GetNetworkingV1PrivateLinkAccess` operationId).
	GetNetworkingV1PrivateLinkAccess(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAccessParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAccessWithBody Update a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link access.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-accesses/{id} (the `UpdateNetworkingV1PrivateLinkAccess` operationId).
	UpdateNetworkingV1PrivateLinkAccessWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAccess Update a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link access.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-accesses/{id} (the `UpdateNetworkingV1PrivateLinkAccess` operationId).
	UpdateNetworkingV1PrivateLinkAccess(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAccessJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1PrivateLinkAttachmentConnections List of Private Link Attachment Connections
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link attachment connections.
	//
	// Corresponds with GET /networking/v1/private-link-attachment-connections (the `ListNetworkingV1PrivateLinkAttachmentConnections` operationId).
	ListNetworkingV1PrivateLinkAttachmentConnections(ctx context.Context, params *ListNetworkingV1PrivateLinkAttachmentConnectionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAttachmentConnectionWithBody Create a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment connection.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/private-link-attachment-connections (the `CreateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	CreateNetworkingV1PrivateLinkAttachmentConnectionWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAttachmentConnection Create a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment connection.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/private-link-attachment-connections (the `CreateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	CreateNetworkingV1PrivateLinkAttachmentConnection(ctx context.Context, body CreateNetworkingV1PrivateLinkAttachmentConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1PrivateLinkAttachmentConnection Delete a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link attachment connection.
	//
	// Corresponds with DELETE /networking/v1/private-link-attachment-connections/{id} (the `DeleteNetworkingV1PrivateLinkAttachmentConnection` operationId).
	DeleteNetworkingV1PrivateLinkAttachmentConnection(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAttachmentConnectionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1PrivateLinkAttachmentConnection Read a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link attachment connection.
	//
	// Corresponds with GET /networking/v1/private-link-attachment-connections/{id} (the `GetNetworkingV1PrivateLinkAttachmentConnection` operationId).
	GetNetworkingV1PrivateLinkAttachmentConnection(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAttachmentConnectionParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAttachmentConnectionWithBody Update a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment connection.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-attachment-connections/{id} (the `UpdateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentConnectionWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAttachmentConnection Update a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment connection.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-attachment-connections/{id} (the `UpdateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentConnection(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1PrivateLinkAttachments List of Private Link Attachments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link attachments.
	//
	// Corresponds with GET /networking/v1/private-link-attachments (the `ListNetworkingV1PrivateLinkAttachments` operationId).
	ListNetworkingV1PrivateLinkAttachments(ctx context.Context, params *ListNetworkingV1PrivateLinkAttachmentsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAttachmentWithBody Create a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/private-link-attachments (the `CreateNetworkingV1PrivateLinkAttachment` operationId).
	CreateNetworkingV1PrivateLinkAttachmentWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1PrivateLinkAttachment Create a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/private-link-attachments (the `CreateNetworkingV1PrivateLinkAttachment` operationId).
	CreateNetworkingV1PrivateLinkAttachment(ctx context.Context, body CreateNetworkingV1PrivateLinkAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1PrivateLinkAttachment Delete a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link attachment.
	//
	// Corresponds with DELETE /networking/v1/private-link-attachments/{id} (the `DeleteNetworkingV1PrivateLinkAttachment` operationId).
	DeleteNetworkingV1PrivateLinkAttachment(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAttachmentParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1PrivateLinkAttachment Read a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link attachment.
	//
	// Corresponds with GET /networking/v1/private-link-attachments/{id} (the `GetNetworkingV1PrivateLinkAttachment` operationId).
	GetNetworkingV1PrivateLinkAttachment(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAttachmentParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAttachmentWithBody Update a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-attachments/{id} (the `UpdateNetworkingV1PrivateLinkAttachment` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1PrivateLinkAttachment Update a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/private-link-attachments/{id} (the `UpdateNetworkingV1PrivateLinkAttachment` operationId).
	UpdateNetworkingV1PrivateLinkAttachment(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNetworkingV1TransitGatewayAttachments List of Transit Gateway Attachments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all transit gateway attachments.
	//
	// Corresponds with GET /networking/v1/transit-gateway-attachments (the `ListNetworkingV1TransitGatewayAttachments` operationId).
	ListNetworkingV1TransitGatewayAttachments(ctx context.Context, params *ListNetworkingV1TransitGatewayAttachmentsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1TransitGatewayAttachmentWithBody Create a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a transit gateway attachment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /networking/v1/transit-gateway-attachments (the `CreateNetworkingV1TransitGatewayAttachment` operationId).
	CreateNetworkingV1TransitGatewayAttachmentWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNetworkingV1TransitGatewayAttachment Create a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a transit gateway attachment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /networking/v1/transit-gateway-attachments (the `CreateNetworkingV1TransitGatewayAttachment` operationId).
	CreateNetworkingV1TransitGatewayAttachment(ctx context.Context, body CreateNetworkingV1TransitGatewayAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNetworkingV1TransitGatewayAttachment Delete a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a transit gateway attachment.
	//
	// Corresponds with DELETE /networking/v1/transit-gateway-attachments/{id} (the `DeleteNetworkingV1TransitGatewayAttachment` operationId).
	DeleteNetworkingV1TransitGatewayAttachment(ctx context.Context, id string, params *DeleteNetworkingV1TransitGatewayAttachmentParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNetworkingV1TransitGatewayAttachment Read a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a transit gateway attachment.
	//
	// Corresponds with GET /networking/v1/transit-gateway-attachments/{id} (the `GetNetworkingV1TransitGatewayAttachment` operationId).
	GetNetworkingV1TransitGatewayAttachment(ctx context.Context, id string, params *GetNetworkingV1TransitGatewayAttachmentParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1TransitGatewayAttachmentWithBody Update a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a transit gateway attachment.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /networking/v1/transit-gateway-attachments/{id} (the `UpdateNetworkingV1TransitGatewayAttachment` operationId).
	UpdateNetworkingV1TransitGatewayAttachmentWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNetworkingV1TransitGatewayAttachment Update a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a transit gateway attachment.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /networking/v1/transit-gateway-attachments/{id} (the `UpdateNetworkingV1TransitGatewayAttachment` operationId).
	UpdateNetworkingV1TransitGatewayAttachment(ctx context.Context, id string, body UpdateNetworkingV1TransitGatewayAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)
}

func (c *oasClient) applyEditors(ctx context.Context, req *http.Request, additionalEditors []RequestEditorFn) error {
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
	return func(c *oasClient) error {
		newBaseURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}
		c.Server = newBaseURL.String()
		return nil
	}
}

type ClientWithResponsesInterface interface {

	// ListNetworkingV1AccessPointsWithResponse List of Access Points
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all access points.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/access-points (the `ListNetworkingV1AccessPoints` operationId).
	ListNetworkingV1AccessPointsWithResponse(ctx context.Context, params *ListNetworkingV1AccessPointsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1AccessPointsResponse, error)

	// CreateNetworkingV1AccessPointWithBodyWithResponse Create an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an access point.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/access-points (the `CreateNetworkingV1AccessPoint` operationId).
	CreateNetworkingV1AccessPointWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1AccessPointResponse, error)

	// CreateNetworkingV1AccessPointWithResponse Create an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an access point.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/access-points (the `CreateNetworkingV1AccessPoint` operationId).
	CreateNetworkingV1AccessPointWithResponse(ctx context.Context, body CreateNetworkingV1AccessPointJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1AccessPointResponse, error)

	// DeleteNetworkingV1AccessPointWithResponse Delete an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an access point.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/access-points/{id} (the `DeleteNetworkingV1AccessPoint` operationId).
	DeleteNetworkingV1AccessPointWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1AccessPointParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1AccessPointResponse, error)

	// GetNetworkingV1AccessPointWithResponse Read an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an access point.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/access-points/{id} (the `GetNetworkingV1AccessPoint` operationId).
	GetNetworkingV1AccessPointWithResponse(ctx context.Context, id string, params *GetNetworkingV1AccessPointParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1AccessPointResponse, error)

	// UpdateNetworkingV1AccessPointWithBodyWithResponse Update an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an access point.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/access-points/{id} (the `UpdateNetworkingV1AccessPoint` operationId).
	UpdateNetworkingV1AccessPointWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1AccessPointResponse, error)

	// UpdateNetworkingV1AccessPointWithResponse Update an Access Point
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an access point.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/access-points/{id} (the `UpdateNetworkingV1AccessPoint` operationId).
	UpdateNetworkingV1AccessPointWithResponse(ctx context.Context, id string, body UpdateNetworkingV1AccessPointJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1AccessPointResponse, error)

	// ListNetworkingV1DnsForwardersWithResponse List of DNS Forwarders
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all DNS forwarders.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/dns-forwarders (the `ListNetworkingV1DnsForwarders` operationId).
	ListNetworkingV1DnsForwardersWithResponse(ctx context.Context, params *ListNetworkingV1DnsForwardersParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1DnsForwardersResponse, error)

	// CreateNetworkingV1DnsForwarderWithBodyWithResponse Create a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS forwarder.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/dns-forwarders (the `CreateNetworkingV1DnsForwarder` operationId).
	CreateNetworkingV1DnsForwarderWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1DnsForwarderResponse, error)

	// CreateNetworkingV1DnsForwarderWithResponse Create a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS forwarder.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/dns-forwarders (the `CreateNetworkingV1DnsForwarder` operationId).
	CreateNetworkingV1DnsForwarderWithResponse(ctx context.Context, body CreateNetworkingV1DnsForwarderJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1DnsForwarderResponse, error)

	// DeleteNetworkingV1DnsForwarderWithResponse Delete a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a DNS forwarder.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/dns-forwarders/{id} (the `DeleteNetworkingV1DnsForwarder` operationId).
	DeleteNetworkingV1DnsForwarderWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1DnsForwarderParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1DnsForwarderResponse, error)

	// GetNetworkingV1DnsForwarderWithResponse Read a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a DNS forwarder.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/dns-forwarders/{id} (the `GetNetworkingV1DnsForwarder` operationId).
	GetNetworkingV1DnsForwarderWithResponse(ctx context.Context, id string, params *GetNetworkingV1DnsForwarderParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1DnsForwarderResponse, error)

	// UpdateNetworkingV1DnsForwarderWithBodyWithResponse Update a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS forwarder.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/dns-forwarders/{id} (the `UpdateNetworkingV1DnsForwarder` operationId).
	UpdateNetworkingV1DnsForwarderWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1DnsForwarderResponse, error)

	// UpdateNetworkingV1DnsForwarderWithResponse Update a DNS Forwarder
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS forwarder.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/dns-forwarders/{id} (the `UpdateNetworkingV1DnsForwarder` operationId).
	UpdateNetworkingV1DnsForwarderWithResponse(ctx context.Context, id string, body UpdateNetworkingV1DnsForwarderJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1DnsForwarderResponse, error)

	// ListNetworkingV1DnsRecordsWithResponse List of DNS Records
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all DNS records.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/dns-records (the `ListNetworkingV1DnsRecords` operationId).
	ListNetworkingV1DnsRecordsWithResponse(ctx context.Context, params *ListNetworkingV1DnsRecordsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1DnsRecordsResponse, error)

	// CreateNetworkingV1DnsRecordWithBodyWithResponse Create a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS record.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/dns-records (the `CreateNetworkingV1DnsRecord` operationId).
	CreateNetworkingV1DnsRecordWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1DnsRecordResponse, error)

	// CreateNetworkingV1DnsRecordWithResponse Create a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a DNS record.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/dns-records (the `CreateNetworkingV1DnsRecord` operationId).
	CreateNetworkingV1DnsRecordWithResponse(ctx context.Context, body CreateNetworkingV1DnsRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1DnsRecordResponse, error)

	// DeleteNetworkingV1DnsRecordWithResponse Delete a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a DNS record.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/dns-records/{id} (the `DeleteNetworkingV1DnsRecord` operationId).
	DeleteNetworkingV1DnsRecordWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1DnsRecordParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1DnsRecordResponse, error)

	// GetNetworkingV1DnsRecordWithResponse Read a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a DNS record.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/dns-records/{id} (the `GetNetworkingV1DnsRecord` operationId).
	GetNetworkingV1DnsRecordWithResponse(ctx context.Context, id string, params *GetNetworkingV1DnsRecordParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1DnsRecordResponse, error)

	// UpdateNetworkingV1DnsRecordWithBodyWithResponse Update a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS record.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/dns-records/{id} (the `UpdateNetworkingV1DnsRecord` operationId).
	UpdateNetworkingV1DnsRecordWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1DnsRecordResponse, error)

	// UpdateNetworkingV1DnsRecordWithResponse Update a DNS Record
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a DNS record.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/dns-records/{id} (the `UpdateNetworkingV1DnsRecord` operationId).
	UpdateNetworkingV1DnsRecordWithResponse(ctx context.Context, id string, body UpdateNetworkingV1DnsRecordJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1DnsRecordResponse, error)

	// ListNetworkingV1GatewaysWithResponse List of Gateways
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all gateways.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/gateways (the `ListNetworkingV1Gateways` operationId).
	ListNetworkingV1GatewaysWithResponse(ctx context.Context, params *ListNetworkingV1GatewaysParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1GatewaysResponse, error)

	// CreateNetworkingV1GatewayWithBodyWithResponse Create a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a gateway.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/gateways (the `CreateNetworkingV1Gateway` operationId).
	CreateNetworkingV1GatewayWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1GatewayResponse, error)

	// CreateNetworkingV1GatewayWithResponse Create a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a gateway.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/gateways (the `CreateNetworkingV1Gateway` operationId).
	CreateNetworkingV1GatewayWithResponse(ctx context.Context, body CreateNetworkingV1GatewayJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1GatewayResponse, error)

	// DeleteNetworkingV1GatewayWithResponse Delete a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a gateway.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/gateways/{id} (the `DeleteNetworkingV1Gateway` operationId).
	DeleteNetworkingV1GatewayWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1GatewayParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1GatewayResponse, error)

	// GetNetworkingV1GatewayWithResponse Read a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a gateway.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/gateways/{id} (the `GetNetworkingV1Gateway` operationId).
	GetNetworkingV1GatewayWithResponse(ctx context.Context, id string, params *GetNetworkingV1GatewayParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1GatewayResponse, error)

	// UpdateNetworkingV1GatewayWithBodyWithResponse Update a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a gateway.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/gateways/{id} (the `UpdateNetworkingV1Gateway` operationId).
	UpdateNetworkingV1GatewayWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1GatewayResponse, error)

	// UpdateNetworkingV1GatewayWithResponse Update a Gateway
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a gateway.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/gateways/{id} (the `UpdateNetworkingV1Gateway` operationId).
	UpdateNetworkingV1GatewayWithResponse(ctx context.Context, id string, body UpdateNetworkingV1GatewayJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1GatewayResponse, error)

	// ListNetworkingV1IpAddressesWithResponse List of IP Addresses
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Related guide: [Use Public Egress IP addresses on Confluent Cloud](https://docs.confluent.io/cloud/current/networking/static-egress-ip-addresses.html)
	//
	// Retrieve a sorted, filtered, paginated list of all IP Addresses.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/ip-addresses (the `ListNetworkingV1IpAddresses` operationId).
	ListNetworkingV1IpAddressesWithResponse(ctx context.Context, params *ListNetworkingV1IpAddressesParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1IpAddressesResponse, error)

	// ListNetworkingV1NetworkLinkEndpointsWithResponse List of Network Link Endpoints
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link endpoints.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-endpoints (the `ListNetworkingV1NetworkLinkEndpoints` operationId).
	ListNetworkingV1NetworkLinkEndpointsWithResponse(ctx context.Context, params *ListNetworkingV1NetworkLinkEndpointsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1NetworkLinkEndpointsResponse, error)

	// CreateNetworkingV1NetworkLinkEndpointWithBodyWithResponse Create a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link endpoint.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/network-link-endpoints (the `CreateNetworkingV1NetworkLinkEndpoint` operationId).
	CreateNetworkingV1NetworkLinkEndpointWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkLinkEndpointResponse, error)

	// CreateNetworkingV1NetworkLinkEndpointWithResponse Create a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link endpoint.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/network-link-endpoints (the `CreateNetworkingV1NetworkLinkEndpoint` operationId).
	CreateNetworkingV1NetworkLinkEndpointWithResponse(ctx context.Context, body CreateNetworkingV1NetworkLinkEndpointJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkLinkEndpointResponse, error)

	// DeleteNetworkingV1NetworkLinkEndpointWithResponse Delete a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network link endpoint.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/network-link-endpoints/{id} (the `DeleteNetworkingV1NetworkLinkEndpoint` operationId).
	DeleteNetworkingV1NetworkLinkEndpointWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1NetworkLinkEndpointParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1NetworkLinkEndpointResponse, error)

	// GetNetworkingV1NetworkLinkEndpointWithResponse Read a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link endpoint.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-endpoints/{id} (the `GetNetworkingV1NetworkLinkEndpoint` operationId).
	GetNetworkingV1NetworkLinkEndpointWithResponse(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkEndpointParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1NetworkLinkEndpointResponse, error)

	// UpdateNetworkingV1NetworkLinkEndpointWithBodyWithResponse Update a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link endpoint.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/network-link-endpoints/{id} (the `UpdateNetworkingV1NetworkLinkEndpoint` operationId).
	UpdateNetworkingV1NetworkLinkEndpointWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkLinkEndpointResponse, error)

	// UpdateNetworkingV1NetworkLinkEndpointWithResponse Update a Network Link Endpoint
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link endpoint.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/network-link-endpoints/{id} (the `UpdateNetworkingV1NetworkLinkEndpoint` operationId).
	UpdateNetworkingV1NetworkLinkEndpointWithResponse(ctx context.Context, id string, body UpdateNetworkingV1NetworkLinkEndpointJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkLinkEndpointResponse, error)

	// ListNetworkingV1NetworkLinkServiceAssociationsWithResponse List of Network Link Service Associations
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link service associations.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-service-associations (the `ListNetworkingV1NetworkLinkServiceAssociations` operationId).
	ListNetworkingV1NetworkLinkServiceAssociationsWithResponse(ctx context.Context, params *ListNetworkingV1NetworkLinkServiceAssociationsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1NetworkLinkServiceAssociationsResponse, error)

	// GetNetworkingV1NetworkLinkServiceAssociationWithResponse Read a Network Link Service Association
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link service association.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-service-associations/{id} (the `GetNetworkingV1NetworkLinkServiceAssociation` operationId).
	GetNetworkingV1NetworkLinkServiceAssociationWithResponse(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkServiceAssociationParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1NetworkLinkServiceAssociationResponse, error)

	// ListNetworkingV1NetworkLinkServicesWithResponse List of Network Link Services
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all network link services.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-services (the `ListNetworkingV1NetworkLinkServices` operationId).
	ListNetworkingV1NetworkLinkServicesWithResponse(ctx context.Context, params *ListNetworkingV1NetworkLinkServicesParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1NetworkLinkServicesResponse, error)

	// CreateNetworkingV1NetworkLinkServiceWithBodyWithResponse Create a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link service.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/network-link-services (the `CreateNetworkingV1NetworkLinkService` operationId).
	CreateNetworkingV1NetworkLinkServiceWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkLinkServiceResponse, error)

	// CreateNetworkingV1NetworkLinkServiceWithResponse Create a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network link service.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/network-link-services (the `CreateNetworkingV1NetworkLinkService` operationId).
	CreateNetworkingV1NetworkLinkServiceWithResponse(ctx context.Context, body CreateNetworkingV1NetworkLinkServiceJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkLinkServiceResponse, error)

	// DeleteNetworkingV1NetworkLinkServiceWithResponse Delete a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network link service.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/network-link-services/{id} (the `DeleteNetworkingV1NetworkLinkService` operationId).
	DeleteNetworkingV1NetworkLinkServiceWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1NetworkLinkServiceParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1NetworkLinkServiceResponse, error)

	// GetNetworkingV1NetworkLinkServiceWithResponse Read a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network link service.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/network-link-services/{id} (the `GetNetworkingV1NetworkLinkService` operationId).
	GetNetworkingV1NetworkLinkServiceWithResponse(ctx context.Context, id string, params *GetNetworkingV1NetworkLinkServiceParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1NetworkLinkServiceResponse, error)

	// UpdateNetworkingV1NetworkLinkServiceWithBodyWithResponse Update a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link service.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/network-link-services/{id} (the `UpdateNetworkingV1NetworkLinkService` operationId).
	UpdateNetworkingV1NetworkLinkServiceWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkLinkServiceResponse, error)

	// UpdateNetworkingV1NetworkLinkServiceWithResponse Update a Network Link Service
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network link service.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/network-link-services/{id} (the `UpdateNetworkingV1NetworkLinkService` operationId).
	UpdateNetworkingV1NetworkLinkServiceWithResponse(ctx context.Context, id string, body UpdateNetworkingV1NetworkLinkServiceJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkLinkServiceResponse, error)

	// ListNetworkingV1NetworksWithResponse List of Networks
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all networks.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/networks (the `ListNetworkingV1Networks` operationId).
	ListNetworkingV1NetworksWithResponse(ctx context.Context, params *ListNetworkingV1NetworksParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1NetworksResponse, error)

	// CreateNetworkingV1NetworkWithBodyWithResponse Create a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/networks (the `CreateNetworkingV1Network` operationId).
	CreateNetworkingV1NetworkWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkResponse, error)

	// CreateNetworkingV1NetworkWithResponse Create a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a network.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/networks (the `CreateNetworkingV1Network` operationId).
	CreateNetworkingV1NetworkWithResponse(ctx context.Context, body CreateNetworkingV1NetworkJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1NetworkResponse, error)

	// DeleteNetworkingV1NetworkWithResponse Delete a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a network.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/networks/{id} (the `DeleteNetworkingV1Network` operationId).
	DeleteNetworkingV1NetworkWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1NetworkParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1NetworkResponse, error)

	// GetNetworkingV1NetworkWithResponse Read a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a network.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/networks/{id} (the `GetNetworkingV1Network` operationId).
	GetNetworkingV1NetworkWithResponse(ctx context.Context, id string, params *GetNetworkingV1NetworkParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1NetworkResponse, error)

	// UpdateNetworkingV1NetworkWithBodyWithResponse Update a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/networks/{id} (the `UpdateNetworkingV1Network` operationId).
	UpdateNetworkingV1NetworkWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkResponse, error)

	// UpdateNetworkingV1NetworkWithResponse Update a Network
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a network.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/networks/{id} (the `UpdateNetworkingV1Network` operationId).
	UpdateNetworkingV1NetworkWithResponse(ctx context.Context, id string, body UpdateNetworkingV1NetworkJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1NetworkResponse, error)

	// ListNetworkingV1PeeringsWithResponse List of Peerings
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all peerings.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/peerings (the `ListNetworkingV1Peerings` operationId).
	ListNetworkingV1PeeringsWithResponse(ctx context.Context, params *ListNetworkingV1PeeringsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1PeeringsResponse, error)

	// CreateNetworkingV1PeeringWithBodyWithResponse Create a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a peering.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/peerings (the `CreateNetworkingV1Peering` operationId).
	CreateNetworkingV1PeeringWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PeeringResponse, error)

	// CreateNetworkingV1PeeringWithResponse Create a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a peering.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/peerings (the `CreateNetworkingV1Peering` operationId).
	CreateNetworkingV1PeeringWithResponse(ctx context.Context, body CreateNetworkingV1PeeringJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PeeringResponse, error)

	// DeleteNetworkingV1PeeringWithResponse Delete a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a peering.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/peerings/{id} (the `DeleteNetworkingV1Peering` operationId).
	DeleteNetworkingV1PeeringWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1PeeringParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1PeeringResponse, error)

	// GetNetworkingV1PeeringWithResponse Read a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a peering.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/peerings/{id} (the `GetNetworkingV1Peering` operationId).
	GetNetworkingV1PeeringWithResponse(ctx context.Context, id string, params *GetNetworkingV1PeeringParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1PeeringResponse, error)

	// UpdateNetworkingV1PeeringWithBodyWithResponse Update a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a peering.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/peerings/{id} (the `UpdateNetworkingV1Peering` operationId).
	UpdateNetworkingV1PeeringWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PeeringResponse, error)

	// UpdateNetworkingV1PeeringWithResponse Update a Peering
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a peering.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/peerings/{id} (the `UpdateNetworkingV1Peering` operationId).
	UpdateNetworkingV1PeeringWithResponse(ctx context.Context, id string, body UpdateNetworkingV1PeeringJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PeeringResponse, error)

	// ListNetworkingV1PrivateLinkAccessesWithResponse List of Private Link Accesses
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link accesses.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-accesses (the `ListNetworkingV1PrivateLinkAccesses` operationId).
	ListNetworkingV1PrivateLinkAccessesWithResponse(ctx context.Context, params *ListNetworkingV1PrivateLinkAccessesParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1PrivateLinkAccessesResponse, error)

	// CreateNetworkingV1PrivateLinkAccessWithBodyWithResponse Create a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link access.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-accesses (the `CreateNetworkingV1PrivateLinkAccess` operationId).
	CreateNetworkingV1PrivateLinkAccessWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAccessResponse, error)

	// CreateNetworkingV1PrivateLinkAccessWithResponse Create a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link access.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-accesses (the `CreateNetworkingV1PrivateLinkAccess` operationId).
	CreateNetworkingV1PrivateLinkAccessWithResponse(ctx context.Context, body CreateNetworkingV1PrivateLinkAccessJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAccessResponse, error)

	// DeleteNetworkingV1PrivateLinkAccessWithResponse Delete a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link access.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/private-link-accesses/{id} (the `DeleteNetworkingV1PrivateLinkAccess` operationId).
	DeleteNetworkingV1PrivateLinkAccessWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAccessParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1PrivateLinkAccessResponse, error)

	// GetNetworkingV1PrivateLinkAccessWithResponse Read a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link access.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-accesses/{id} (the `GetNetworkingV1PrivateLinkAccess` operationId).
	GetNetworkingV1PrivateLinkAccessWithResponse(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAccessParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1PrivateLinkAccessResponse, error)

	// UpdateNetworkingV1PrivateLinkAccessWithBodyWithResponse Update a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link access.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-accesses/{id} (the `UpdateNetworkingV1PrivateLinkAccess` operationId).
	UpdateNetworkingV1PrivateLinkAccessWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAccessResponse, error)

	// UpdateNetworkingV1PrivateLinkAccessWithResponse Update a Private Link Access
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link access.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-accesses/{id} (the `UpdateNetworkingV1PrivateLinkAccess` operationId).
	UpdateNetworkingV1PrivateLinkAccessWithResponse(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAccessJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAccessResponse, error)

	// ListNetworkingV1PrivateLinkAttachmentConnectionsWithResponse List of Private Link Attachment Connections
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link attachment connections.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-attachment-connections (the `ListNetworkingV1PrivateLinkAttachmentConnections` operationId).
	ListNetworkingV1PrivateLinkAttachmentConnectionsWithResponse(ctx context.Context, params *ListNetworkingV1PrivateLinkAttachmentConnectionsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1PrivateLinkAttachmentConnectionsResponse, error)

	// CreateNetworkingV1PrivateLinkAttachmentConnectionWithBodyWithResponse Create a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment connection.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-attachment-connections (the `CreateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	CreateNetworkingV1PrivateLinkAttachmentConnectionWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// CreateNetworkingV1PrivateLinkAttachmentConnectionWithResponse Create a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment connection.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-attachment-connections (the `CreateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	CreateNetworkingV1PrivateLinkAttachmentConnectionWithResponse(ctx context.Context, body CreateNetworkingV1PrivateLinkAttachmentConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// DeleteNetworkingV1PrivateLinkAttachmentConnectionWithResponse Delete a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link attachment connection.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/private-link-attachment-connections/{id} (the `DeleteNetworkingV1PrivateLinkAttachmentConnection` operationId).
	DeleteNetworkingV1PrivateLinkAttachmentConnectionWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAttachmentConnectionParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// GetNetworkingV1PrivateLinkAttachmentConnectionWithResponse Read a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link attachment connection.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-attachment-connections/{id} (the `GetNetworkingV1PrivateLinkAttachmentConnection` operationId).
	GetNetworkingV1PrivateLinkAttachmentConnectionWithResponse(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAttachmentConnectionParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// UpdateNetworkingV1PrivateLinkAttachmentConnectionWithBodyWithResponse Update a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment connection.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-attachment-connections/{id} (the `UpdateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentConnectionWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// UpdateNetworkingV1PrivateLinkAttachmentConnectionWithResponse Update a Private Link Attachment Connection
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment connection.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-attachment-connections/{id} (the `UpdateNetworkingV1PrivateLinkAttachmentConnection` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentConnectionWithResponse(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAttachmentConnectionJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse, error)

	// ListNetworkingV1PrivateLinkAttachmentsWithResponse List of Private Link Attachments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all private link attachments.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-attachments (the `ListNetworkingV1PrivateLinkAttachments` operationId).
	ListNetworkingV1PrivateLinkAttachmentsWithResponse(ctx context.Context, params *ListNetworkingV1PrivateLinkAttachmentsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1PrivateLinkAttachmentsResponse, error)

	// CreateNetworkingV1PrivateLinkAttachmentWithBodyWithResponse Create a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-attachments (the `CreateNetworkingV1PrivateLinkAttachment` operationId).
	CreateNetworkingV1PrivateLinkAttachmentWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAttachmentResponse, error)

	// CreateNetworkingV1PrivateLinkAttachmentWithResponse Create a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a private link attachment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/private-link-attachments (the `CreateNetworkingV1PrivateLinkAttachment` operationId).
	CreateNetworkingV1PrivateLinkAttachmentWithResponse(ctx context.Context, body CreateNetworkingV1PrivateLinkAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1PrivateLinkAttachmentResponse, error)

	// DeleteNetworkingV1PrivateLinkAttachmentWithResponse Delete a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a private link attachment.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/private-link-attachments/{id} (the `DeleteNetworkingV1PrivateLinkAttachment` operationId).
	DeleteNetworkingV1PrivateLinkAttachmentWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1PrivateLinkAttachmentParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1PrivateLinkAttachmentResponse, error)

	// GetNetworkingV1PrivateLinkAttachmentWithResponse Read a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a private link attachment.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/private-link-attachments/{id} (the `GetNetworkingV1PrivateLinkAttachment` operationId).
	GetNetworkingV1PrivateLinkAttachmentWithResponse(ctx context.Context, id string, params *GetNetworkingV1PrivateLinkAttachmentParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1PrivateLinkAttachmentResponse, error)

	// UpdateNetworkingV1PrivateLinkAttachmentWithBodyWithResponse Update a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-attachments/{id} (the `UpdateNetworkingV1PrivateLinkAttachment` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAttachmentResponse, error)

	// UpdateNetworkingV1PrivateLinkAttachmentWithResponse Update a Private Link Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a private link attachment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/private-link-attachments/{id} (the `UpdateNetworkingV1PrivateLinkAttachment` operationId).
	UpdateNetworkingV1PrivateLinkAttachmentWithResponse(ctx context.Context, id string, body UpdateNetworkingV1PrivateLinkAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1PrivateLinkAttachmentResponse, error)

	// ListNetworkingV1TransitGatewayAttachmentsWithResponse List of Transit Gateway Attachments
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all transit gateway attachments.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/transit-gateway-attachments (the `ListNetworkingV1TransitGatewayAttachments` operationId).
	ListNetworkingV1TransitGatewayAttachmentsWithResponse(ctx context.Context, params *ListNetworkingV1TransitGatewayAttachmentsParams, reqEditors ...RequestEditorFn) (*ListNetworkingV1TransitGatewayAttachmentsResponse, error)

	// CreateNetworkingV1TransitGatewayAttachmentWithBodyWithResponse Create a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a transit gateway attachment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/transit-gateway-attachments (the `CreateNetworkingV1TransitGatewayAttachment` operationId).
	CreateNetworkingV1TransitGatewayAttachmentWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNetworkingV1TransitGatewayAttachmentResponse, error)

	// CreateNetworkingV1TransitGatewayAttachmentWithResponse Create a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a transit gateway attachment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /networking/v1/transit-gateway-attachments (the `CreateNetworkingV1TransitGatewayAttachment` operationId).
	CreateNetworkingV1TransitGatewayAttachmentWithResponse(ctx context.Context, body CreateNetworkingV1TransitGatewayAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNetworkingV1TransitGatewayAttachmentResponse, error)

	// DeleteNetworkingV1TransitGatewayAttachmentWithResponse Delete a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a transit gateway attachment.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /networking/v1/transit-gateway-attachments/{id} (the `DeleteNetworkingV1TransitGatewayAttachment` operationId).
	DeleteNetworkingV1TransitGatewayAttachmentWithResponse(ctx context.Context, id string, params *DeleteNetworkingV1TransitGatewayAttachmentParams, reqEditors ...RequestEditorFn) (*DeleteNetworkingV1TransitGatewayAttachmentResponse, error)

	// GetNetworkingV1TransitGatewayAttachmentWithResponse Read a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a transit gateway attachment.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /networking/v1/transit-gateway-attachments/{id} (the `GetNetworkingV1TransitGatewayAttachment` operationId).
	GetNetworkingV1TransitGatewayAttachmentWithResponse(ctx context.Context, id string, params *GetNetworkingV1TransitGatewayAttachmentParams, reqEditors ...RequestEditorFn) (*GetNetworkingV1TransitGatewayAttachmentResponse, error)

	// UpdateNetworkingV1TransitGatewayAttachmentWithBodyWithResponse Update a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a transit gateway attachment.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/transit-gateway-attachments/{id} (the `UpdateNetworkingV1TransitGatewayAttachment` operationId).
	UpdateNetworkingV1TransitGatewayAttachmentWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1TransitGatewayAttachmentResponse, error)

	// UpdateNetworkingV1TransitGatewayAttachmentWithResponse Update a Transit Gateway Attachment
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a transit gateway attachment.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /networking/v1/transit-gateway-attachments/{id} (the `UpdateNetworkingV1TransitGatewayAttachment` operationId).
	UpdateNetworkingV1TransitGatewayAttachmentWithResponse(ctx context.Context, id string, body UpdateNetworkingV1TransitGatewayAttachmentJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNetworkingV1TransitGatewayAttachmentResponse, error)
}

func (r ListNetworkingV1AccessPointsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1AccessPoints200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Gateway     interface{} `json:"gateway,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1AccessPoints200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1AccessPointsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1AccessPointsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1AccessPointsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1AccessPointsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1AccessPointsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1AccessPointsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1AccessPointsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1AccessPointsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1AccessPoint202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1AccessPoint202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Access Point
	Status NetworkingV1AccessPointStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1AccessPointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1AccessPointResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1AccessPointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1AccessPointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1AccessPointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1AccessPointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1AccessPointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1AccessPointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1AccessPointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1AccessPointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1AccessPointResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1AccessPointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1AccessPointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1AccessPointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1AccessPointResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1AccessPoint200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1AccessPoint200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Access Point
	Status NetworkingV1AccessPointStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1AccessPointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1AccessPointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1AccessPointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1AccessPointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1AccessPointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1AccessPointResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1AccessPointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1AccessPointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1AccessPointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1AccessPoint200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1AccessPoint200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Access Point
	Status NetworkingV1AccessPointStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1AccessPointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1AccessPointResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1AccessPointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1AccessPointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1AccessPointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1DnsForwardersResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1DnsForwarders200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Gateway     interface{} `json:"gateway,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1DnsForwarders200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1DnsForwardersResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1DnsForwardersResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1DnsForwardersResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1DnsForwardersResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1DnsForwardersResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1DnsForwardersResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1DnsForwardersResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1DnsForwardersResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1DnsForwarder202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1DnsForwarder202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Forwarder
	Status NetworkingV1DnsForwarderStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1DnsForwarderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1DnsForwarderResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1DnsForwarderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1DnsForwarderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1DnsForwarderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1DnsForwarderResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1DnsForwarderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1DnsForwarderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1DnsForwarderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1DnsForwarder200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1DnsForwarder200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Forwarder
	Status NetworkingV1DnsForwarderStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1DnsForwarderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1DnsForwarderResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1DnsForwarderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1DnsForwarderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1DnsForwarderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1DnsForwarder200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1DnsForwarder200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Forwarder
	Status NetworkingV1DnsForwarderStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1DnsForwarderResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1DnsForwarderResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1DnsForwarderResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1DnsForwarderResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1DnsRecordsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1DnsRecords200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Gateway     interface{} `json:"gateway,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1DnsRecords200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1DnsRecordsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1DnsRecordsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1DnsRecordsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1DnsRecordsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1DnsRecordsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1DnsRecordsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1DnsRecordsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1DnsRecordsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1DnsRecord202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1DnsRecord202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Record
	Status NetworkingV1DnsRecordStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1DnsRecordResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1DnsRecordResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1DnsRecordResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1DnsRecordResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1DnsRecordResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1DnsRecordResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1DnsRecordResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1DnsRecordResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1DnsRecordResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1DnsRecordResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1DnsRecordResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1DnsRecordResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1DnsRecordResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1DnsRecordResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1DnsRecord200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1DnsRecord200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Record
	Status NetworkingV1DnsRecordStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1DnsRecordResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1DnsRecordResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1DnsRecordResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1DnsRecordResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1DnsRecordResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1DnsRecord200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1DnsRecord200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Gateway     interface{} `json:"gateway,omitempty"`
	} `json:"spec"`

	// Status The status of the Dns Record
	Status NetworkingV1DnsRecordStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1DnsRecordResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1DnsRecordResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1DnsRecordResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1DnsRecordResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1DnsRecordResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1GatewaysResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1Gateways200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1Gateways200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1GatewaysResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1GatewaysResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1GatewaysResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1GatewaysResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1GatewaysResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1GatewaysResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1GatewaysResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1GatewaysResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1GatewayResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1Gateway202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1Gateway202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Gateway
	Status NetworkingV1GatewayStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1GatewayResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1GatewayResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1GatewayResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1GatewayResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1GatewayResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1GatewayResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1GatewayResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1GatewayResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1GatewayResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1GatewayResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1GatewayResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1GatewayResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1GatewayResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1GatewayResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1GatewayResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1GatewayResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1GatewayResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1GatewayResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1GatewayResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1GatewayResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1GatewayResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1Gateway200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1Gateway200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Gateway
	Status NetworkingV1GatewayStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1GatewayResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1GatewayResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1GatewayResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1GatewayResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1GatewayResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1GatewayResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1GatewayResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1GatewayResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1GatewayResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1Gateway200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1Gateway200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Gateway
	Status NetworkingV1GatewayStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1GatewayResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1GatewayResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1GatewayResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1GatewayResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1GatewayResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1IpAddressesResponse) GetJSON200() *NetworkingV1IpAddressList {
	return r.JSON200
}
func (r ListNetworkingV1IpAddressesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1IpAddressesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1IpAddressesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1IpAddressesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1IpAddressesResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1IpAddressesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1IpAddressesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1IpAddressesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment        interface{} `json:"environment,omitempty"`
			Network            interface{} `json:"network,omitempty"`
			NetworkLinkService interface{} `json:"network_link_service,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1NetworkLinkEndpoints200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1NetworkLinkEndpointsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1NetworkLinkEndpoint202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment        interface{} `json:"environment,omitempty"`
		Network            interface{} `json:"network,omitempty"`
		NetworkLinkService interface{} `json:"network_link_service,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Endpoint
	Status NetworkingV1NetworkLinkEndpointStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1NetworkLinkEndpointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1NetworkLinkEndpointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind `json:"kind"`
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
		Environment        interface{} `json:"environment,omitempty"`
		Network            interface{} `json:"network,omitempty"`
		NetworkLinkService interface{} `json:"network_link_service,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Endpoint
	Status NetworkingV1NetworkLinkEndpointStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1NetworkLinkEndpointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1NetworkLinkEndpoint200JSONResponseBodyKind `json:"kind"`
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
		Environment        interface{} `json:"environment,omitempty"`
		Network            interface{} `json:"network,omitempty"`
		NetworkLinkService interface{} `json:"network_link_service,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Endpoint
	Status NetworkingV1NetworkLinkEndpointStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1NetworkLinkEndpointResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment        interface{} `json:"environment,omitempty"`
			NetworkLinkService interface{} `json:"network_link_service,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1NetworkLinkServiceAssociations200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1NetworkLinkServiceAssociationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1NetworkLinkServiceAssociation200JSONResponseBodyKind `json:"kind"`
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
		Environment        interface{} `json:"environment,omitempty"`
		NetworkLinkService interface{} `json:"network_link_service,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Service Association
	Status NetworkingV1NetworkLinkServiceAssociationStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1NetworkLinkServiceAssociationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1NetworkLinkServicesResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1NetworkLinkServices200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1NetworkLinkServices200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1NetworkLinkServicesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1NetworkLinkServicesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1NetworkLinkServicesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1NetworkLinkServicesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1NetworkLinkServicesResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1NetworkLinkServicesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1NetworkLinkServicesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1NetworkLinkServicesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1NetworkLinkService202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1NetworkLinkService202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Service
	Status NetworkingV1NetworkLinkServiceStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1NetworkLinkServiceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1NetworkLinkServiceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1NetworkLinkService200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Service
	Status NetworkingV1NetworkLinkServiceStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1NetworkLinkServiceResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1NetworkLinkServiceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1NetworkLinkServiceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1NetworkLinkServiceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1NetworkLinkService200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1NetworkLinkService200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Network Link Service
	Status NetworkingV1NetworkLinkServiceStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1NetworkLinkServiceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1NetworksResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1Networks200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1Networks200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1NetworksResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1NetworksResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1NetworksResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1NetworksResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1NetworksResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1NetworksResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1NetworksResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1NetworksResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1NetworkResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1Network202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1Network202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Network
	Status NetworkingV1NetworkStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1NetworkResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1NetworkResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1NetworkResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1NetworkResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1NetworkResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1NetworkResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1NetworkResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1NetworkResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1NetworkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1NetworkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1NetworkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1NetworkResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1NetworkResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1NetworkResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1NetworkResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1NetworkResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1NetworkResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1NetworkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1NetworkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1NetworkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1NetworkResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1Network200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1Network200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Network
	Status NetworkingV1NetworkStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1NetworkResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1NetworkResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1NetworkResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1NetworkResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1NetworkResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1NetworkResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1NetworkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1NetworkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1NetworkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1Network200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1Network200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Network
	Status NetworkingV1NetworkStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1NetworkResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1NetworkResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1NetworkResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1NetworkResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1NetworkResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1PeeringsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1Peerings200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1Peerings200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1PeeringsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1PeeringsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1PeeringsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1PeeringsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1PeeringsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1PeeringsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1PeeringsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1PeeringsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1PeeringResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1Peering202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1Peering202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Peering
	Status NetworkingV1PeeringStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1PeeringResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1PeeringResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1PeeringResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1PeeringResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1PeeringResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1PeeringResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1PeeringResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1PeeringResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1PeeringResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1PeeringResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1PeeringResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1PeeringResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1PeeringResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1PeeringResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1PeeringResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1PeeringResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1PeeringResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1PeeringResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1PeeringResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1PeeringResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1PeeringResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1Peering200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1Peering200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Peering
	Status NetworkingV1PeeringStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1PeeringResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1PeeringResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1PeeringResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1PeeringResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1PeeringResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1PeeringResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1PeeringResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1PeeringResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1PeeringResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1Peering200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1Peering200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Peering
	Status NetworkingV1PeeringStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1PeeringResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1PeeringResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1PeeringResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1PeeringResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1PeeringResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1PrivateLinkAccesses200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1PrivateLinkAccessesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1PrivateLinkAccess202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Access
	Status NetworkingV1PrivateLinkAccessStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1PrivateLinkAccessResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1PrivateLinkAccessResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1PrivateLinkAccess200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Access
	Status NetworkingV1PrivateLinkAccessStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1PrivateLinkAccessResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1PrivateLinkAccessResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1PrivateLinkAccessResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1PrivateLinkAccessResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1PrivateLinkAccess200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Access
	Status NetworkingV1PrivateLinkAccessStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1PrivateLinkAccessResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment           interface{} `json:"environment,omitempty"`
			PrivateLinkAttachment interface{} `json:"private_link_attachment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1PrivateLinkAttachmentConnections200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1PrivateLinkAttachmentConnectionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1PrivateLinkAttachmentConnection202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment           interface{} `json:"environment,omitempty"`
		PrivateLinkAttachment interface{} `json:"private_link_attachment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment Connection
	Status NetworkingV1PrivateLinkAttachmentConnectionStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1PrivateLinkAttachmentConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1PrivateLinkAttachmentConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind `json:"kind"`
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
		Environment           interface{} `json:"environment,omitempty"`
		PrivateLinkAttachment interface{} `json:"private_link_attachment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment Connection
	Status NetworkingV1PrivateLinkAttachmentConnectionStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1PrivateLinkAttachmentConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1PrivateLinkAttachmentConnection200JSONResponseBodyKind `json:"kind"`
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
		Environment           interface{} `json:"environment,omitempty"`
		PrivateLinkAttachment interface{} `json:"private_link_attachment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment Connection
	Status NetworkingV1PrivateLinkAttachmentConnectionStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1PrivateLinkAttachmentConnectionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1PrivateLinkAttachments200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1PrivateLinkAttachmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1PrivateLinkAttachment202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment
	Status NetworkingV1PrivateLinkAttachmentStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1PrivateLinkAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1PrivateLinkAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment
	Status NetworkingV1PrivateLinkAttachmentStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1PrivateLinkAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1PrivateLinkAttachment200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
	} `json:"spec"`

	// Status The status of the Private Link Attachment
	Status NetworkingV1PrivateLinkAttachmentStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1PrivateLinkAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyApiVersion `json:"api_version"`
	Data       []struct {
		Spec *struct {
			Environment interface{} `json:"environment,omitempty"`
			Network     interface{} `json:"network,omitempty"`
		} `json:"spec,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     ListNetworkingV1TransitGatewayAttachments200JSONResponseBodyKind `json:"kind"`
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
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNetworkingV1TransitGatewayAttachmentsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON202() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNetworkingV1TransitGatewayAttachment202JSONResponseBodyKind `json:"kind,omitempty"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Transit Gateway Attachment
	Status NetworkingV1TransitGatewayAttachmentStatus `json:"status"`
} {
	return r.JSON202
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNetworkingV1TransitGatewayAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNetworkingV1TransitGatewayAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Transit Gateway Attachment
	Status NetworkingV1TransitGatewayAttachmentStatus `json:"status"`
} {
	return r.JSON200
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNetworkingV1TransitGatewayAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNetworkingV1TransitGatewayAttachment200JSONResponseBodyKind `json:"kind"`
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
		Environment interface{} `json:"environment,omitempty"`
		Network     interface{} `json:"network,omitempty"`
	} `json:"spec"`

	// Status The status of the Transit Gateway Attachment
	Status NetworkingV1TransitGatewayAttachmentStatus `json:"status"`
} {
	return r.JSON200
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNetworkingV1TransitGatewayAttachmentResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
