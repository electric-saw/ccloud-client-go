package notifications

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
	InApp NotificationsV1InAppTargetKind = "InApp"
)

func (e NotificationsV1InAppTargetKind) Valid() bool {
	switch e {
	case InApp:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1IntegrationApiVersionNotificationsv1 NotificationsV1IntegrationApiVersion = "notifications/v1"
)

func (e NotificationsV1IntegrationApiVersion) Valid() bool {
	switch e {
	case NotificationsV1IntegrationApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1IntegrationKindIntegration NotificationsV1IntegrationKind = "Integration"
)

func (e NotificationsV1IntegrationKind) Valid() bool {
	switch e {
	case NotificationsV1IntegrationKindIntegration:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1IntegrationListApiVersionNotificationsv1 NotificationsV1IntegrationListApiVersion = "notifications/v1"
)

func (e NotificationsV1IntegrationListApiVersion) Valid() bool {
	switch e {
	case NotificationsV1IntegrationListApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1IntegrationListDataApiVersionNotificationsv1 NotificationsV1IntegrationListDataApiVersion = "notifications/v1"
)

func (e NotificationsV1IntegrationListDataApiVersion) Valid() bool {
	switch e {
	case NotificationsV1IntegrationListDataApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1IntegrationListDataKindIntegration NotificationsV1IntegrationListDataKind = "Integration"
)

func (e NotificationsV1IntegrationListDataKind) Valid() bool {
	switch e {
	case NotificationsV1IntegrationListDataKindIntegration:
		return true
	default:
		return false
	}
}

const (
	IntegrationList NotificationsV1IntegrationListKind = "IntegrationList"
)

func (e NotificationsV1IntegrationListKind) Valid() bool {
	switch e {
	case IntegrationList:
		return true
	default:
		return false
	}
}

const (
	MsTeams NotificationsV1MsTeamsTargetKind = "MsTeams"
)

func (e NotificationsV1MsTeamsTargetKind) Valid() bool {
	switch e {
	case MsTeams:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1NotificationTypeApiVersionNotificationsv1 NotificationsV1NotificationTypeApiVersion = "notifications/v1"
)

func (e NotificationsV1NotificationTypeApiVersion) Valid() bool {
	switch e {
	case NotificationsV1NotificationTypeApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1NotificationTypeKindNotificationType NotificationsV1NotificationTypeKind = "NotificationType"
)

func (e NotificationsV1NotificationTypeKind) Valid() bool {
	switch e {
	case NotificationsV1NotificationTypeKindNotificationType:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1NotificationTypeListApiVersionNotificationsv1 NotificationsV1NotificationTypeListApiVersion = "notifications/v1"
)

func (e NotificationsV1NotificationTypeListApiVersion) Valid() bool {
	switch e {
	case NotificationsV1NotificationTypeListApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1NotificationTypeListDataApiVersionNotificationsv1 NotificationsV1NotificationTypeListDataApiVersion = "notifications/v1"
)

func (e NotificationsV1NotificationTypeListDataApiVersion) Valid() bool {
	switch e {
	case NotificationsV1NotificationTypeListDataApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1NotificationTypeListDataKindNotificationType NotificationsV1NotificationTypeListDataKind = "NotificationType"
)

func (e NotificationsV1NotificationTypeListDataKind) Valid() bool {
	switch e {
	case NotificationsV1NotificationTypeListDataKindNotificationType:
		return true
	default:
		return false
	}
}

const (
	NotificationTypeList NotificationsV1NotificationTypeListKind = "NotificationTypeList"
)

func (e NotificationsV1NotificationTypeListKind) Valid() bool {
	switch e {
	case NotificationTypeList:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourcePreferenceApiVersionNotificationsv1 NotificationsV1ResourcePreferenceApiVersion = "notifications/v1"
)

func (e NotificationsV1ResourcePreferenceApiVersion) Valid() bool {
	switch e {
	case NotificationsV1ResourcePreferenceApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourcePreferenceKindResourcePreference NotificationsV1ResourcePreferenceKind = "ResourcePreference"
)

func (e NotificationsV1ResourcePreferenceKind) Valid() bool {
	switch e {
	case NotificationsV1ResourcePreferenceKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourceSubscriptionApiVersionNotificationsv1 NotificationsV1ResourceSubscriptionApiVersion = "notifications/v1"
)

func (e NotificationsV1ResourceSubscriptionApiVersion) Valid() bool {
	switch e {
	case NotificationsV1ResourceSubscriptionApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourceSubscriptionKindResourceSubscription NotificationsV1ResourceSubscriptionKind = "ResourceSubscription"
)

func (e NotificationsV1ResourceSubscriptionKind) Valid() bool {
	switch e {
	case NotificationsV1ResourceSubscriptionKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourceSubscriptionListApiVersionNotificationsv1 NotificationsV1ResourceSubscriptionListApiVersion = "notifications/v1"
)

func (e NotificationsV1ResourceSubscriptionListApiVersion) Valid() bool {
	switch e {
	case NotificationsV1ResourceSubscriptionListApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourceSubscriptionListDataApiVersionNotificationsv1 NotificationsV1ResourceSubscriptionListDataApiVersion = "notifications/v1"
)

func (e NotificationsV1ResourceSubscriptionListDataApiVersion) Valid() bool {
	switch e {
	case NotificationsV1ResourceSubscriptionListDataApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1ResourceSubscriptionListDataKindResourceSubscription NotificationsV1ResourceSubscriptionListDataKind = "ResourceSubscription"
)

func (e NotificationsV1ResourceSubscriptionListDataKind) Valid() bool {
	switch e {
	case NotificationsV1ResourceSubscriptionListDataKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	ResourceSubscriptionList NotificationsV1ResourceSubscriptionListKind = "ResourceSubscriptionList"
)

func (e NotificationsV1ResourceSubscriptionListKind) Valid() bool {
	switch e {
	case ResourceSubscriptionList:
		return true
	default:
		return false
	}
}

const (
	RoleEmail NotificationsV1RoleEmailTargetKind = "RoleEmail"
)

func (e NotificationsV1RoleEmailTargetKind) Valid() bool {
	switch e {
	case RoleEmail:
		return true
	default:
		return false
	}
}

const (
	Slack NotificationsV1SlackTargetKind = "Slack"
)

func (e NotificationsV1SlackTargetKind) Valid() bool {
	switch e {
	case Slack:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SubscriptionApiVersionNotificationsv1 NotificationsV1SubscriptionApiVersion = "notifications/v1"
)

func (e NotificationsV1SubscriptionApiVersion) Valid() bool {
	switch e {
	case NotificationsV1SubscriptionApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SubscriptionKindSubscription NotificationsV1SubscriptionKind = "Subscription"
)

func (e NotificationsV1SubscriptionKind) Valid() bool {
	switch e {
	case NotificationsV1SubscriptionKindSubscription:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SubscriptionListApiVersionNotificationsv1 NotificationsV1SubscriptionListApiVersion = "notifications/v1"
)

func (e NotificationsV1SubscriptionListApiVersion) Valid() bool {
	switch e {
	case NotificationsV1SubscriptionListApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SubscriptionListDataApiVersionNotificationsv1 NotificationsV1SubscriptionListDataApiVersion = "notifications/v1"
)

func (e NotificationsV1SubscriptionListDataApiVersion) Valid() bool {
	switch e {
	case NotificationsV1SubscriptionListDataApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SubscriptionListDataKindSubscription NotificationsV1SubscriptionListDataKind = "Subscription"
)

func (e NotificationsV1SubscriptionListDataKind) Valid() bool {
	switch e {
	case NotificationsV1SubscriptionListDataKindSubscription:
		return true
	default:
		return false
	}
}

const (
	SubscriptionList NotificationsV1SubscriptionListKind = "SubscriptionList"
)

func (e NotificationsV1SubscriptionListKind) Valid() bool {
	switch e {
	case SubscriptionList:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1SummaryApiVersionNotificationsv1 NotificationsV1SummaryApiVersion = "notifications/v1"
)

func (e NotificationsV1SummaryApiVersion) Valid() bool {
	switch e {
	case NotificationsV1SummaryApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	Summary NotificationsV1SummaryKind = "Summary"
)

func (e NotificationsV1SummaryKind) Valid() bool {
	switch e {
	case Summary:
		return true
	default:
		return false
	}
}

const (
	UserEmail NotificationsV1UserEmailTargetKind = "UserEmail"
)

func (e NotificationsV1UserEmailTargetKind) Valid() bool {
	switch e {
	case UserEmail:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1UserNotificationApiVersionNotificationsv1 NotificationsV1UserNotificationApiVersion = "notifications/v1"
)

func (e NotificationsV1UserNotificationApiVersion) Valid() bool {
	switch e {
	case NotificationsV1UserNotificationApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1UserNotificationKindUserNotification NotificationsV1UserNotificationKind = "UserNotification"
)

func (e NotificationsV1UserNotificationKind) Valid() bool {
	switch e {
	case NotificationsV1UserNotificationKindUserNotification:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1UserNotificationListApiVersionNotificationsv1 NotificationsV1UserNotificationListApiVersion = "notifications/v1"
)

func (e NotificationsV1UserNotificationListApiVersion) Valid() bool {
	switch e {
	case NotificationsV1UserNotificationListApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1UserNotificationListDataApiVersionNotificationsv1 NotificationsV1UserNotificationListDataApiVersion = "notifications/v1"
)

func (e NotificationsV1UserNotificationListDataApiVersion) Valid() bool {
	switch e {
	case NotificationsV1UserNotificationListDataApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	NotificationsV1UserNotificationListDataKindUserNotification NotificationsV1UserNotificationListDataKind = "UserNotification"
)

func (e NotificationsV1UserNotificationListDataKind) Valid() bool {
	switch e {
	case NotificationsV1UserNotificationListDataKindUserNotification:
		return true
	default:
		return false
	}
}

const (
	UserNotificationList NotificationsV1UserNotificationListKind = "UserNotificationList"
)

func (e NotificationsV1UserNotificationListKind) Valid() bool {
	switch e {
	case UserNotificationList:
		return true
	default:
		return false
	}
}

const (
	Webhook NotificationsV1WebhookTargetKind = "Webhook"
)

func (e NotificationsV1WebhookTargetKind) Valid() bool {
	switch e {
	case Webhook:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1IntegrationJSONBodyApiVersionNotificationsv1 CreateNotificationsV1IntegrationJSONBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1IntegrationJSONBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1IntegrationJSONBodyKindIntegration CreateNotificationsV1IntegrationJSONBodyKind = "Integration"
)

func (e CreateNotificationsV1IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1IntegrationJSONBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1Integration201JSONResponseBodyApiVersionNotificationsv1 CreateNotificationsV1Integration201JSONResponseBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1Integration201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1Integration201JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1Integration201JSONResponseBodyKindIntegration CreateNotificationsV1Integration201JSONResponseBodyKind = "Integration"
)

func (e CreateNotificationsV1Integration201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1Integration201JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1Integration200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1Integration200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1Integration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1Integration200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1Integration200JSONResponseBodyKindIntegration GetNotificationsV1Integration200JSONResponseBodyKind = "Integration"
)

func (e GetNotificationsV1Integration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1Integration200JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1Integration200JSONResponseBodyApiVersionNotificationsv1 UpdateNotificationsV1Integration200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e UpdateNotificationsV1Integration200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNotificationsV1Integration200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1Integration200JSONResponseBodyKindIntegration UpdateNotificationsV1Integration200JSONResponseBodyKind = "Integration"
)

func (e UpdateNotificationsV1Integration200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNotificationsV1Integration200JSONResponseBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	TestNotificationsV1IntegrationJSONBodyApiVersionNotificationsv1 TestNotificationsV1IntegrationJSONBodyApiVersion = "notifications/v1"
)

func (e TestNotificationsV1IntegrationJSONBodyApiVersion) Valid() bool {
	switch e {
	case TestNotificationsV1IntegrationJSONBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	TestNotificationsV1IntegrationJSONBodyKindIntegration TestNotificationsV1IntegrationJSONBodyKind = "Integration"
)

func (e TestNotificationsV1IntegrationJSONBodyKind) Valid() bool {
	switch e {
	case TestNotificationsV1IntegrationJSONBodyKindIntegration:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1NotificationType200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1NotificationType200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1NotificationType200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1NotificationType200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1NotificationType200JSONResponseBodyKindNotificationType GetNotificationsV1NotificationType200JSONResponseBodyKind = "NotificationType"
)

func (e GetNotificationsV1NotificationType200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1NotificationType200JSONResponseBodyKindNotificationType:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourcePreferenceJSONBodyApiVersionNotificationsv1 CreateNotificationsV1ResourcePreferenceJSONBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1ResourcePreferenceJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourcePreferenceJSONBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourcePreferenceJSONBodyKindResourcePreference CreateNotificationsV1ResourcePreferenceJSONBodyKind = "ResourcePreference"
)

func (e CreateNotificationsV1ResourcePreferenceJSONBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourcePreferenceJSONBodyKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourcePreference201JSONResponseBodyApiVersionNotificationsv1 CreateNotificationsV1ResourcePreference201JSONResponseBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1ResourcePreference201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourcePreference201JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourcePreference201JSONResponseBodyKindResourcePreference CreateNotificationsV1ResourcePreference201JSONResponseBodyKind = "ResourcePreference"
)

func (e CreateNotificationsV1ResourcePreference201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourcePreference201JSONResponseBodyKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourcePreference200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1ResourcePreference200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1ResourcePreference200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1ResourcePreference200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourcePreference200JSONResponseBodyKindResourcePreference GetNotificationsV1ResourcePreference200JSONResponseBodyKind = "ResourcePreference"
)

func (e GetNotificationsV1ResourcePreference200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1ResourcePreference200JSONResponseBodyKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1ResourcePreference200JSONResponseBodyApiVersionNotificationsv1 UpdateNotificationsV1ResourcePreference200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e UpdateNotificationsV1ResourcePreference200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNotificationsV1ResourcePreference200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1ResourcePreference200JSONResponseBodyKindResourcePreference UpdateNotificationsV1ResourcePreference200JSONResponseBodyKind = "ResourcePreference"
)

func (e UpdateNotificationsV1ResourcePreference200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNotificationsV1ResourcePreference200JSONResponseBodyKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyKindResourcePreference GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyKind = "ResourcePreference"
)

func (e GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyKindResourcePreference:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourceSubscriptionJSONBodyApiVersionNotificationsv1 CreateNotificationsV1ResourceSubscriptionJSONBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1ResourceSubscriptionJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourceSubscriptionJSONBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourceSubscriptionJSONBodyKindResourceSubscription CreateNotificationsV1ResourceSubscriptionJSONBodyKind = "ResourceSubscription"
)

func (e CreateNotificationsV1ResourceSubscriptionJSONBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourceSubscriptionJSONBodyKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourceSubscription201JSONResponseBodyApiVersionNotificationsv1 CreateNotificationsV1ResourceSubscription201JSONResponseBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1ResourceSubscription201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourceSubscription201JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1ResourceSubscription201JSONResponseBodyKindResourceSubscription CreateNotificationsV1ResourceSubscription201JSONResponseBodyKind = "ResourceSubscription"
)

func (e CreateNotificationsV1ResourceSubscription201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1ResourceSubscription201JSONResponseBodyKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourceSubscription200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1ResourceSubscription200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1ResourceSubscription200JSONResponseBodyKindResourceSubscription GetNotificationsV1ResourceSubscription200JSONResponseBodyKind = "ResourceSubscription"
)

func (e GetNotificationsV1ResourceSubscription200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1ResourceSubscription200JSONResponseBodyKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1ResourceSubscription200JSONResponseBodyApiVersionNotificationsv1 UpdateNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e UpdateNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNotificationsV1ResourceSubscription200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1ResourceSubscription200JSONResponseBodyKindResourceSubscription UpdateNotificationsV1ResourceSubscription200JSONResponseBodyKind = "ResourceSubscription"
)

func (e UpdateNotificationsV1ResourceSubscription200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNotificationsV1ResourceSubscription200JSONResponseBodyKindResourceSubscription:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1SubscriptionJSONBodyApiVersionNotificationsv1 CreateNotificationsV1SubscriptionJSONBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1SubscriptionJSONBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1SubscriptionJSONBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1SubscriptionJSONBodyKindSubscription CreateNotificationsV1SubscriptionJSONBodyKind = "Subscription"
)

func (e CreateNotificationsV1SubscriptionJSONBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1SubscriptionJSONBodyKindSubscription:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1Subscription201JSONResponseBodyApiVersionNotificationsv1 CreateNotificationsV1Subscription201JSONResponseBodyApiVersion = "notifications/v1"
)

func (e CreateNotificationsV1Subscription201JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case CreateNotificationsV1Subscription201JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	CreateNotificationsV1Subscription201JSONResponseBodyKindSubscription CreateNotificationsV1Subscription201JSONResponseBodyKind = "Subscription"
)

func (e CreateNotificationsV1Subscription201JSONResponseBodyKind) Valid() bool {
	switch e {
	case CreateNotificationsV1Subscription201JSONResponseBodyKindSubscription:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1Subscription200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1Subscription200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1Subscription200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1Subscription200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1Subscription200JSONResponseBodyKindSubscription GetNotificationsV1Subscription200JSONResponseBodyKind = "Subscription"
)

func (e GetNotificationsV1Subscription200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1Subscription200JSONResponseBodyKindSubscription:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1Subscription200JSONResponseBodyApiVersionNotificationsv1 UpdateNotificationsV1Subscription200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e UpdateNotificationsV1Subscription200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNotificationsV1Subscription200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1Subscription200JSONResponseBodyKindSubscription UpdateNotificationsV1Subscription200JSONResponseBodyKind = "Subscription"
)

func (e UpdateNotificationsV1Subscription200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNotificationsV1Subscription200JSONResponseBodyKindSubscription:
		return true
	default:
		return false
	}
}

const (
	MinusReceivedAt ListNotificationsV1UserNotificationsParamsSort = "-received_at"
	MinusSeverity   ListNotificationsV1UserNotificationsParamsSort = "-severity"
	ReceivedAt      ListNotificationsV1UserNotificationsParamsSort = "received_at"
	Severity        ListNotificationsV1UserNotificationsParamsSort = "severity"
)

func (e ListNotificationsV1UserNotificationsParamsSort) Valid() bool {
	switch e {
	case MinusReceivedAt:
		return true
	case MinusSeverity:
		return true
	case ReceivedAt:
		return true
	case Severity:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1UserNotification200JSONResponseBodyApiVersionNotificationsv1 GetNotificationsV1UserNotification200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e GetNotificationsV1UserNotification200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case GetNotificationsV1UserNotification200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	GetNotificationsV1UserNotification200JSONResponseBodyKindUserNotification GetNotificationsV1UserNotification200JSONResponseBodyKind = "UserNotification"
)

func (e GetNotificationsV1UserNotification200JSONResponseBodyKind) Valid() bool {
	switch e {
	case GetNotificationsV1UserNotification200JSONResponseBodyKindUserNotification:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1UserNotification200JSONResponseBodyApiVersionNotificationsv1 UpdateNotificationsV1UserNotification200JSONResponseBodyApiVersion = "notifications/v1"
)

func (e UpdateNotificationsV1UserNotification200JSONResponseBodyApiVersion) Valid() bool {
	switch e {
	case UpdateNotificationsV1UserNotification200JSONResponseBodyApiVersionNotificationsv1:
		return true
	default:
		return false
	}
}

const (
	UpdateNotificationsV1UserNotification200JSONResponseBodyKindUserNotification UpdateNotificationsV1UserNotification200JSONResponseBodyKind = "UserNotification"
)

func (e UpdateNotificationsV1UserNotification200JSONResponseBodyKind) Valid() bool {
	switch e {
	case UpdateNotificationsV1UserNotification200JSONResponseBodyKindUserNotification:
		return true
	default:
		return false
	}
}

type AssignmentsType = string
type BooleanFilter = bool
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
type NotificationsV1InAppTarget struct {
	// Kind Integration Type
	Kind NotificationsV1InAppTargetKind `json:"kind"`

	// User Reference to the user the in-app target belongs to.
	User GlobalObjectReference `json:"user"`
}
type NotificationsV1InAppTargetKind string
type NotificationsV1Integration struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1IntegrationApiVersion `json:"api_version,omitempty"`

	// Description A human readable description for the particular integration
	Description *string `json:"description,omitempty"`

	// DisplayName A human readable name for the particular integration
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1IntegrationKind `json:"kind,omitempty"`
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

	// Target Integration-specific details (integration targets)
	Target *NotificationsV1Target `json:"target,omitempty"`
}
type NotificationsV1IntegrationApiVersion string
type NotificationsV1IntegrationKind string
type NotificationsV1IntegrationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1IntegrationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NotificationsV1IntegrationListDataApiVersion `json:"api_version,omitempty"`

		// Description A human readable description for the particular integration
		Description *string `json:"description,omitempty"`

		// DisplayName A human readable name for the particular integration
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NotificationsV1IntegrationListDataKind `json:"kind,omitempty"`
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

		// Target Integration-specific details (integration targets)
		Target NotificationsV1Target `json:"target"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NotificationsV1IntegrationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NotificationsV1IntegrationListApiVersion string
type NotificationsV1IntegrationListDataApiVersion string
type NotificationsV1IntegrationListDataKind string
type NotificationsV1IntegrationListKind string
type NotificationsV1MsTeamsTarget struct {
	// Kind Integration Type
	Kind NotificationsV1MsTeamsTargetKind `json:"kind"`

	// WebhookUrl MS Teams Webhook URL for the particular team channel
	WebhookUrl string `json:"webhook_url"`
}
type NotificationsV1MsTeamsTargetKind string
type NotificationsV1NotificationAction struct {
	// Identifier Stable identifier for the action, suitable for analytics. Stable
	// across notification deliveries that recommend the same action.
	Identifier string `json:"identifier"`

	// Role Visual prominence of the action. `PRIMARY` is the recommended
	// default action; `SECONDARY` is shown alongside as a less prominent
	// option.
	Role string `json:"role"`

	// Url Confluent Cloud URL this action navigates to.
	Url string `json:"url"`
}
type NotificationsV1NotificationType struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1NotificationTypeApiVersion `json:"api_version,omitempty"`

	// Category Represents the group with which the notification is associated.
	// Notifications are grouped under certain categories for better organization.
	// - BILLING_LICENSING: All billing, payments or licensing related notifications are grouped here.
	// - SECURITY: All Confluent Cloud and Platform security related notifications are grouped here.
	// - SERVICE: All Confluent services (eg. Kafka, Schema Registry, Connect etc.) related notifications are
	//   grouped here.
	// - ACCOUNT: All Confluent account related notifications are grouped here.
	// For example: Billing, payment or license related notifications are grouped in BILLING_LICENSING category.
	Category *string `json:"category,omitempty"`

	// Description Human readable description of the notification type
	Description *string `json:"description,omitempty"`

	// DisplayName Human readable display name of the notification type
	DisplayName *string `json:"display_name,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// IsIncludedInPlan Whether this notification is available to subscribe or not
	// as per the user's current billing plan.
	IsIncludedInPlan *bool `json:"is_included_in_plan,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1NotificationTypeKind `json:"kind,omitempty"`
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

	// ResourceType The type of resource this notification is associated with. Optional field.
	ResourceType *string `json:"resource_type,omitempty"`

	// Severity Severity indicates the impact of this notification.
	// - CRITICAL: a high impact notification which needs immediate attention.
	// - WARN: a warning notification which can be addressed now or later.
	// - INFO: an informational notification.
	Severity *string `json:"severity,omitempty"`

	// SubscriptionPriority Indicates whether the notification is auto-subscribed and if the user can opt-out.
	// - REQUIRED: the user is auto-subscribed to this notification and can't opt-out.
	// - RECOMMENDED: the user is auto-subscribed to this notification and can opt-out.
	// - OPTIONAL: the user is not auto-subscribed to this notification but can explicitly subscribe to it.
	SubscriptionPriority *string `json:"subscription_priority,omitempty"`
}
type NotificationsV1NotificationTypeApiVersion string
type NotificationsV1NotificationTypeKind string
type NotificationsV1NotificationTypeList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1NotificationTypeListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NotificationsV1NotificationTypeListDataApiVersion `json:"api_version,omitempty"`

		// Category Represents the group with which the notification is associated.
		// Notifications are grouped under certain categories for better organization.
		// - BILLING_LICENSING: All billing, payments or licensing related notifications are grouped here.
		// - SECURITY: All Confluent Cloud and Platform security related notifications are grouped here.
		// - SERVICE: All Confluent services (eg. Kafka, Schema Registry, Connect etc.) related notifications are
		//   grouped here.
		// - ACCOUNT: All Confluent account related notifications are grouped here.
		// For example: Billing, payment or license related notifications are grouped in BILLING_LICENSING category.
		Category string `json:"category"`

		// Description Human readable description of the notification type
		Description string `json:"description"`

		// DisplayName Human readable display name of the notification type
		DisplayName string `json:"display_name"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// IsIncludedInPlan Whether this notification is available to subscribe or not
		// as per the user's current billing plan.
		IsIncludedInPlan bool `json:"is_included_in_plan"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NotificationsV1NotificationTypeListDataKind `json:"kind,omitempty"`
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

		// ResourceType The type of resource this notification is associated with. Optional field.
		ResourceType *string `json:"resource_type,omitempty"`

		// Severity Severity indicates the impact of this notification.
		// - CRITICAL: a high impact notification which needs immediate attention.
		// - WARN: a warning notification which can be addressed now or later.
		// - INFO: an informational notification.
		Severity string `json:"severity"`

		// SubscriptionPriority Indicates whether the notification is auto-subscribed and if the user can opt-out.
		// - REQUIRED: the user is auto-subscribed to this notification and can't opt-out.
		// - RECOMMENDED: the user is auto-subscribed to this notification and can opt-out.
		// - OPTIONAL: the user is not auto-subscribed to this notification but can explicitly subscribe to it.
		SubscriptionPriority string `json:"subscription_priority"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NotificationsV1NotificationTypeListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NotificationsV1NotificationTypeListApiVersion string
type NotificationsV1NotificationTypeListDataApiVersion string
type NotificationsV1NotificationTypeListDataKind string
type NotificationsV1NotificationTypeListKind string
type NotificationsV1RecommendedActions struct {
	// Content Human-readable body text describing the recommended actions.
	// Rendered as Markdown for `version: 1`.
	Content string `json:"content"`

	// Version Schema version of the `recommended_actions` payload. Increment when
	// the payload shape changes in a non-backward-compatible way.
	Version int32 `json:"version"`
}
type NotificationsV1ResourcePreference struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1ResourcePreferenceApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1ResourcePreferenceKind `json:"kind,omitempty"`
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

	// Resource Denotes the Confluent Cloud resource definition.
	Resource *string `json:"resource,omitempty"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType *string `json:"resource_type,omitempty"`
}
type NotificationsV1ResourcePreferenceApiVersion string
type NotificationsV1ResourcePreferenceKind string
type NotificationsV1ResourceSnapshot struct {
	// Crn CRN of the Confluent Cloud resource at delivery time.
	Crn string `json:"crn"`

	// DisplayName Human-readable name of the resource captured at notification time.
	// Does not update if the underlying resource is later renamed.
	DisplayName string `json:"display_name"`

	// Type The type of Confluent Cloud resource this notification relates to.
	Type string `json:"type"`
}
type NotificationsV1ResourceSubscription struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1ResourceSubscriptionApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations *[]GlobalObjectReference `json:"integrations,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1ResourceSubscriptionKind `json:"kind,omitempty"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType *GlobalObjectReference `json:"notification_type,omitempty"`

	// Resource Denotes the Confluent Cloud resource definition.
	Resource *string `json:"resource,omitempty"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType *string `json:"resource_type,omitempty"`
}
type NotificationsV1ResourceSubscriptionApiVersion string
type NotificationsV1ResourceSubscriptionKind string
type NotificationsV1ResourceSubscriptionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1ResourceSubscriptionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NotificationsV1ResourceSubscriptionListDataApiVersion `json:"api_version,omitempty"`

		// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
		// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
		// receive any notification for the resource.
		// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
		CurrentState *string `json:"current_state,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Integrations Integrations to which notifications are to be sent.
		Integrations []GlobalObjectReference `json:"integrations"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NotificationsV1ResourceSubscriptionListDataKind `json:"kind,omitempty"`
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

		// NotificationType The type of notification to subscribe to.
		NotificationType GlobalObjectReference `json:"notification_type"`

		// Resource Denotes the Confluent Cloud resource definition.
		Resource string `json:"resource"`

		// ResourceType Denotes the Confluent Cloud resource type.
		ResourceType string `json:"resource_type"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NotificationsV1ResourceSubscriptionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NotificationsV1ResourceSubscriptionListApiVersion string
type NotificationsV1ResourceSubscriptionListDataApiVersion string
type NotificationsV1ResourceSubscriptionListDataKind string
type NotificationsV1ResourceSubscriptionListKind string
type NotificationsV1RoleEmailTarget struct {
	// Kind Email Integration type for Role
	Kind NotificationsV1RoleEmailTargetKind `json:"kind"`

	// RoleName name of the role
	RoleName string `json:"role_name"`
}
type NotificationsV1RoleEmailTargetKind string
type NotificationsV1SlackTarget struct {
	// Kind Integration Type
	Kind NotificationsV1SlackTargetKind `json:"kind"`

	// WebhookUrl Slack Webhook URL for the particular Slack channel
	WebhookUrl string `json:"webhook_url"`
}
type NotificationsV1SlackTargetKind string
type NotificationsV1Subscription struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1SubscriptionApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the subscription. When the subscription is ENABLED, the user will receive
	// notification on the configured Integrations. If the subscription is DISABLED, the user will not
	// recieve any notification for the configured notification type. Note that, you cannot disable
	// a subscription for `REQUIRED` notification type.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations *[]GlobalObjectReference `json:"integrations,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1SubscriptionKind `json:"kind,omitempty"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType *GlobalObjectReference `json:"notification_type,omitempty"`
}
type NotificationsV1SubscriptionApiVersion string
type NotificationsV1SubscriptionKind string
type NotificationsV1SubscriptionList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1SubscriptionListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NotificationsV1SubscriptionListDataApiVersion `json:"api_version,omitempty"`

		// CurrentState Denotes the state of the subscription. When the subscription is ENABLED, the user will receive
		// notification on the configured Integrations. If the subscription is DISABLED, the user will not
		// recieve any notification for the configured notification type. Note that, you cannot disable
		// a subscription for `REQUIRED` notification type.
		CurrentState *string `json:"current_state,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Integrations Integrations to which notifications are to be sent.
		Integrations []GlobalObjectReference `json:"integrations"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NotificationsV1SubscriptionListDataKind `json:"kind,omitempty"`
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

		// NotificationType The type of notification to subscribe to.
		NotificationType GlobalObjectReference `json:"notification_type"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NotificationsV1SubscriptionListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NotificationsV1SubscriptionListApiVersion string
type NotificationsV1SubscriptionListDataApiVersion string
type NotificationsV1SubscriptionListDataKind string
type NotificationsV1SubscriptionListKind string
type NotificationsV1Summary struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1SummaryApiVersion `json:"api_version"`

	// Kind Kind defines the object this REST resource represents.
	Kind NotificationsV1SummaryKind `json:"kind"`

	// Severities Breakdown of unread notifications by severity level. One entry per
	// severity present in the user's unread set; severities with a zero
	// count may be omitted. New severity values may be added over time
	// without a breaking change to this schema.
	Severities []struct {
		// Count Number of unread notifications at this severity.
		Count int32 `json:"count"`

		// Severity The severity level this entry counts.
		Severity string `json:"severity"`
	} `json:"severities"`

	// UnreadCount Total number of unread notifications.
	UnreadCount int32 `json:"unread_count"`
}
type NotificationsV1SummaryApiVersion string
type NotificationsV1SummaryKind string
type NotificationsV1Target struct {
	union json.RawMessage
}
type NotificationsV1UpdateUserNotificationsReadRequest struct {
	// Read The target read state to apply to all notifications matching the
	// filter query parameters. `true` marks them as read; `false` marks
	// them as unread.
	Read bool `json:"read"`
}
type NotificationsV1UserEmailTarget struct {
	// Kind Email Integration type for User
	Kind NotificationsV1UserEmailTargetKind `json:"kind"`

	// User Reference to the user
	User GlobalObjectReference `json:"user"`
}
type NotificationsV1UserEmailTargetKind string
type NotificationsV1UserNotification struct {
	// Actions Ordered list of user-facing actions associated with this notification.
	// The first entry is the primary action (`role: PRIMARY`) and is always
	// present; subsequent entries are secondary. Cardinality is open-ended —
	// additional actions may be added over time without a breaking schema
	// change.
	Actions *[]NotificationsV1NotificationAction `json:"actions,omitempty"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *NotificationsV1UserNotificationApiVersion `json:"api_version,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Integrations The integrations this notification was delivered to. Each entry is a
	// point-in-time snapshot of the integration at delivery time, so values
	// remain accurate even if the underlying `Integration` is later
	// modified or deleted. Populated on single-resource reads
	// (`GET /user-notifications/{id}`); omitted from list responses.
	Integrations *[]NotificationsV1Integration `json:"integrations,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *NotificationsV1UserNotificationKind `json:"kind,omitempty"`
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

	// NotificationType The notification type that triggered this notification, embedded as
	// a point-in-time snapshot at delivery time so values remain accurate
	// even if the underlying `NotificationType` is later modified.
	NotificationType *NotificationsV1NotificationType `json:"notification_type,omitempty"`

	// Read Whether the notification has been read by the user.
	Read *bool `json:"read,omitempty"`

	// ReadAt The time the notification was marked as read, or `null` if it is unread.
	ReadAt *time.Time `json:"read_at,omitempty"`

	// ReceivedAt The time the underlying event was generated.
	ReceivedAt *time.Time `json:"received_at,omitempty"`

	// RecommendedActions Versioned payload describing the recommended actions a user can take
	// in response to this notification. The shape is stable per `version`
	// and consumers should branch on `version` when deserializing.
	// Populated on single-resource reads (`GET /user-notifications/{id}`);
	// omitted from list responses.
	RecommendedActions *NotificationsV1RecommendedActions `json:"recommended_actions,omitempty"`

	// Resource The Confluent Cloud resource this notification relates to, embedded
	// as a point-in-time snapshot at delivery time. Values remain accurate
	// even if the underlying resource is later renamed or deleted.
	Resource *NotificationsV1ResourceSnapshot `json:"resource,omitempty"`

	// Severity The severity level of the notification.
	// - CRITICAL: a high impact notification which needs immediate attention.
	// - WARN: a warning notification which can be addressed now or later.
	// - INFO: an informational notification.
	Severity *string `json:"severity,omitempty"`
}
type NotificationsV1UserNotificationApiVersion string
type NotificationsV1UserNotificationKind string
type NotificationsV1UserNotificationList struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion NotificationsV1UserNotificationListApiVersion `json:"api_version"`

	// Data A data property that contains an array of resource items. Each entry in the array is a separate resource.
	Data []struct {
		// Actions Ordered list of user-facing actions associated with this notification.
		// The first entry is the primary action (`role: PRIMARY`) and is always
		// present; subsequent entries are secondary. Cardinality is open-ended —
		// additional actions may be added over time without a breaking schema
		// change.
		Actions []NotificationsV1NotificationAction `json:"actions"`

		// ApiVersion APIVersion defines the schema version of this representation of a resource.
		ApiVersion *NotificationsV1UserNotificationListDataApiVersion `json:"api_version,omitempty"`

		// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
		Id string `json:"id"`

		// Integrations The integrations this notification was delivered to. Each entry is a
		// point-in-time snapshot of the integration at delivery time, so values
		// remain accurate even if the underlying `Integration` is later
		// modified or deleted. Populated on single-resource reads
		// (`GET /user-notifications/{id}`); omitted from list responses.
		Integrations *[]NotificationsV1Integration `json:"integrations,omitempty"`

		// Kind Kind defines the object this REST resource represents.
		Kind     *NotificationsV1UserNotificationListDataKind `json:"kind,omitempty"`
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

		// NotificationType The notification type that triggered this notification, embedded as
		// a point-in-time snapshot at delivery time so values remain accurate
		// even if the underlying `NotificationType` is later modified.
		NotificationType NotificationsV1NotificationType `json:"notification_type"`

		// Read Whether the notification has been read by the user.
		Read bool `json:"read"`

		// ReadAt The time the notification was marked as read, or `null` if it is unread.
		ReadAt *time.Time `json:"read_at,omitempty"`

		// ReceivedAt The time the underlying event was generated.
		ReceivedAt time.Time `json:"received_at"`

		// RecommendedActions Versioned payload describing the recommended actions a user can take
		// in response to this notification. The shape is stable per `version`
		// and consumers should branch on `version` when deserializing.
		// Populated on single-resource reads (`GET /user-notifications/{id}`);
		// omitted from list responses.
		RecommendedActions *NotificationsV1RecommendedActions `json:"recommended_actions,omitempty"`

		// Resource The Confluent Cloud resource this notification relates to, embedded
		// as a point-in-time snapshot at delivery time. Values remain accurate
		// even if the underlying resource is later renamed or deleted.
		Resource NotificationsV1ResourceSnapshot `json:"resource"`

		// Severity The severity level of the notification.
		// - CRITICAL: a high impact notification which needs immediate attention.
		// - WARN: a warning notification which can be addressed now or later.
		// - INFO: an informational notification.
		Severity *string `json:"severity,omitempty"`
	} `json:"data"`

	// Kind Kind defines the object this REST resource represents.
	Kind     NotificationsV1UserNotificationListKind `json:"kind"`
	Metadata struct {
		First interface{} `json:"first,omitempty"`
		Last  interface{} `json:"last,omitempty"`
		Next  interface{} `json:"next,omitempty"`
		Prev  interface{} `json:"prev,omitempty"`

		// TotalSize Number of records in the full result set. This response may be paginated and have a smaller number of records.
		TotalSize *int32 `json:"total_size,omitempty"`
	} `json:"metadata"`
}
type NotificationsV1UserNotificationListApiVersion string
type NotificationsV1UserNotificationListDataApiVersion string
type NotificationsV1UserNotificationListDataKind string
type NotificationsV1UserNotificationListKind string
type NotificationsV1WebhookTarget struct {
	// Kind Integration Type
	Kind NotificationsV1WebhookTargetKind `json:"kind"`

	// Url URL endpoint for the webhook
	Url string `json:"url"`
}
type NotificationsV1WebhookTargetKind string
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
func (t NotificationsV1Target) AsNotificationsV1SlackTarget() (NotificationsV1SlackTarget, error) {
	var body NotificationsV1SlackTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1SlackTarget(v NotificationsV1SlackTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Slack"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1SlackTarget(v NotificationsV1SlackTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Slack"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) AsNotificationsV1RoleEmailTarget() (NotificationsV1RoleEmailTarget, error) {
	var body NotificationsV1RoleEmailTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1RoleEmailTarget(v NotificationsV1RoleEmailTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"RoleEmail"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1RoleEmailTarget(v NotificationsV1RoleEmailTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"RoleEmail"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) AsNotificationsV1UserEmailTarget() (NotificationsV1UserEmailTarget, error) {
	var body NotificationsV1UserEmailTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1UserEmailTarget(v NotificationsV1UserEmailTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"UserEmail"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1UserEmailTarget(v NotificationsV1UserEmailTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"UserEmail"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) AsNotificationsV1WebhookTarget() (NotificationsV1WebhookTarget, error) {
	var body NotificationsV1WebhookTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1WebhookTarget(v NotificationsV1WebhookTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Webhook"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1WebhookTarget(v NotificationsV1WebhookTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"Webhook"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) AsNotificationsV1MsTeamsTarget() (NotificationsV1MsTeamsTarget, error) {
	var body NotificationsV1MsTeamsTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1MsTeamsTarget(v NotificationsV1MsTeamsTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"MsTeams"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1MsTeamsTarget(v NotificationsV1MsTeamsTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"MsTeams"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) AsNotificationsV1InAppTarget() (NotificationsV1InAppTarget, error) {
	var body NotificationsV1InAppTarget
	err := json.Unmarshal(t.union, &body)
	return body, err
}
func (t *NotificationsV1Target) FromNotificationsV1InAppTarget(v NotificationsV1InAppTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"InApp"}`))
	t.union = b
	return err
}
func (t *NotificationsV1Target) MergeNotificationsV1InAppTarget(v NotificationsV1InAppTarget) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err = runtime.JSONMerge(b, []byte(`{"kind":"InApp"}`))
	if err != nil {
		return err
	}

	merged, err := runtime.JSONMerge(t.union, b)
	t.union = merged
	return err
}
func (t NotificationsV1Target) Discriminator() (string, error) {
	var discriminator struct {
		Discriminator string `json:"kind"`
	}
	err := json.Unmarshal(t.union, &discriminator)
	return discriminator.Discriminator, err
}
func (t NotificationsV1Target) ValueByDiscriminator() (interface{}, error) {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "InApp":
		return t.AsNotificationsV1InAppTarget()
	case "MsTeams":
		return t.AsNotificationsV1MsTeamsTarget()
	case "RoleEmail":
		return t.AsNotificationsV1RoleEmailTarget()
	case "Slack":
		return t.AsNotificationsV1SlackTarget()
	case "UserEmail":
		return t.AsNotificationsV1UserEmailTarget()
	case "Webhook":
		return t.AsNotificationsV1WebhookTarget()
	default:
		return nil, errors.New("unknown discriminator value: " + discriminator)
	}
}
func (t NotificationsV1Target) MarshalJSON() ([]byte, error) {
	b, err := t.union.MarshalJSON()
	return b, err
}
func (t *NotificationsV1Target) UnmarshalJSON(b []byte) error {
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

	// ListNotificationsV1Integrations Retrieve a list of integrations. Optionally filter by resource and resource type.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_resource_type an integration.
	//
	// Corresponds with GET /notifications/v1/integrations (the `ListNotificationsV1Integrations` operationId).
	ListNotificationsV1Integrations(ctx context.Context, params *ListNotificationsV1IntegrationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1IntegrationWithBody Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /notifications/v1/integrations (the `CreateNotificationsV1Integration` operationId).
	CreateNotificationsV1IntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1Integration Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /notifications/v1/integrations (the `CreateNotificationsV1Integration` operationId).
	CreateNotificationsV1Integration(ctx context.Context, body CreateNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNotificationsV1Integration Delete an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an integration.
	//
	// Corresponds with DELETE /notifications/v1/integrations/{id} (the `DeleteNotificationsV1Integration` operationId).
	DeleteNotificationsV1Integration(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1Integration Read an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an integration.
	//
	// Corresponds with GET /notifications/v1/integrations/{id} (the `GetNotificationsV1Integration` operationId).
	GetNotificationsV1Integration(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1IntegrationWithBody Update an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an integration.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/integrations/{id} (the `UpdateNotificationsV1Integration` operationId).
	UpdateNotificationsV1IntegrationWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1Integration Update an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an integration.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/integrations/{id} (the `UpdateNotificationsV1Integration` operationId).
	UpdateNotificationsV1Integration(ctx context.Context, id string, body UpdateNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// TestNotificationsV1IntegrationWithBody Test a Webhook, Slack or Microsoft Teams integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Sends a test notification to validate the integration. This is supported only for Webhook, Slack
	// and MsTeams targets
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /notifications/v1/integrations:test (the `TestNotificationsV1Integration` operationId).
	TestNotificationsV1IntegrationWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// TestNotificationsV1Integration Test a Webhook, Slack or Microsoft Teams integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Sends a test notification to validate the integration. This is supported only for Webhook, Slack
	// and MsTeams targets
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /notifications/v1/integrations:test (the `TestNotificationsV1Integration` operationId).
	TestNotificationsV1Integration(ctx context.Context, body TestNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNotificationsV1NotificationTypes Retrieve a list of all notification types for the resource type.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_resource_type a notification type.
	//
	// Corresponds with GET /notifications/v1/notification-types (the `ListNotificationsV1NotificationTypes` operationId).
	ListNotificationsV1NotificationTypes(ctx context.Context, params *ListNotificationsV1NotificationTypesParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1NotificationType Read a Notification Type
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a notification type.
	//
	// Corresponds with GET /notifications/v1/notification-types/{id} (the `GetNotificationsV1NotificationType` operationId).
	GetNotificationsV1NotificationType(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1ResourcePreferenceWithBody Create a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource preference.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /notifications/v1/resource-preferences (the `CreateNotificationsV1ResourcePreference` operationId).
	CreateNotificationsV1ResourcePreferenceWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1ResourcePreference Create a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource preference.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /notifications/v1/resource-preferences (the `CreateNotificationsV1ResourcePreference` operationId).
	CreateNotificationsV1ResourcePreference(ctx context.Context, body CreateNotificationsV1ResourcePreferenceJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNotificationsV1ResourcePreference Delete a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a resource preference.
	//
	// Corresponds with DELETE /notifications/v1/resource-preferences/{id} (the `DeleteNotificationsV1ResourcePreference` operationId).
	DeleteNotificationsV1ResourcePreference(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1ResourcePreference Read a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a resource preference.
	//
	// Corresponds with GET /notifications/v1/resource-preferences/{id} (the `GetNotificationsV1ResourcePreference` operationId).
	GetNotificationsV1ResourcePreference(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1ResourcePreferenceWithBody Update a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource preference.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/resource-preferences/{id} (the `UpdateNotificationsV1ResourcePreference` operationId).
	UpdateNotificationsV1ResourcePreferenceWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1ResourcePreference Update a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource preference.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/resource-preferences/{id} (the `UpdateNotificationsV1ResourcePreference` operationId).
	UpdateNotificationsV1ResourcePreference(ctx context.Context, id string, body UpdateNotificationsV1ResourcePreferenceJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1ResourcePreferenceByFilter Lookup a resource preference by filter (returns one)
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read_by_filter a resource preference.
	//
	// Corresponds with GET /notifications/v1/resource-preferences:lookup (the `GetNotificationsV1ResourcePreferenceByFilter` operationId).
	GetNotificationsV1ResourcePreferenceByFilter(ctx context.Context, params *GetNotificationsV1ResourcePreferenceByFilterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1ResourceSubscriptionWithBody Create a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource subscription.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /notifications/v1/resource-subscriptions (the `CreateNotificationsV1ResourceSubscription` operationId).
	CreateNotificationsV1ResourceSubscriptionWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1ResourceSubscription Create a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource subscription.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /notifications/v1/resource-subscriptions (the `CreateNotificationsV1ResourceSubscription` operationId).
	CreateNotificationsV1ResourceSubscription(ctx context.Context, body CreateNotificationsV1ResourceSubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNotificationsV1ResourceSubscription Delete a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a resource subscription.
	//
	// Corresponds with DELETE /notifications/v1/resource-subscriptions/{id} (the `DeleteNotificationsV1ResourceSubscription` operationId).
	DeleteNotificationsV1ResourceSubscription(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1ResourceSubscription Read a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a resource subscription.
	//
	// Corresponds with GET /notifications/v1/resource-subscriptions/{id} (the `GetNotificationsV1ResourceSubscription` operationId).
	GetNotificationsV1ResourceSubscription(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1ResourceSubscriptionWithBody Update a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource subscription.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/resource-subscriptions/{id} (the `UpdateNotificationsV1ResourceSubscription` operationId).
	UpdateNotificationsV1ResourceSubscriptionWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1ResourceSubscription Update a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource subscription.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/resource-subscriptions/{id} (the `UpdateNotificationsV1ResourceSubscription` operationId).
	UpdateNotificationsV1ResourceSubscription(ctx context.Context, id string, body UpdateNotificationsV1ResourceSubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNotificationsV1ResourceSubscriptionsByFilter Lookup a list of resource subscription by filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_filter a resource subscription.
	//
	// Corresponds with GET /notifications/v1/resource-subscriptions:lookup (the `ListNotificationsV1ResourceSubscriptionsByFilter` operationId).
	ListNotificationsV1ResourceSubscriptionsByFilter(ctx context.Context, params *ListNotificationsV1ResourceSubscriptionsByFilterParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNotificationsV1Subscriptions List of Subscriptions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all subscriptions.
	//
	// Corresponds with GET /notifications/v1/subscriptions (the `ListNotificationsV1Subscriptions` operationId).
	ListNotificationsV1Subscriptions(ctx context.Context, params *ListNotificationsV1SubscriptionsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1SubscriptionWithBody Create a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a subscription.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with POST /notifications/v1/subscriptions (the `CreateNotificationsV1Subscription` operationId).
	CreateNotificationsV1SubscriptionWithBody(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// CreateNotificationsV1Subscription Create a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a subscription.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with POST /notifications/v1/subscriptions (the `CreateNotificationsV1Subscription` operationId).
	CreateNotificationsV1Subscription(ctx context.Context, body CreateNotificationsV1SubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// DeleteNotificationsV1Subscription Delete a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a subscription.
	//
	// Corresponds with DELETE /notifications/v1/subscriptions/{id} (the `DeleteNotificationsV1Subscription` operationId).
	DeleteNotificationsV1Subscription(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1Subscription Read a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a subscription.
	//
	// Corresponds with GET /notifications/v1/subscriptions/{id} (the `GetNotificationsV1Subscription` operationId).
	GetNotificationsV1Subscription(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1SubscriptionWithBody Update a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a subscription.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/subscriptions/{id} (the `UpdateNotificationsV1Subscription` operationId).
	UpdateNotificationsV1SubscriptionWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1Subscription Update a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a subscription.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/subscriptions/{id} (the `UpdateNotificationsV1Subscription` operationId).
	UpdateNotificationsV1Subscription(ctx context.Context, id string, body UpdateNotificationsV1SubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// ListNotificationsV1UserNotifications List of User Notifications
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all user notifications.
	//
	// Corresponds with GET /notifications/v1/user-notifications (the `ListNotificationsV1UserNotifications` operationId).
	ListNotificationsV1UserNotifications(ctx context.Context, params *ListNotificationsV1UserNotificationsParams, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1UserNotification Read a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read a user notification.
	//
	// Corresponds with GET /notifications/v1/user-notifications/{id} (the `GetNotificationsV1UserNotification` operationId).
	GetNotificationsV1UserNotification(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1UserNotificationWithBody Update a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a user notification.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/user-notifications/{id} (the `UpdateNotificationsV1UserNotification` operationId).
	UpdateNotificationsV1UserNotificationWithBody(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// UpdateNotificationsV1UserNotification Update a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a user notification.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/user-notifications/{id} (the `UpdateNotificationsV1UserNotification` operationId).
	UpdateNotificationsV1UserNotification(ctx context.Context, id string, body UpdateNotificationsV1UserNotificationJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// MarkAllNotificationsV1UserNotificationsWithBody Mark multiple notifications read or unread
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Sets the read state on every notification matching the supplied filter
	// query parameters. Accepts the same filter parameters as the list
	// endpoint (except `include`, which is a list-only partial-response
	// selector). The request body sets the target read state to apply.
	//
	// Takes any type of body and a specified content type.
	//
	// Corresponds with PATCH /notifications/v1/user-notifications:mark-all (the `MarkAllNotificationsV1UserNotifications` operationId).
	MarkAllNotificationsV1UserNotificationsWithBody(ctx context.Context, params *MarkAllNotificationsV1UserNotificationsParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error)

	// MarkAllNotificationsV1UserNotifications Mark multiple notifications read or unread
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Sets the read state on every notification matching the supplied filter
	// query parameters. Accepts the same filter parameters as the list
	// endpoint (except `include`, which is a list-only partial-response
	// selector). The request body sets the target read state to apply.
	//
	// Takes a body of the `application/json` content type.
	//
	// Corresponds with PATCH /notifications/v1/user-notifications:mark-all (the `MarkAllNotificationsV1UserNotifications` operationId).
	MarkAllNotificationsV1UserNotifications(ctx context.Context, params *MarkAllNotificationsV1UserNotificationsParams, body MarkAllNotificationsV1UserNotificationsJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error)

	// GetNotificationsV1UserNotificationsSummary Get notification summary
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Returns the authenticated user's total unread notification count along with
	// a breakdown by severity.
	//
	// Corresponds with GET /notifications/v1/user-notifications:summary (the `GetNotificationsV1UserNotificationsSummary` operationId).
	GetNotificationsV1UserNotificationsSummary(ctx context.Context, reqEditors ...RequestEditorFn) (*http.Response, error)
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

	// ListNotificationsV1IntegrationsWithResponse Retrieve a list of integrations. Optionally filter by resource and resource type.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_resource_type an integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/integrations (the `ListNotificationsV1Integrations` operationId).
	ListNotificationsV1IntegrationsWithResponse(ctx context.Context, params *ListNotificationsV1IntegrationsParams, reqEditors ...RequestEditorFn) (*ListNotificationsV1IntegrationsResponse, error)

	// CreateNotificationsV1IntegrationWithBodyWithResponse Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/integrations (the `CreateNotificationsV1Integration` operationId).
	CreateNotificationsV1IntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNotificationsV1IntegrationResponse, error)

	// CreateNotificationsV1IntegrationWithResponse Create an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create an integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/integrations (the `CreateNotificationsV1Integration` operationId).
	CreateNotificationsV1IntegrationWithResponse(ctx context.Context, body CreateNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNotificationsV1IntegrationResponse, error)

	// DeleteNotificationsV1IntegrationWithResponse Delete an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete an integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /notifications/v1/integrations/{id} (the `DeleteNotificationsV1Integration` operationId).
	DeleteNotificationsV1IntegrationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteNotificationsV1IntegrationResponse, error)

	// GetNotificationsV1IntegrationWithResponse Read an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read an integration.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/integrations/{id} (the `GetNotificationsV1Integration` operationId).
	GetNotificationsV1IntegrationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1IntegrationResponse, error)

	// UpdateNotificationsV1IntegrationWithBodyWithResponse Update an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an integration.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/integrations/{id} (the `UpdateNotificationsV1Integration` operationId).
	UpdateNotificationsV1IntegrationWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1IntegrationResponse, error)

	// UpdateNotificationsV1IntegrationWithResponse Update an Integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update an integration.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/integrations/{id} (the `UpdateNotificationsV1Integration` operationId).
	UpdateNotificationsV1IntegrationWithResponse(ctx context.Context, id string, body UpdateNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1IntegrationResponse, error)

	// TestNotificationsV1IntegrationWithBodyWithResponse Test a Webhook, Slack or Microsoft Teams integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Sends a test notification to validate the integration. This is supported only for Webhook, Slack
	// and MsTeams targets
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/integrations:test (the `TestNotificationsV1Integration` operationId).
	TestNotificationsV1IntegrationWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*TestNotificationsV1IntegrationResponse, error)

	// TestNotificationsV1IntegrationWithResponse Test a Webhook, Slack or Microsoft Teams integration
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Sends a test notification to validate the integration. This is supported only for Webhook, Slack
	// and MsTeams targets
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/integrations:test (the `TestNotificationsV1Integration` operationId).
	TestNotificationsV1IntegrationWithResponse(ctx context.Context, body TestNotificationsV1IntegrationJSONRequestBody, reqEditors ...RequestEditorFn) (*TestNotificationsV1IntegrationResponse, error)

	// ListNotificationsV1NotificationTypesWithResponse Retrieve a list of all notification types for the resource type.
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_resource_type a notification type.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/notification-types (the `ListNotificationsV1NotificationTypes` operationId).
	ListNotificationsV1NotificationTypesWithResponse(ctx context.Context, params *ListNotificationsV1NotificationTypesParams, reqEditors ...RequestEditorFn) (*ListNotificationsV1NotificationTypesResponse, error)

	// GetNotificationsV1NotificationTypeWithResponse Read a Notification Type
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a notification type.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/notification-types/{id} (the `GetNotificationsV1NotificationType` operationId).
	GetNotificationsV1NotificationTypeWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1NotificationTypeResponse, error)

	// CreateNotificationsV1ResourcePreferenceWithBodyWithResponse Create a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource preference.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/resource-preferences (the `CreateNotificationsV1ResourcePreference` operationId).
	CreateNotificationsV1ResourcePreferenceWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNotificationsV1ResourcePreferenceResponse, error)

	// CreateNotificationsV1ResourcePreferenceWithResponse Create a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource preference.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/resource-preferences (the `CreateNotificationsV1ResourcePreference` operationId).
	CreateNotificationsV1ResourcePreferenceWithResponse(ctx context.Context, body CreateNotificationsV1ResourcePreferenceJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNotificationsV1ResourcePreferenceResponse, error)

	// DeleteNotificationsV1ResourcePreferenceWithResponse Delete a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a resource preference.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /notifications/v1/resource-preferences/{id} (the `DeleteNotificationsV1ResourcePreference` operationId).
	DeleteNotificationsV1ResourcePreferenceWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteNotificationsV1ResourcePreferenceResponse, error)

	// GetNotificationsV1ResourcePreferenceWithResponse Read a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a resource preference.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/resource-preferences/{id} (the `GetNotificationsV1ResourcePreference` operationId).
	GetNotificationsV1ResourcePreferenceWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1ResourcePreferenceResponse, error)

	// UpdateNotificationsV1ResourcePreferenceWithBodyWithResponse Update a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource preference.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/resource-preferences/{id} (the `UpdateNotificationsV1ResourcePreference` operationId).
	UpdateNotificationsV1ResourcePreferenceWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1ResourcePreferenceResponse, error)

	// UpdateNotificationsV1ResourcePreferenceWithResponse Update a Resource Preference
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource preference.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/resource-preferences/{id} (the `UpdateNotificationsV1ResourcePreference` operationId).
	UpdateNotificationsV1ResourcePreferenceWithResponse(ctx context.Context, id string, body UpdateNotificationsV1ResourcePreferenceJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1ResourcePreferenceResponse, error)

	// GetNotificationsV1ResourcePreferenceByFilterWithResponse Lookup a resource preference by filter (returns one)
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read_by_filter a resource preference.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/resource-preferences:lookup (the `GetNotificationsV1ResourcePreferenceByFilter` operationId).
	GetNotificationsV1ResourcePreferenceByFilterWithResponse(ctx context.Context, params *GetNotificationsV1ResourcePreferenceByFilterParams, reqEditors ...RequestEditorFn) (*GetNotificationsV1ResourcePreferenceByFilterResponse, error)

	// CreateNotificationsV1ResourceSubscriptionWithBodyWithResponse Create a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource subscription.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/resource-subscriptions (the `CreateNotificationsV1ResourceSubscription` operationId).
	CreateNotificationsV1ResourceSubscriptionWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNotificationsV1ResourceSubscriptionResponse, error)

	// CreateNotificationsV1ResourceSubscriptionWithResponse Create a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a resource subscription.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/resource-subscriptions (the `CreateNotificationsV1ResourceSubscription` operationId).
	CreateNotificationsV1ResourceSubscriptionWithResponse(ctx context.Context, body CreateNotificationsV1ResourceSubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNotificationsV1ResourceSubscriptionResponse, error)

	// DeleteNotificationsV1ResourceSubscriptionWithResponse Delete a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a resource subscription.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /notifications/v1/resource-subscriptions/{id} (the `DeleteNotificationsV1ResourceSubscription` operationId).
	DeleteNotificationsV1ResourceSubscriptionWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteNotificationsV1ResourceSubscriptionResponse, error)

	// GetNotificationsV1ResourceSubscriptionWithResponse Read a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a resource subscription.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/resource-subscriptions/{id} (the `GetNotificationsV1ResourceSubscription` operationId).
	GetNotificationsV1ResourceSubscriptionWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1ResourceSubscriptionResponse, error)

	// UpdateNotificationsV1ResourceSubscriptionWithBodyWithResponse Update a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource subscription.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/resource-subscriptions/{id} (the `UpdateNotificationsV1ResourceSubscription` operationId).
	UpdateNotificationsV1ResourceSubscriptionWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1ResourceSubscriptionResponse, error)

	// UpdateNotificationsV1ResourceSubscriptionWithResponse Update a Resource Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a resource subscription.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/resource-subscriptions/{id} (the `UpdateNotificationsV1ResourceSubscription` operationId).
	UpdateNotificationsV1ResourceSubscriptionWithResponse(ctx context.Context, id string, body UpdateNotificationsV1ResourceSubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1ResourceSubscriptionResponse, error)

	// ListNotificationsV1ResourceSubscriptionsByFilterWithResponse Lookup a list of resource subscription by filter
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to list_by_filter a resource subscription.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/resource-subscriptions:lookup (the `ListNotificationsV1ResourceSubscriptionsByFilter` operationId).
	ListNotificationsV1ResourceSubscriptionsByFilterWithResponse(ctx context.Context, params *ListNotificationsV1ResourceSubscriptionsByFilterParams, reqEditors ...RequestEditorFn) (*ListNotificationsV1ResourceSubscriptionsByFilterResponse, error)

	// ListNotificationsV1SubscriptionsWithResponse List of Subscriptions
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Retrieve a sorted, filtered, paginated list of all subscriptions.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/subscriptions (the `ListNotificationsV1Subscriptions` operationId).
	ListNotificationsV1SubscriptionsWithResponse(ctx context.Context, params *ListNotificationsV1SubscriptionsParams, reqEditors ...RequestEditorFn) (*ListNotificationsV1SubscriptionsResponse, error)

	// CreateNotificationsV1SubscriptionWithBodyWithResponse Create a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a subscription.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/subscriptions (the `CreateNotificationsV1Subscription` operationId).
	CreateNotificationsV1SubscriptionWithBodyWithResponse(ctx context.Context, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreateNotificationsV1SubscriptionResponse, error)

	// CreateNotificationsV1SubscriptionWithResponse Create a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to create a subscription.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with POST /notifications/v1/subscriptions (the `CreateNotificationsV1Subscription` operationId).
	CreateNotificationsV1SubscriptionWithResponse(ctx context.Context, body CreateNotificationsV1SubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*CreateNotificationsV1SubscriptionResponse, error)

	// DeleteNotificationsV1SubscriptionWithResponse Delete a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to delete a subscription.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with DELETE /notifications/v1/subscriptions/{id} (the `DeleteNotificationsV1Subscription` operationId).
	DeleteNotificationsV1SubscriptionWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*DeleteNotificationsV1SubscriptionResponse, error)

	// GetNotificationsV1SubscriptionWithResponse Read a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to read a subscription.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/subscriptions/{id} (the `GetNotificationsV1Subscription` operationId).
	GetNotificationsV1SubscriptionWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1SubscriptionResponse, error)

	// UpdateNotificationsV1SubscriptionWithBodyWithResponse Update a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a subscription.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/subscriptions/{id} (the `UpdateNotificationsV1Subscription` operationId).
	UpdateNotificationsV1SubscriptionWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1SubscriptionResponse, error)

	// UpdateNotificationsV1SubscriptionWithResponse Update a Subscription
	//
	// [![General Availability](https://img.shields.io/badge/Lifecycle%20Stage-General%20Availability-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy)
	//
	// Make a request to update a subscription.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/subscriptions/{id} (the `UpdateNotificationsV1Subscription` operationId).
	UpdateNotificationsV1SubscriptionWithResponse(ctx context.Context, id string, body UpdateNotificationsV1SubscriptionJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1SubscriptionResponse, error)

	// ListNotificationsV1UserNotificationsWithResponse List of User Notifications
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Retrieve a sorted, filtered, paginated list of all user notifications.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/user-notifications (the `ListNotificationsV1UserNotifications` operationId).
	ListNotificationsV1UserNotificationsWithResponse(ctx context.Context, params *ListNotificationsV1UserNotificationsParams, reqEditors ...RequestEditorFn) (*ListNotificationsV1UserNotificationsResponse, error)

	// GetNotificationsV1UserNotificationWithResponse Read a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to read a user notification.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/user-notifications/{id} (the `GetNotificationsV1UserNotification` operationId).
	GetNotificationsV1UserNotificationWithResponse(ctx context.Context, id string, reqEditors ...RequestEditorFn) (*GetNotificationsV1UserNotificationResponse, error)

	// UpdateNotificationsV1UserNotificationWithBodyWithResponse Update a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a user notification.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/user-notifications/{id} (the `UpdateNotificationsV1UserNotification` operationId).
	UpdateNotificationsV1UserNotificationWithBodyWithResponse(ctx context.Context, id string, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1UserNotificationResponse, error)

	// UpdateNotificationsV1UserNotificationWithResponse Update a User Notification
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Make a request to update a user notification.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/user-notifications/{id} (the `UpdateNotificationsV1UserNotification` operationId).
	UpdateNotificationsV1UserNotificationWithResponse(ctx context.Context, id string, body UpdateNotificationsV1UserNotificationJSONRequestBody, reqEditors ...RequestEditorFn) (*UpdateNotificationsV1UserNotificationResponse, error)

	// MarkAllNotificationsV1UserNotificationsWithBodyWithResponse Mark multiple notifications read or unread
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Sets the read state on every notification matching the supplied filter
	// query parameters. Accepts the same filter parameters as the list
	// endpoint (except `include`, which is a list-only partial-response
	// selector). The request body sets the target read state to apply.
	//
	// Takes any type of body and a specified content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/user-notifications:mark-all (the `MarkAllNotificationsV1UserNotifications` operationId).
	MarkAllNotificationsV1UserNotificationsWithBodyWithResponse(ctx context.Context, params *MarkAllNotificationsV1UserNotificationsParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*MarkAllNotificationsV1UserNotificationsResponse, error)

	// MarkAllNotificationsV1UserNotificationsWithResponse Mark multiple notifications read or unread
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Sets the read state on every notification matching the supplied filter
	// query parameters. Accepts the same filter parameters as the list
	// endpoint (except `include`, which is a list-only partial-response
	// selector). The request body sets the target read state to apply.
	//
	// Takes a body of the `application/json` content type, and returns a wrapper object for the known response body format(s).
	//
	// Corresponds with PATCH /notifications/v1/user-notifications:mark-all (the `MarkAllNotificationsV1UserNotifications` operationId).
	MarkAllNotificationsV1UserNotificationsWithResponse(ctx context.Context, params *MarkAllNotificationsV1UserNotificationsParams, body MarkAllNotificationsV1UserNotificationsJSONRequestBody, reqEditors ...RequestEditorFn) (*MarkAllNotificationsV1UserNotificationsResponse, error)

	// GetNotificationsV1UserNotificationsSummaryWithResponse Get notification summary
	//
	// [![Early Access](https://img.shields.io/badge/Lifecycle%20Stage-Early%20Access-%2345c6e8)](#section/Versioning/API-Lifecycle-Policy) [![Request Access To User Notifications API v1](https://img.shields.io/badge/-Request%20Access%20To%20User%20Notifications%20API%20v1-%23bc8540)](mailto:ccloud-api-access+notifications-v1-early-access@confluent.io?subject=Request%20to%20join%20notifications/v1%20API%20Early%20Access&body=I%E2%80%99d%20like%20to%20join%20the%20Confluent%20Cloud%20API%20Early%20Access%20for%20notifications/v1%20to%20provide%20early%20feedback%21%20My%20Cloud%20Organization%20ID%20is%20%3Cretrieve%20from%20https%3A//confluent.cloud/settings/billing/payment%3E.)
	//
	// Returns the authenticated user's total unread notification count along with
	// a breakdown by severity.
	//
	// Returns a wrapper object for the known response body format(s).
	//
	// Corresponds with GET /notifications/v1/user-notifications:summary (the `GetNotificationsV1UserNotificationsSummary` operationId).
	GetNotificationsV1UserNotificationsSummaryWithResponse(ctx context.Context, reqEditors ...RequestEditorFn) (*GetNotificationsV1UserNotificationsSummaryResponse, error)
}

func (r ListNotificationsV1IntegrationsResponse) GetJSON200() *NotificationsV1IntegrationList {
	return r.JSON200
}
func (r ListNotificationsV1IntegrationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNotificationsV1IntegrationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNotificationsV1IntegrationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNotificationsV1IntegrationsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListNotificationsV1IntegrationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNotificationsV1IntegrationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNotificationsV1IntegrationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNotificationsV1IntegrationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNotificationsV1IntegrationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNotificationsV1Integration201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// Description A human readable description for the particular integration
	Description *string `json:"description,omitempty"`

	// DisplayName A human readable name for the particular integration
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNotificationsV1Integration201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// Target Integration-specific details (integration targets)
	Target NotificationsV1Target `json:"target"`
} {
	return r.JSON201
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNotificationsV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNotificationsV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNotificationsV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNotificationsV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNotificationsV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNotificationsV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNotificationsV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNotificationsV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNotificationsV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNotificationsV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNotificationsV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNotificationsV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNotificationsV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNotificationsV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1IntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1Integration200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A human readable description for the particular integration
	Description *string `json:"description,omitempty"`

	// DisplayName A human readable name for the particular integration
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1Integration200JSONResponseBodyKind `json:"kind"`
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

	// Target Integration-specific details (integration targets)
	Target NotificationsV1Target `json:"target"`
} {
	return r.JSON200
}
func (r GetNotificationsV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNotificationsV1Integration200JSONResponseBodyApiVersion `json:"api_version"`

	// Description A human readable description for the particular integration
	Description *string `json:"description,omitempty"`

	// DisplayName A human readable name for the particular integration
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNotificationsV1Integration200JSONResponseBodyKind `json:"kind"`
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

	// Target Integration-specific details (integration targets)
	Target NotificationsV1Target `json:"target"`
} {
	return r.JSON200
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON402() *OverQuotaError {
	return r.JSON402
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNotificationsV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNotificationsV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNotificationsV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNotificationsV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNotificationsV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r TestNotificationsV1IntegrationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r TestNotificationsV1IntegrationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r TestNotificationsV1IntegrationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r TestNotificationsV1IntegrationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r TestNotificationsV1IntegrationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r TestNotificationsV1IntegrationResponse) GetBody() []byte {
	return r.Body
}
func (r TestNotificationsV1IntegrationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r TestNotificationsV1IntegrationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r TestNotificationsV1IntegrationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON200() *NotificationsV1NotificationTypeList {
	return r.JSON200
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListNotificationsV1NotificationTypesResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNotificationsV1NotificationTypesResponse) GetBody() []byte {
	return r.Body
}
func (r ListNotificationsV1NotificationTypesResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNotificationsV1NotificationTypesResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNotificationsV1NotificationTypesResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1NotificationType200JSONResponseBodyApiVersion `json:"api_version"`

	// Category Represents the group with which the notification is associated.
	// Notifications are grouped under certain categories for better organization.
	// - BILLING_LICENSING: All billing, payments or licensing related notifications are grouped here.
	// - SECURITY: All Confluent Cloud and Platform security related notifications are grouped here.
	// - SERVICE: All Confluent services (eg. Kafka, Schema Registry, Connect etc.) related notifications are
	//   grouped here.
	// - ACCOUNT: All Confluent account related notifications are grouped here.
	// For example: Billing, payment or license related notifications are grouped in BILLING_LICENSING category.
	Category string `json:"category"`

	// Description Human readable description of the notification type
	Description string `json:"description"`

	// DisplayName Human readable display name of the notification type
	DisplayName string `json:"display_name"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// IsIncludedInPlan Whether this notification is available to subscribe or not
	// as per the user's current billing plan.
	IsIncludedInPlan bool `json:"is_included_in_plan"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1NotificationType200JSONResponseBodyKind `json:"kind"`
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

	// ResourceType The type of resource this notification is associated with. Optional field.
	ResourceType *string `json:"resource_type,omitempty"`

	// Severity Severity indicates the impact of this notification.
	// - CRITICAL: a high impact notification which needs immediate attention.
	// - WARN: a warning notification which can be addressed now or later.
	// - INFO: an informational notification.
	Severity string `json:"severity"`

	// SubscriptionPriority Indicates whether the notification is auto-subscribed and if the user can opt-out.
	// - REQUIRED: the user is auto-subscribed to this notification and can't opt-out.
	// - RECOMMENDED: the user is auto-subscribed to this notification and can opt-out.
	// - OPTIONAL: the user is not auto-subscribed to this notification but can explicitly subscribe to it.
	SubscriptionPriority string `json:"subscription_priority"`
} {
	return r.JSON200
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1NotificationTypeResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1NotificationTypeResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1NotificationTypeResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1NotificationTypeResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1NotificationTypeResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNotificationsV1ResourcePreference201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNotificationsV1ResourcePreference201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON201
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNotificationsV1ResourcePreferenceResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNotificationsV1ResourcePreferenceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNotificationsV1ResourcePreferenceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNotificationsV1ResourcePreferenceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNotificationsV1ResourcePreferenceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1ResourcePreference200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1ResourcePreference200JSONResponseBodyKind `json:"kind"`
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

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON200
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1ResourcePreferenceResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1ResourcePreferenceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1ResourcePreferenceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1ResourcePreferenceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNotificationsV1ResourcePreference200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNotificationsV1ResourcePreference200JSONResponseBodyKind `json:"kind"`
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

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON200
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNotificationsV1ResourcePreferenceResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1ResourcePreferenceByFilter200JSONResponseBodyKind `json:"kind"`
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

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON200
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1ResourcePreferenceByFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNotificationsV1ResourceSubscription201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNotificationsV1ResourceSubscription201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON201
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNotificationsV1ResourceSubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNotificationsV1ResourceSubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1ResourceSubscription200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON200
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1ResourceSubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1ResourceSubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1ResourceSubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1ResourceSubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNotificationsV1ResourceSubscription200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the resource preference. When the resource preference is ENABLED, the user will receive
	// notifications for the Confluent Cloud resource. If the resource preference is DISABLED, the user will not
	// receive any notification for the resource.
	// Note that, you will still receive notifications for `REQUIRED` notification type even when it is DISABLED.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNotificationsV1ResourceSubscription200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`

	// Resource Denotes the Confluent Cloud resource definition.
	Resource string `json:"resource"`

	// ResourceType Denotes the Confluent Cloud resource type.
	ResourceType string `json:"resource_type"`
} {
	return r.JSON200
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNotificationsV1ResourceSubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON200() *NotificationsV1ResourceSubscriptionList {
	return r.JSON200
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) GetBody() []byte {
	return r.Body
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNotificationsV1ResourceSubscriptionsByFilterResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNotificationsV1SubscriptionsResponse) GetJSON200() *NotificationsV1SubscriptionList {
	return r.JSON200
}
func (r ListNotificationsV1SubscriptionsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNotificationsV1SubscriptionsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNotificationsV1SubscriptionsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNotificationsV1SubscriptionsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNotificationsV1SubscriptionsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNotificationsV1SubscriptionsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNotificationsV1SubscriptionsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNotificationsV1SubscriptionsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON201() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion *CreateNotificationsV1Subscription201JSONResponseBodyApiVersion `json:"api_version,omitempty"`

	// CurrentState Denotes the state of the subscription. When the subscription is ENABLED, the user will receive
	// notification on the configured Integrations. If the subscription is DISABLED, the user will not
	// recieve any notification for the configured notification type. Note that, you cannot disable
	// a subscription for `REQUIRED` notification type.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id *string `json:"id,omitempty"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     *CreateNotificationsV1Subscription201JSONResponseBodyKind `json:"kind,omitempty"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`
} {
	return r.JSON201
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r CreateNotificationsV1SubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r CreateNotificationsV1SubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r CreateNotificationsV1SubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r CreateNotificationsV1SubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r CreateNotificationsV1SubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r DeleteNotificationsV1SubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r DeleteNotificationsV1SubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r DeleteNotificationsV1SubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r DeleteNotificationsV1SubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r DeleteNotificationsV1SubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r DeleteNotificationsV1SubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r DeleteNotificationsV1SubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r DeleteNotificationsV1SubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r DeleteNotificationsV1SubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1Subscription200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the subscription. When the subscription is ENABLED, the user will receive
	// notification on the configured Integrations. If the subscription is DISABLED, the user will not
	// recieve any notification for the configured notification type. Note that, you cannot disable
	// a subscription for `REQUIRED` notification type.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1Subscription200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`
} {
	return r.JSON200
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1SubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1SubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1SubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1SubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1SubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON200() *struct {
	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNotificationsV1Subscription200JSONResponseBodyApiVersion `json:"api_version"`

	// CurrentState Denotes the state of the subscription. When the subscription is ENABLED, the user will receive
	// notification on the configured Integrations. If the subscription is DISABLED, the user will not
	// recieve any notification for the configured notification type. Note that, you cannot disable
	// a subscription for `REQUIRED` notification type.
	CurrentState *string `json:"current_state,omitempty"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations Integrations to which notifications are to be sent.
	Integrations []GlobalObjectReference `json:"integrations"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNotificationsV1Subscription200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The type of notification to subscribe to.
	NotificationType GlobalObjectReference `json:"notification_type"`
} {
	return r.JSON200
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNotificationsV1SubscriptionResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNotificationsV1SubscriptionResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNotificationsV1SubscriptionResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNotificationsV1SubscriptionResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNotificationsV1SubscriptionResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r ListNotificationsV1UserNotificationsResponse) GetJSON200() *NotificationsV1UserNotificationList {
	return r.JSON200
}
func (r ListNotificationsV1UserNotificationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r ListNotificationsV1UserNotificationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r ListNotificationsV1UserNotificationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r ListNotificationsV1UserNotificationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r ListNotificationsV1UserNotificationsResponse) GetBody() []byte {
	return r.Body
}
func (r ListNotificationsV1UserNotificationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r ListNotificationsV1UserNotificationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r ListNotificationsV1UserNotificationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON200() *struct {
	// Actions Ordered list of user-facing actions associated with this notification.
	// The first entry is the primary action (`role: PRIMARY`) and is always
	// present; subsequent entries are secondary. Cardinality is open-ended —
	// additional actions may be added over time without a breaking schema
	// change.
	Actions []NotificationsV1NotificationAction `json:"actions"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion GetNotificationsV1UserNotification200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations The integrations this notification was delivered to. Each entry is a
	// point-in-time snapshot of the integration at delivery time, so values
	// remain accurate even if the underlying `Integration` is later
	// modified or deleted. Populated on single-resource reads
	// (`GET /user-notifications/{id}`); omitted from list responses.
	Integrations *[]NotificationsV1Integration `json:"integrations,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     GetNotificationsV1UserNotification200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The notification type that triggered this notification, embedded as
	// a point-in-time snapshot at delivery time so values remain accurate
	// even if the underlying `NotificationType` is later modified.
	NotificationType NotificationsV1NotificationType `json:"notification_type"`

	// Read Whether the notification has been read by the user.
	Read bool `json:"read"`

	// ReadAt The time the notification was marked as read, or `null` if it is unread.
	ReadAt *time.Time `json:"read_at,omitempty"`

	// ReceivedAt The time the underlying event was generated.
	ReceivedAt time.Time `json:"received_at"`

	// RecommendedActions Versioned payload describing the recommended actions a user can take
	// in response to this notification. The shape is stable per `version`
	// and consumers should branch on `version` when deserializing.
	// Populated on single-resource reads (`GET /user-notifications/{id}`);
	// omitted from list responses.
	RecommendedActions *NotificationsV1RecommendedActions `json:"recommended_actions,omitempty"`

	// Resource The Confluent Cloud resource this notification relates to, embedded
	// as a point-in-time snapshot at delivery time. Values remain accurate
	// even if the underlying resource is later renamed or deleted.
	Resource NotificationsV1ResourceSnapshot `json:"resource"`

	// Severity The severity level of the notification.
	// - CRITICAL: a high impact notification which needs immediate attention.
	// - WARN: a warning notification which can be addressed now or later.
	// - INFO: an informational notification.
	Severity *string `json:"severity,omitempty"`
} {
	return r.JSON200
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1UserNotificationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1UserNotificationResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1UserNotificationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1UserNotificationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1UserNotificationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON200() *struct {
	// Actions Ordered list of user-facing actions associated with this notification.
	// The first entry is the primary action (`role: PRIMARY`) and is always
	// present; subsequent entries are secondary. Cardinality is open-ended —
	// additional actions may be added over time without a breaking schema
	// change.
	Actions []NotificationsV1NotificationAction `json:"actions"`

	// ApiVersion APIVersion defines the schema version of this representation of a resource.
	ApiVersion UpdateNotificationsV1UserNotification200JSONResponseBodyApiVersion `json:"api_version"`

	// Id ID is the "natural identifier" for an object within its scope/namespace; it is normally unique across time but not space. That is, you can assume that the ID will not be reclaimed and reused after an object is deleted ("time"); however, it may collide with IDs for other object `kinds` or objects of the same `kind` within a different scope/namespace ("space").
	Id string `json:"id"`

	// Integrations The integrations this notification was delivered to. Each entry is a
	// point-in-time snapshot of the integration at delivery time, so values
	// remain accurate even if the underlying `Integration` is later
	// modified or deleted. Populated on single-resource reads
	// (`GET /user-notifications/{id}`); omitted from list responses.
	Integrations *[]NotificationsV1Integration `json:"integrations,omitempty"`

	// Kind Kind defines the object this REST resource represents.
	Kind     UpdateNotificationsV1UserNotification200JSONResponseBodyKind `json:"kind"`
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

	// NotificationType The notification type that triggered this notification, embedded as
	// a point-in-time snapshot at delivery time so values remain accurate
	// even if the underlying `NotificationType` is later modified.
	NotificationType NotificationsV1NotificationType `json:"notification_type"`

	// Read Whether the notification has been read by the user.
	Read bool `json:"read"`

	// ReadAt The time the notification was marked as read, or `null` if it is unread.
	ReadAt *time.Time `json:"read_at,omitempty"`

	// ReceivedAt The time the underlying event was generated.
	ReceivedAt time.Time `json:"received_at"`

	// RecommendedActions Versioned payload describing the recommended actions a user can take
	// in response to this notification. The shape is stable per `version`
	// and consumers should branch on `version` when deserializing.
	// Populated on single-resource reads (`GET /user-notifications/{id}`);
	// omitted from list responses.
	RecommendedActions *NotificationsV1RecommendedActions `json:"recommended_actions,omitempty"`

	// Resource The Confluent Cloud resource this notification relates to, embedded
	// as a point-in-time snapshot at delivery time. Values remain accurate
	// even if the underlying resource is later renamed or deleted.
	Resource NotificationsV1ResourceSnapshot `json:"resource"`

	// Severity The severity level of the notification.
	// - CRITICAL: a high impact notification which needs immediate attention.
	// - WARN: a warning notification which can be addressed now or later.
	// - INFO: an informational notification.
	Severity *string `json:"severity,omitempty"`
} {
	return r.JSON200
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON409() *ConflictError {
	return r.JSON409
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON422() *ValidationError {
	return r.JSON422
}
func (r UpdateNotificationsV1UserNotificationResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r UpdateNotificationsV1UserNotificationResponse) GetBody() []byte {
	return r.Body
}
func (r UpdateNotificationsV1UserNotificationResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r UpdateNotificationsV1UserNotificationResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r UpdateNotificationsV1UserNotificationResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r MarkAllNotificationsV1UserNotificationsResponse) GetBody() []byte {
	return r.Body
}
func (r MarkAllNotificationsV1UserNotificationsResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r MarkAllNotificationsV1UserNotificationsResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r MarkAllNotificationsV1UserNotificationsResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON200() *NotificationsV1Summary {
	return r.JSON200
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON400() *BadRequestError {
	return r.JSON400
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON401() *UnauthenticatedError {
	return r.JSON401
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON403() *UnauthorizedError {
	return r.JSON403
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON404() *NotFoundError {
	return r.JSON404
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetJSON500() *DefaultSystemError {
	return r.JSON500
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) GetBody() []byte {
	return r.Body
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) Status() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Status
	}
	return http.StatusText(0)
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) StatusCode() int {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.StatusCode
	}
	return 0
}
func (r GetNotificationsV1UserNotificationsSummaryResponse) ContentType() string {
	if r.HTTPResponse != nil {
		return r.HTTPResponse.Header.Get("Content-Type")
	}
	return ""
}
