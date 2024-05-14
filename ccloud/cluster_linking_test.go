package ccloud_test

import (
	"os"
	"testing"

	"github.com/electric-saw/ccloud-client-go/ccloud/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/joho/godotenv"
)

func TestGetClusterLinking(t *testing.T) {
	err := godotenv.Load()
	assert.NoError(t, err)
	
	c, err := makeClusterClient()
	assert.NoError(t, err)

	linking, err := c.GetClusterLinking()
	assert.NoError(t, err)

	assert.NotNil(t, linking)
}

func TestGetClusterLinkingConfig(t *testing.T) {
	err := godotenv.Load()
	assert.NoError(t, err)
	
	c, err := makeClusterClient()
	assert.NoError(t, err)

	linking, err := c.GetClusterLinkingConfig(os.Getenv("LINK_NAME"))
	assert.NoError(t, err)

	assert.NotNil(t, linking)
}

func TestCreateMirrorTopic(t *testing.T) {
	err := godotenv.Load()
	assert.NoError(t, err)
	
	c, err := makeClusterClient()
	assert.NoError(t, err)

	err = c.CreateMirrorTopics(os.Getenv("LINK_NAME_DR"), os.Getenv("TOPIC_NAME"), os.Getenv("MIRROR_TOPIC_NAME"))
	assert.NoError(t, err)

}

func makeClusterClient() (*cluster.ConfluentClusterClient, error) {
	//user, password, clusterId, clusterUrl string
	key := os.Getenv("CLUSTER_USER_DR")
	secret := os.Getenv("CLUSTER_PASSWORD_DR")
	clusterId := os.Getenv("CLUSTER_ID_DR")
	clusterUrl := os.Getenv("CLUSTER_URL_DR")
	return cluster.NewClusterClient(key, secret, clusterId, clusterUrl)
}