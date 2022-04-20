package cluster

import "github.com/electric-saw/ccloud-client-go/ccloud/common"

type ConfigOp string

const (
	ConfigOpSet    ConfigOp = "SET"
	ConfigOpDelete ConfigOp = "DELETE"
)

type KafkaConfig struct {
	common.BaseModel
	ClusterId   string `json:"cluster_id"`
	ToipcName   string `json:"topic_name"`
	Name        string `json:"name"`
	Value       string `json:"value"`
	IsDefault   bool   `json:"is_default"`
	IsReadOnly  bool   `json:"is_read_only"`
	IsSensitive bool   `json:"is_sensitive"`
	Source      string `json:"source"`
	Synonyms    []struct {
		Name   string `json:"name"`
		Value  string `json:"value"`
		Source string `json:"source"`
	} `json:"synonyms"`
}

type KafkaConfigList struct {
	common.BaseModel
	Data []KafkaConfig `json:"data"`
}

type KafkaConfigUpdateReq struct {
	Value string `json:"value"`
}

type KafkaConfigUpdateBatch struct {
	Data []struct {
		Name      string   `json:"name"`
		Value     string   `json:"value"`
		Operation ConfigOp `json:"operation"`
	} `json:"data"`
}
