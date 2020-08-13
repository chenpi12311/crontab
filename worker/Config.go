package worker

import (
	"encoding/json"
	"io/ioutil"
)

// Config 程序配置
type Config struct {
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int64    `json:"mongodbConnectTimeout"`
	MongodbDatabase       string   `json:"mongodbDatabase"`
	MongodbCollection     string   `json:"mongodbCollection"`
	JobLogBatchSize       int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout   int64    `json: "jobLogCommitTimeout"`
}

var (
	// 单例
	G_config *Config
)

// InitConfig 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 1. 把配置文件读过来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2. 把JSON反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3. 赋值单例
	G_config = &conf

	return
}
