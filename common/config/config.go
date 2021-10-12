package config

import (
	"encoding/json"
	"io/ioutil"
)

type GlobalConf struct {
	ServerName        string        `json:"server_name"`
	ServerPort        string        `json:"server_port"`
	ServerWaitTimeout int           `json:"server_wait_timeout"`
	Env               string        `json:"env"`
	Interval          int           `json:"interval"`
	Log               LogConf       `json:"log"`
	Mysql             []MysqlConf   `json:"mysql"`
	Redis             []RedisConf   `json:"redis"`
	Elastic           []ElasticConf `json:"elastic"`
}
type LogConf struct {
	FilePath string `json:"file_path"`
	MaxDays  int    `json:"max_days"`
	Level    int    `json:"level"`
}

type MysqlConf struct {
	Name    string `json:"name"`
	Addr    string `json:"addr"`
	Timeout int    `json:"timeout"`
	MaxIdle int    `json:"max_idle"`
	MaxOpen int    `json:"max_open"`
}

type RedisConf struct {
	Name      string `json:"name"`
	Addr      string `json:"addr"`
	Timeout   int    `json:"timeout"`
	Password  string `json:"password"`
	MaxIdle   int    `json:"max_idle"`
	MaxActive int    `json:"max_active"`
}

type ElasticConf struct {
	Name     string `json:"name"`
	Addr     string `json:"addr"`
	MaxRetry int    `json:"max_retry"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func (c *GlobalConf) ParseConf(confFile string) error {
	var err error

	bdata, err := ioutil.ReadFile(confFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bdata, c)
	if err != nil {
		return err
	}

	return nil
}
