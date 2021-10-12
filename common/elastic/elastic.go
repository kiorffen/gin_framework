// description: A Es Client that init, parse field from source
// author: tonytang

package elastic

import (
	"encoding/json"

	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticConf struct {
	Address  string
	MaxRetry int
	User     string
	Password string
}

type ElasticClient struct {
	Client *elastic.Client

	address  string
	maxRetry int
	user     string
	password string
}

func New(conf ElasticConf) (*ElasticClient, error) {
	e := &ElasticClient{
		address:  conf.Address,
		maxRetry: conf.MaxRetry,
		user:     conf.User,
		password: conf.Password,
	}

	var err error
	e.Client, err = elastic.NewClient(elastic.SetURL(conf.Address),
		elastic.SetMaxRetries(conf.MaxRetry),
		elastic.SetBasicAuth(conf.User, conf.Password))
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (c *ElasticClient) ParseDoc(source *json.RawMessage, fields []string) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	var jdata map[string]interface{}
	err := json.Unmarshal(*source, &jdata)
	if err != nil {
		return res, nil
	}

	for _, field := range fields {
		res[field], _ = jdata[field]
	}

	return res, nil
}
