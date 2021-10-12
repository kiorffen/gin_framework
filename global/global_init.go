package global

import (
	"errors"
	"fmt"
	"time"

	"gin_framework/common/config"

	"gin_framework/common/bcache"
	log "gin_framework/common/logger"

	es "gin_framework/common/elastic"
	mc "gin_framework/common/mysql"
	rc "gin_framework/common/redis"

	"go.uber.org/zap"
)

var (
	G_conf config.GlobalConf

	G_logger *log.Logger

	G_mc    map[string]*mc.Mysql
	G_rc    map[string]*rc.Redis
	G_es    map[string]*es.ElasticClient
	G_cache map[string]*bcache.Bcache

	G_env   string
	G_debug bool
)

func init() {
	var err error
	err = G_conf.ParseConf("./conf/main.conf")
	if err != nil {
		fmt.Println("ParseConf failed")
		panic(err)
	}

	err = globalInit()
	if err != nil {
		panic(err)
	}
}

func globalInit() error {
	var err error

	err = initLogger()
	if err != nil {
		fmt.Println("init logger failed. err: " + err.Error())
		return err
	}

	//err = initMysqlClient()
	//if err != nil {
	//	G_logger.Logger().Fatal("init mysql failed", zap.String("errmsg", err.Error()))
	//	return err
	//}

	//err = initElasticClient()
	//if err != nil {
	//	G_logger.Logger().Fatal("init elastic failed", zap.String("errmsg", err.Error()))
	//	return err
	//}

	//err = initRedisClient()
	//if err != nil {
	//	G_logger.Logger().Fatal("init redis failed", zap.String("errmsg", err.Error()))
	//	return err
	//}

	err = initLocalCache()
	if err != nil {
		G_logger.Logger().Fatal("init local cache failed", zap.String("errmsg", err.Error()))
		return err
	}

	return nil
}

func initLogger() error {
	var err error

	conf := log.LoggerConf{
		FilePath:    G_conf.Log.FilePath,
		IsLocalTime: true,
		MaxSize:     1024,
		MaxBackups:  30,
		MaxDays:     G_conf.Log.MaxDays,
		IsCompress:  false,
		Level:       G_conf.Log.Level,
		ServerName:  G_conf.ServerName,
	}

	G_logger, err = log.NewLogger(conf)
	if err != nil {
		return err
	}

	return nil
}

func initMysqlClient() error {
	G_mc = make(map[string]*mc.Mysql)

	for _, item := range G_conf.Mysql {
		mysqlConf := mc.MysqlConf{
			Address:      item.Addr,
			Timeout:      time.Duration(item.Timeout) * time.Second,
			MaxIdleConns: item.MaxIdle,
			MaxOpenConns: item.MaxOpen,
		}

		m := mc.New(mysqlConf)
		if m == nil {
			return errors.New("create mysql client failed. name: " + item.Name)
		}

		G_mc[item.Name] = m
	}

	return nil
}

func initElasticClient() error {
	for _, item := range G_conf.Elastic {
		esConf := es.ElasticConf{
			Address:  item.Addr,
			MaxRetry: item.MaxRetry,
			User:     item.User,
			Password: item.Password,
		}

		e, err := es.New(esConf)
		if err != nil {
			return err
		}

		G_es[item.Name] = e
	}

	return nil
}

func initRedisClient() error {
	for _, item := range G_conf.Redis {
		redisConf := rc.RedisConf{
			Address:   item.Addr,
			Timeout:   time.Duration(item.Timeout) * time.Second,
			Password:  item.Password,
			MaxIdle:   item.MaxIdle,
			MaxActive: item.MaxActive,
		}

		r := rc.New(redisConf)
		if r == nil {
			return errors.New("create redis failed.")
		}

		G_rc[item.Name] = r
	}

	return nil
}

func initLocalCache() error {
	G_cache = make(map[string]*bcache.Bcache)

	G_cache["content_info"] = bcache.NewBcache("content_info", 128*1024).Ttl(time.Second * 60)

	return nil
}
