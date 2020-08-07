package xredis

import (
	"testing"
	"context"
	"time"
	"github.com/go-redis/redis"
	"github.com/tal-tech/xredis/core"
)
//go test -args -c /home/.../conf/conf.ini -v -test.run TestNewXesRedis
func TestNewXesRedis(t *testing.T) {
	//If your configuration file has the following configuration,you can use this function.
	//[Redis]
	//cache=127.0.0.1:6379
	xredis := NewXesRedis()
	xredis.Set("key", []interface{}{}, "test", 0)
	v, err := xredis.Get("key", []interface{}{})
	if err != nil {
		t.Error(err)
	}

	if v == "test" {
		t.Log(v)
	}
}

//**************************************************************************************************
type TestHandler struct {
	xesRedis core.XesRedisBase
}

func (h *TestHandler) GetInstance() string {
	h.xesRedis.SetInstanceIP("127.0.0.1:6379")
	return "cache"
}

func (h *TestHandler) GetKey() string {
	return h.xesRedis.GetKey()
}

func (h *TestHandler) SetXesRedis(xesredis core.XesRedisBase) {
	h.xesRedis = xesredis
}

//go test -args -c /home/.../conf/conf.ini -v -test.run TestNewXesRedisOfCtx
func TestNewXesRedisOfCtx(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "handler", &TestHandler{})
	xredis := NewXesRedisOfCtx(ctx)
	xredis.Set("ctxKey", []interface{}{}, "test", 0)
	v, err := xredis.Get("ctxKey", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	if v == "test" {
		t.Log(v)
	}
}

//**************************************************************************************************
//go test -args -c /home/.../conf/conf.ini -v -test.run TestNewSimpleXesRedis
func TestNewSimpleXesRedis(t *testing.T) {
	//If your configuration file has the following configuration,you can use this function.
	//[Redis]
	//tw=192.168.7.2:6000 192.168.7.3:6000

	xredis := NewSimpleXesRedis(context.Background(), "cache")
	xredis.Set("key", []interface{}{}, "test", 0)
	v, err := xredis.Get("key", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	if v == "test" {
		t.Log(v)
	}
}

//**************************************************************************************************
//go test -args -c /home/.../conf/conf.ini -v -test.run TestNewShardingXesRedis
func TestNewShardingXesRedis(t *testing.T) {
	//If your configuration file has the following configuration,you can use this function.
	//[Redis]
	//shareding0=192.168.7.5:6000
	//shareding1=192.168.7.6:6000
	//shareding2=192.168.7.7:6000
	v1 := "value1"
	v2 := "value2"
	v3 := "value3"
	ins := NewShardingXesRedis(context.Background(), "shareding")
	_, err := ins.Set("shardKey", nil, v1, 0)
	if err != nil {
		t.Error(err)
	}
	_, err = ins.Set("shardKey_%v_%v_%v", []interface{}{1, 2, 3}, v2, 0)
	if err != nil {
		t.Error(err)
	}
	_, err = ins.Set("shardKey_%v_%v_%v", []interface{}{71, 81, 91}, v3, 0)
	if err != nil {
		t.Error(err)
	}

	_v1, err := ins.Get("shardKey", nil)
	if err != nil {
		t.Errorf("get shardkey err :%v", err)
	}
	if _v1 != v1 {
		t.Error("get value err!")
	}

	_v2, err := ins.Get("shardKey_%v_%v_%v", []interface{}{1, 2, 3})
	if err != nil {
		t.Errorf("get key2 err :%+v", err)
	}
	if _v2 != v2 {
		t.Error("get value err!")
	}

	_v3, err := ins.Get("shardKey_%v_%v_%v", []interface{}{71, 81, 91})
	if err != nil {
		t.Errorf("get key3 err :%+v", err)
	}
	if _v3 != v3 {
		t.Error("get value err!")
	}
}

//**************************************************************************************************
//go test -args -c /home/.../conf/conf.ini -v -test.run TestAddRedis
func TestAddRedis(t *testing.T) {
	//If you don't have a config file.
	options := redis.Options{
		Password:    "",
		DB:          0,
		PoolSize:    100,
		IdleTimeout: 240 * time.Second,
		ReadTimeout: 5 * time.Second,
	}
	AddRedis("test", "127.0.0.1:6379", options)
	xredis := NewSimpleXesRedis(context.Background(), "test")
	xredis.Set("key", []interface{}{}, 666, 0)
	v, err := xredis.Get("key", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	if v == "666" {
		t.Log(v)
	}
}

//**************************************************************************************************
//go test -args -c any -v -test.run TestNewRedisWithAnyConf
func TestNewRedisWithAnyConf(t *testing.T) {
	//If you want to use a custom configuration file...
	xredis := NewSimpleXesRedis(context.Background(), "test")
	xredis.Set("key", []interface{}{}, 13232, 0)
	v, err := xredis.Get("key", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	if v == "13232" {
		t.Log(v)
	}
}
