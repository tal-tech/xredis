/*===============================================================
*   Copyright (C) 2020 All rights reserved.
*
*   FileName：xredis.go
*   Author：WuGuoFu
*   Date： 2020-03-19
*   Description：
*
================================================================*/
package xredis

import (
	"context"

	redo "github.com/tal-tech/xredis/handler"
	"github.com/tal-tech/xredis/internal/core"
	"github.com/tal-tech/xredis/internal/handlers"
	"github.com/go-redis/redis"
)

// NewXesRedis returns a client to the Redis Server specified by default options.
func NewXesRedis() *core.XesRedis {
	instance_XesRedis := new(core.XesRedis)
	handler := &handlers.DefaultHandler{}
	instance_XesRedis.SetHandler(handler)
	return instance_XesRedis
}

// NewXesRedisOfCtx returns a client to the Redis Server that you can Specify handler.
func NewXesRedisOfCtx(ctx context.Context) *core.XesRedis {
	instance_XesRedis := new(core.XesRedis)
	instance_XesRedis.SetCtx(ctx)
	if hdler, ok := ctx.Value("handler").(func() redo.XesRedisHandler); ok {
		instance_XesRedis.SetHandler(hdler())
	} else {
		handler := &handlers.DefaultHandler{}
		instance_XesRedis.SetHandler(handler)
	}
	return instance_XesRedis
}

//NewSimpleXesRedis returns a client to the Redis Server specified by instance.
func NewSimpleXesRedis(ctx context.Context, instance string) *core.XesRedis {
	instance_XesRedis := new(core.XesRedis)
	instance_XesRedis.SetCtx(ctx)
	handler := &handlers.SimpleHandler{}
	handler.SetInstance(instance)
	instance_XesRedis.SetHandler(handler)
	return instance_XesRedis
}

//NewShardingXesRedis returns a client to the Redis Server specified by cluster Name.
func NewShardingXesRedis(ctx context.Context, cluster string) *core.XesRedis {
	instance_XesRedis := new(core.XesRedis)
	instance_XesRedis.SetCtx(ctx)
	handler := &handlers.ShardingHandler{}
	handler.SetCluster(cluster)
	instance_XesRedis.SetHandler(handler)
	return instance_XesRedis
}

//GetClient return the client of the original reference package "go-redis".
func GetClient(key string) *redis.Client {
	selectors := core.GetSelectors()
	selectorIns, ok := selectors[key]
	if !ok {
		return nil
	}
	server := selectorIns.Select()
	return core.GetRedisClient(server)
}

//AddRedis can set redis option without reading config file
func AddRedis(key, addr string, options redis.Options) {
	core.AddRedis(key, addr, options)
}
