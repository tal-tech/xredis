package handler

import "github.com/tal-tech/xredis/core"

type XesRedisHandler interface {
	GetInstance(xesRedis core.XesRedisBase) string
	GetKey(xesRedis core.XesRedisBase) string
}
