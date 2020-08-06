package core

import (
	"context"
	"sync"
	redo "github.com/tal-tech/xredis/handler"
	"github.com/go-redis/redis"
)

var redisCmd = struct {
	sync.RWMutex
	rMap map[string]string
}{rMap: make(map[string]string, 0)}

type XesRedis struct {
	instance   string        `json:"instance"`
	instanceIP string        `json:"instanceip"`
	keyName    string        `json:"keyname"`
	keyParams  []interface{} `json:"keyparams"`
	cmd        string        `json:"cmd"`
	key        string        `json:"key"`
	args       interface{}   `json:"args"`

	pipeLiners map[string]*PipelineIns
	goPipeLine bool
	async      bool

	loggerTag string

	ctx     context.Context
	handler redo.XesRedisHandler
}

type RedisResult struct {
	value interface{}
	err   error
}

type PipelineIns struct {
	pipeliner redis.Pipeliner
	used      bool
}
