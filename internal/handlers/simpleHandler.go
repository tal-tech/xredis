package handlers

import (
	"fmt"

	redo "github.com/tal-tech/xredis/core"
	"github.com/tal-tech/xredis/internal/core"
	"github.com/spf13/cast"
)

type SimpleHandler struct {
	instance string
}

func (this *SimpleHandler) SetInstance(instance string) {
	this.instance = instance
}

//GetKey returns redis key and params (If it's a stress test scenario,return benchmark_keyName+params)
func (this *SimpleHandler) GetKey(xesRedis redo.XesRedisBase) (ret string) {
	defer func() {
		if xesRedis.GetCtx() == nil {
			return
		}
		bench := xesRedis.GetCtx().Value("IS_BENCHMARK")
		if cast.ToString(bench) == "1" {
			ret = "benchmark_" + ret
		}
	}()
	ret = fmt.Sprintf(xesRedis.GetKeyName(), (xesRedis.GetKeyParams())...)
	return
}

//GetInstance returns redis client config's key.and setting redis server Host into redis client.
func (this *SimpleHandler) GetInstance(xesRedis redo.XesRedisBase) (instance string) {
	xesRedis.SetInstanceIP(core.RedisConfMap[this.instance])
	return this.instance
}
