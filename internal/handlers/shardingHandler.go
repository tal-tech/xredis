package handlers

import (
	"fmt"
	"hash/crc32"
	"math"
	"strconv"

	redo "github.com/tal-tech/xredis/core"
	"github.com/tal-tech/xredis/internal/core"
	"github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
	"github.com/spf13/cast"
)

type ShardingHandler struct {
	cluster string
}

func (this *ShardingHandler) SetCluster(cluster string) {
	this.cluster = cluster
}

//GetKey returns redis key and params (If it's a stress test scenario,return benchmark_keyName+params )
func (this *ShardingHandler) GetKey(xesRedis redo.XesRedisBase) (ret string) {
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
func (this *ShardingHandler) GetInstance(xesRedis redo.XesRedisBase) (instance string) {
	machineNum, err := strconv.Atoi(confutil.GetConf("", this.cluster+".num"))
	if err != nil || machineNum == 0 {
		logger.Ex(xesRedis.GetCtx(), "Redis", "GetInstance "+this.cluster+" err %v(or value is 0)", err)
		return this.cluster
	}

	checkNum := cast.ToInt(math.Abs(cast.ToFloat64(crc32.ChecksumIEEE([]byte(xesRedis.GetKey())))))
	whichMachine := checkNum % machineNum

	machine := strconv.Itoa(whichMachine)
	shareName := this.cluster + machine
	xesRedis.SetInstanceIP(core.RedisConfMap[shareName])

	return shareName
}
