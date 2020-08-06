package handlers

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
	"strconv"
	"sync"

	redo "github.com/tal-tech/xredis/core"
	"github.com/tal-tech/xredis/internal/common"
	"github.com/tal-tech/xredis/internal/core"
	"github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
	"github.com/spf13/cast"
)

var globalConfig = struct {
	sync.RWMutex
	gMap map[string]interface{}
}{gMap: make(map[string]interface{}, 0)}

type DefaultHandler struct {
}

//Algorithm of hash on redis keyName.
//getShardingKeyInConf returns shareding redis sever name. and setting redis server host into redis client.
func (this *DefaultHandler) getShardingKeyInConf(xesRedis redo.XesRedisBase, shardingNumInConf, shardingPrefixInConf string) string {
	var clusterNum int
	if xesRedis.GetCtx() != nil && shardingPrefixInConf == "shareding" {
		clusterNum = cast.ToInt(xesRedis.GetCtx().Value("REDIS_CLUSTER"))
	}

	machineNum, err := strconv.Atoi(confutil.GetConf("", shardingNumInConf))
	if err != nil || machineNum == 0 {
		logger.Ex(xesRedis.GetCtx(), "Redis", "GetInstance "+shardingNumInConf+" err %v(or value is 0)", err)
		machineNum = 0
	}

	checkNum := cast.ToInt(math.Abs(cast.ToFloat64(crc32.ChecksumIEEE([]byte(xesRedis.GetKey())))))
	whichMachine := checkNum%machineNum + clusterNum*machineNum

	machine := strconv.Itoa(whichMachine)
	shareName := shardingPrefixInConf + machine
	xesRedis.SetInstanceIP(core.RedisConfMap[shareName])

	return shareName
}

//Ketama hash algorithm
//getFNVShardingKeyInConf returns shareding redis sever name. and setting redis server host into redis client.
func (this *DefaultHandler) getFNVShardingKeyInConf(xesRedis redo.XesRedisBase, shardingNumInConf, shardingPrefixInConf string) string {

	h := fnv.New64a()
	h.Write([]byte(xesRedis.GetKey()))
	checkNum := cast.ToUint32(h.Sum64())
	whichMachine := common.GetKetamaIndex(shardingPrefixInConf, checkNum)

	machine := strconv.Itoa(whichMachine)
	shareName := shardingPrefixInConf + machine
	xesRedis.SetInstanceIP(core.RedisConfMap[shareName])

	return shareName
}

//SetGlobalConfigMap will set a map key/value into GlobalConfigMap.
func SetGlobalConfigMap(key string, value interface{}) {
	globalConfig.Lock()
	globalConfig.gMap[key] = value
	globalConfig.Unlock()
}

//GetGlobalConfig returns the value of globalConfig.gMap
func GetGlobalConfig(key string) interface{} {
	globalConfig.RLock()
	value := globalConfig.gMap[key]
	globalConfig.RUnlock()
	return value
}

//getKeyInfo returns redis keyName.
func (this *DefaultHandler) getKeyInfo(xesRedis redo.XesRedisBase) (back map[string]interface{}) {
	ret := GetGlobalConfig(xesRedis.GetKeyName())
	if result, ok := ret.(map[string]interface{}); ok {
		return result
	}

	return cast.ToStringMap(ret)
}

//GetKey returns redis key and params (If it's a stress test scenario,return benchmark_keyName+params)
func (this *DefaultHandler) GetKey(xesRedis redo.XesRedisBase) (ret string) {
	defer func() {
		if xesRedis.GetCtx() == nil {
			return
		}
		bench := xesRedis.GetCtx().Value("IS_BENCHMARK")
		if cast.ToString(bench) == "1" {
			ret = "benchmark_" + ret
		}
	}()

	keyInfo := this.getKeyInfo(xesRedis)
	key := cast.ToString(keyInfo["key"])
	if key == "" {
		ret = xesRedis.GetKeyName()
		return
	}
	ret = fmt.Sprintf(key, (xesRedis.GetKeyParams())...)
	return
}

//[Redis]
//cache=127.0.0.1:6379
//GetInstance returns redis client config's key ,such as "cache".
//GetInstance will set redis server address into redis client.
func (this *DefaultHandler) GetInstance(xesRedis redo.XesRedisBase) (instance string) {
	conf := this.getKeyInfo(xesRedis)
	RedisConfMap := core.GetRedisConf()

	if xesRedis.GetCtx() != nil {
		if val := xesRedis.GetCtx().Value("CacheRemember"); val != nil && cast.ToBool(val) == true {
			return this.getShardingKeyInConf(xesRedis, "localredis.num", "localredis")
		}
	}
	if xesRedis.GetKey() == xesRedis.GetKeyName() {
		return "cache"
	}

	//回放的时候是否有指定的redis连接
	usePika := false
	if xesRedis.GetCtx() != nil {
		if IS_PLAYBACK := xesRedis.GetCtx().Value("IS_PLAYBACK"); IS_PLAYBACK != nil {
			if val, ok := IS_PLAYBACK.(string); ok {
				if val == "1" {
					usePika = true
				}
			}
		}
	}
	if _, ok := conf["playbackconnection"]; ok && usePika {
		logger.Dx(xesRedis.GetCtx(), "[getInstance]", "usepika IS_PLAYBACK:%s,keyInfo:%v", xesRedis.GetCtx().Value("IS_PLAYBACK"), conf)
		xesRedis.SetInstanceIP(confutil.GetConf("Redis", cast.ToString(conf["playbackconnection"])))
		instance = cast.ToString(conf["playbackconnection"])
		if instance == "playbackpika" {
			instance = this.getFNVShardingKeyInConf(xesRedis, "playbackpika.num", instance)
		}
		return
	}
	if sharding, ok := conf["sharding"].(string); ok && sharding != "" {
		return this.getShardingKeyInConf(xesRedis, "shareding.num", "shareding")
	}
	//If there is already an available link, reuse the original link.
	if connection, ok := conf["connection"]; ok {
		if conn, ok := connection.(string); ok {
			xesRedis.SetInstanceIP(RedisConfMap[conn])
			return conn
		} else {
			xesRedis.SetInstanceIP("")
			return ""
		}
	}
	//set redis server address into redis client.
	xesRedis.SetInstanceIP(RedisConfMap["cache"])
	return "cache"
}
