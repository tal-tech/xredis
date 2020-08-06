package core

import (
	"github.com/tal-tech/loggerX"
	"github.com/go-redis/redis"
	"github.com/spf13/cast"
)

func (this *XesRedis) execCmd() (res RedisResult) {
	if this.async {
		this.async = false
		this.goRoutine()
	} else {
		res = this.exec()
	}
	return
}

func (this *XesRedis) Exists(keyName string, keyParams []interface{}) (back bool, err error) {
	this.setInfo(keyName, keyParams, "Exists", []interface{}{})

	res := this.exec()
	return res.ToBool()

}

func (this *XesRedis) Get(keyName string, keyParams []interface{}) (back string, err error) {
	this.setInfo(keyName, keyParams, "Get", []interface{}{})

	res := this.exec()
	return res.ToString()
}

func (this *XesRedis) GetSet(keyName string, keyParams []interface{}, value interface{}) (back string, err error) {
	this.setInfo(keyName, keyParams, "GetSet", []interface{}{value})

	res := this.exec()
	return res.ToString()
}

func (this *XesRedis) Set(keyName string, keyParams []interface{}, value interface{}, ex int64) (back string, err error) {
	args := []interface{}{value}
	if ex > 0 {
		args = append(args, "ex")
		args = append(args, ex)
	}
	this.setInfo(keyName, keyParams, "Set", args)
	res := this.execCmd()
	return res.ToString()
}

//返回类型不匹配，请使用SetNXBool，已废弃，兼容已上线服务保留
func (this *XesRedis) SetNX(keyName string, keyParams []interface{}, value interface{}, ex int64) (back string, err error) {
	args := []interface{}{value}
	if ex > 0 {
		args = append(args, "ex")
		args = append(args, ex)
	}
	args = append(args, "nx")
	this.setInfo(keyName, keyParams, "Set", args)

	return this.execCmd().ToString()
}

func (this *XesRedis) SetNXBool(keyName string, keyParams []interface{}, value interface{}) (back bool, err error) {
	args := []interface{}{value}
	this.setInfo(keyName, keyParams, "SetNx", args)

	back, err = this.execCmd().ToBool()
	if err == redis.Nil {
		err = nil
	}
	return
}

//	NOTE: MGet("key1_%v",[]interface{}{100},"key2",nil,"key3",nil)
func (this *XesRedis) MGet(keyName string, keyParams []interface{}, otherKeys ...interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "MGet", []interface{}{})
	if len(otherKeys) > 0 {
		if len(otherKeys)%2 != 0 {
			return nil, logger.NewError("MGet Args Count Error")
		}
		args := make([]interface{}, 0)
		for i := 0; i < len(otherKeys); i += 2 {
			args = append(args, this.key)
			params, _ := otherKeys[i+1].([]interface{})
			this.setInfo(cast.ToString(otherKeys[i]), params, "MGet", []interface{}{})
		}
		args = append(args, this.key)
		this.key = ""
		this.args = args
	}

	res := this.exec()
	return res.ToStringSlice()
}

//	NOTE: MSet("key1_%v",[]interface{}{100},"value1","key2",nil,"value2","key3",nil,"value3")
func (this *XesRedis) MSet(keyName string, keyParams []interface{}, value interface{}, otherKeys ...interface{}) (back string, err error) {
	args := make([]interface{}, 0)
	args = append(args, value)
	this.setInfo(keyName, keyParams, "MSet", args)
	if len(otherKeys) > 0 {
		if len(otherKeys)%3 != 0 {
			return "", logger.NewError("MSet Args Count Error")
		}
		args = make([]interface{}, 0)
		args = append(args, this.key)
		args = append(args, value)
		for i := 0; i < len(otherKeys); i += 3 {
			params, _ := otherKeys[i+1].([]interface{})
			this.setInfo(cast.ToString(otherKeys[i]), params, "MSet", []interface{}{})
			args = append(args, this.key)
			args = append(args, otherKeys[i+2])
		}
		this.key = ""
		this.args = args
	}

	res := this.execCmd()
	return res.ToString()
}

func (this *XesRedis) HGet(keyName string, keyParams []interface{}, filed interface{}) (back string, err error) {
	this.setInfo(keyName, keyParams, "HGet", []interface{}{filed})

	res := this.exec()
	return res.ToString()
}

func (this *XesRedis) HGetAll(keyName string, keyParams []interface{}) (back map[string]string, err error) {
	this.setInfo(keyName, keyParams, "HGetAll", []interface{}{})

	res := this.exec()
	return res.ToStringStringMap()
}

func (this *XesRedis) HMGet(keyName string, keyParams []interface{}, fileds []interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "HMGet", fileds)

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) HSet(keyName string, keyParams []interface{}, field interface{}, value interface{}) (back string, err error) {
	var args = make([]interface{}, 0, 2)
	args = append(args, field)
	args = append(args, value)
	this.setInfo(keyName, keyParams, "HSet", args)

	return this.execCmd().ToString()
}

func (this *XesRedis) HSetNx(keyName string, keyParams []interface{}, field interface{}, value interface{}) (back bool, err error) {
	var args = make([]interface{}, 0, 2)
	args = append(args, field)
	args = append(args, value)
	this.setInfo(keyName, keyParams, "HSetNx", args)

	return this.execCmd().ToBool()
}

func (this *XesRedis) HDel(keyName string, keyParams []interface{}, fields []interface{}) (back string, err error) {
	this.setInfo(keyName, keyParams, "HDel", fields)

	return this.execCmd().ToString()
}

func (this *XesRedis) HMSet(keyName string, keyParams []interface{}, value map[string]interface{}) (back string, err error) {
	var args = make([]interface{}, 0, len(value)*2)
	for k, v := range value {
		args = append(args, k)
		args = append(args, v)
	}
	this.setInfo(keyName, keyParams, "HMSet", args)

	return this.execCmd().ToString()
}

func (this *XesRedis) Incr(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "Incr", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) IncrBy(keyName string, keyParams []interface{}, increment interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "IncrBy", []interface{}{increment})

	return this.execCmd().ToInt64()
}

//	NOTE: support multi del  Del("key1",nil,"key2",nil,"key3",nil)
func (this *XesRedis) Del(keyName string, keyParams []interface{}, otherKeys ...interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "Del", []interface{}{})
	if len(otherKeys) > 0 {
		if len(otherKeys)%2 != 0 {
			return 0, logger.NewError("Del Args Count Error")
		}
		args := make([]interface{}, 0)
		for i := 0; i < len(otherKeys); i += 2 {
			args = append(args, this.key)
			params, _ := otherKeys[i+1].([]interface{})
			this.setInfo(cast.ToString(otherKeys[i]), params, "Del", []interface{}{})
		}
		args = append(args, this.key)
		this.key = ""
		this.args = args
	}

	return this.execCmd().ToInt64()
}

func (this *XesRedis) ZAdd(keyName string, keyParams []interface{}, zParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "ZAdd", zParams)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) ZScore(keyName string, keyParams []interface{}, member string) (back float64, err error) {
	this.setInfo(keyName, keyParams, "ZScore", []interface{}{member})

	res := this.exec()
	return res.ToFloat64()

}

func (this *XesRedis) ZIncrBy(keyName string, keyParams []interface{}, increment interface{}, member string) (back float64, err error) {
	this.setInfo(keyName, keyParams, "ZIncrBy", []interface{}{increment, member})

	return this.execCmd().ToFloat64()
}

func (this *XesRedis) ZRangeWithScores(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back []redis.Z, err error) {
	args := []interface{}{start, end}
	args = append(args, "withscores")

	this.setInfo(keyName, keyParams, "ZRange", args)

	res := this.exec()
	return res.ToZSlice()

}

func (this *XesRedis) ZCount(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "ZCount", []interface{}{start, end})

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) ZRange(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "ZRange", []interface{}{start, end})

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) ZRangeByScore(keyName string, keyParams []interface{}, start interface{}, end interface{}, limits ...interface{}) (back []string, err error) {
	args := []interface{}{start, end}
	if len(limits) == 3 {
		args = append(args, limits...)
	}

	this.setInfo(keyName, keyParams, "ZRangeByScore", args)

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) ZRangeByScoreWithSocres(keyName string, keyParams []interface{}, start interface{}, end interface{}, limits ...interface{}) (back []redis.Z, err error) {
	args := []interface{}{start, end, "withscores"}
	if len(limits) == 3 {
		args = append(args, limits...)
	}

	this.setInfo(keyName, keyParams, "ZRangeByScore", args)

	res := this.exec()
	return res.ToZSlice()
}

func (this *XesRedis) ZRevRangeByScore(keyName string, keyParams []interface{}, start interface{}, end interface{}, limits ...interface{}) (back []string, err error) {
	args := []interface{}{start, end}
	if len(limits) == 3 {
		args = append(args, limits...)
	}

	this.setInfo(keyName, keyParams, "ZRevRangeByScore", args)

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) ZRevRangeByScoreWithSocres(keyName string, keyParams []interface{}, start interface{}, end interface{}, limits ...interface{}) (back []redis.Z, err error) {
	args := []interface{}{start, end, "withscores"}
	if len(limits) == 3 {
		args = append(args, limits...)
	}

	this.setInfo(keyName, keyParams, "ZRevRangeByScore", args)

	res := this.exec()
	return res.ToZSlice()
}

func (this *XesRedis) ZRevRangeWithScores(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back []redis.Z, err error) {
	args := []interface{}{start, end}
	args = append(args, "withscores")

	this.setInfo(keyName, keyParams, "ZRevRange", args)

	res := this.exec()
	return res.ToZSlice()
}

func (this *XesRedis) ZRevRange(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "ZRevRange", []interface{}{start, end})

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) ZRank(keyName string, keyParams []interface{}, member string) (back int64, err error) {
	this.setInfo(keyName, keyParams, "ZRank", []interface{}{member})

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) ZRevRank(keyName string, keyParams []interface{}, member string) (back int64, err error) {
	this.setInfo(keyName, keyParams, "ZRevRank", []interface{}{member})

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) LPush(keyName string, keyParams []interface{}, value []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "LPush", value)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) LLen(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "LLen", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) LRem(keyName string, keyParams []interface{}, value interface{}, count int) (back int64, err error) {
	this.setInfo(keyName, keyParams, "LRem", []interface{}{count, value})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) LTrim(keyName string, keyParams []interface{}, start, stop int) (back int64, err error) {
	this.setInfo(keyName, keyParams, "LTrim", []interface{}{start, stop})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) LRange(keyName string, keyParams []interface{}, start interface{}, end interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "LRange", []interface{}{start, end})

	res := this.exec()
	return res.ToStringSlice()
}

func (this *XesRedis) RPop(keyName string, keyParams []interface{}) (back string, err error) {
	this.setInfo(keyName, keyParams, "RPop", []interface{}{})

	return this.execCmd().ToString()
}

func (this *XesRedis) RPush(keyName string, keyParams []interface{}, value []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "RPush", value)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) Expire(keyName string, keyParams []interface{}, value interface{}) (back bool, err error) {
	this.setInfo(keyName, keyParams, "Expire", []interface{}{value})

	return this.execCmd().ToBool()
}

func (this *XesRedis) IncrByFloat(keyName string, keyParams []interface{}, value interface{}) (back float64, err error) {
	this.setInfo(keyName, keyParams, "IncrByFloat", []interface{}{value})

	return this.execCmd().ToFloat64()
}

func (this *XesRedis) Decr(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "Decr", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) DecrBy(keyName string, keyParams []interface{}, decrement interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "DecrBy", []interface{}{decrement})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) HIncrBy(keyName string, keyParams []interface{}, field interface{}, increment interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "HIncrBy", []interface{}{field, increment})

	return this.execCmd().ToInt64()
}

//exec redis lua
func (this *XesRedis) Eval(instance string, script string, keys []string, args ...interface{}) (interface{}, error) {
	client := GetClient(instance)
	res := client.Eval(script, keys, args...)
	return res.Result()
}

func (this *XesRedis) ZCard(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "ZCard", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) HLen(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "HLen", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) Hvals(keyName string, keyParams []interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "Hvals", []interface{}{})
	return this.execCmd().ToStringSlice()
}

func (this *XesRedis) Hkeys(keyName string, keyParams []interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "Hkeys", []interface{}{})
	return this.execCmd().ToStringSlice()
}

func (this *XesRedis) ZRemRangeByRank(keyName string, keyParams []interface{}, start interface{}, end interface{}) (int64, error) {
	this.setInfo(keyName, keyParams, "ZRemRangeByRank", []interface{}{start, end})

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) ZRemRangeByScore(keyName string, keyParams []interface{}, min interface{}, max interface{}) (int64, error) {
	this.setInfo(keyName, keyParams, "ZRemRangeByScore", []interface{}{min, max})

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) ZRem(keyName string, keyParams []interface{}, childkeys []interface{}) (int64, error) {
	this.setInfo(keyName, keyParams, "ZRem", childkeys)

	res := this.exec()
	return res.ToInt64()
}

func (this *XesRedis) SAdd(keyName string, keyParams []interface{}, members []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "SAdd", members)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) SCard(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "SCard", []interface{}{})

	return this.execCmd().ToInt64()
}

func (this *XesRedis) SDiff(keyName string, keyParams []interface{}, otherKeys ...interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "SDiff", otherKeys)

	return this.execCmd().ToStringSlice()
}

func (this *XesRedis) SRandMember(keyName string, keyParams []interface{}, count []interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "SRandMember", count)

	return this.execCmd().ToStringSlice()
}

func (this *XesRedis) SMembers(keyName string, keyParams []interface{}) (back []string, err error) {
	this.setInfo(keyName, keyParams, "SMembers", nil)

	return this.execCmd().ToStringSlice()
}

func (this *XesRedis) SisMember(keyName string, keyParams []interface{}, member []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "SisMember", member)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) SRem(keyName string, keyParams []interface{}, member []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "SRem", member)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) SUnionStore(keyName string, keyParams []interface{}, otherKeys ...interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "SUnionStore", otherKeys)

	return this.execCmd().ToInt64()
}

func (this *XesRedis) TTL(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "TTL", nil)

	return this.exec().ToInt64()
}

func (this *XesRedis) Persist(keyName string, keyParams []interface{}) (back int64, err error) {
	this.setInfo(keyName, keyParams, "Persist", nil)

	return this.exec().ToInt64()
}
