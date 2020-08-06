package core

import (
	"testing"
	"fmt"
	"github.com/tal-tech/xredis/core"
)

//该初始化方式，仅供单测参考。具体初始化方式，请参考文件xredis_test.go。
var xredis = new(XesRedis)

func init() {
	xredis.handler = &TestHandler{}
}

type TestHandler struct {
}

func (this *TestHandler) GetKey(xesRedis core.XesRedisBase) (ret string) {
	ret = fmt.Sprintf(xesRedis.GetKeyName(), (xesRedis.GetKeyParams())...)
	return
}

func (this *TestHandler) GetInstance(xesRedis core.XesRedisBase) (instance string) {
	xesRedis.SetInstanceIP("127.0.0.1:6379")
	return "cache"
}

//redis cmd [Exists testKey]
func TestXesRedis_Exists(t *testing.T) {
	ret, err := xredis.Exists("testKey", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [Get testKey]
func TestXesRedis_Get(t *testing.T) {
	ret, err := xredis.Get("testKey", []interface{}{})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [Set testKey 2 ex 10]
func TestXesRedis_Set(t *testing.T) {
	ret, err := xredis.Set("testKey", []interface{}{}, "2", 10)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [SetNx testKey_1 2]
func TestXesRedis_SetNXBool(t *testing.T) {
	key := "testKey_%v"
	keyParams := []interface{}{1}
	ret, err := xredis.SetNXBool(key, keyParams, "2")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [Expire testKey_1 10]
func TestXesRedis_Expire(t *testing.T) {
	key := "testKey_%v"
	keyParams := []interface{}{1}
	//Expire time 10s
	ret, err := xredis.Expire(key, keyParams, 10)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [MSet key1_100 value1 key2 value2 key3 value3]
func TestXesRedis_MSet(t *testing.T) {
	ret, err := xredis.MSet("key1_%v", []interface{}{100}, "value1", "key2", nil, "value2", "key3", nil, "value3")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [MGet key1_100 key2 key3]
func TestXesRedis_MGet(t *testing.T) {
	ret, err := xredis.MGet("key1_%v", []interface{}{100}, "key2", nil, "key3", nil)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [HSet key_99 name lily]
func TestXesRedis_HSet(t *testing.T) {
	ret, err := xredis.HSet("key_%v", []interface{}{99}, "name", "lily")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [HGet key_99 name]
func TestXesRedis_HGet(t *testing.T) {
	ret, err := xredis.HGet("key_%v", []interface{}{99}, "name")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [HMSet key_98 name lily age 10 pet hope]
func TestXesRedis_HMSet(t *testing.T) {
	info := make(map[string]interface{}, 3)
	info["name"] = "lily"
	info["age"] = 10
	info["pet"] = "hope"
	ret, err := xredis.HMSet("key_%v", []interface{}{98}, info)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [HMGet key_98 name age]
func TestXesRedis_HMGet(t *testing.T) {
	ret, err := xredis.HMGet("key_%v", []interface{}{98}, []interface{}{"name", "age"})
	if err != nil {
		t.Error(err)
	}
	//map[age:10 name:lily]
	t.Log(ret)
}

//redis cmd [HGetAll key_98]
func TestXesRedis_HGetAll(t *testing.T) {
	ret, err := xredis.HGetAll("key_%v", []interface{}{98})
	if err != nil {
		t.Error(err)
	}
	//map[age:10 name:lily pet:hope]
	t.Log(ret)
}

//redis cmd [HDel key_98 name age]
func TestXesRedis_HDel(t *testing.T) {
	ret, err := xredis.HDel("key_%v", []interface{}{98}, []interface{}{"name", "age"})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [Incr key_97]
func TestXesRedis_Incr(t *testing.T) {
	ret, err := xredis.Incr("key_%v", []interface{}{97})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [IncrBy key_97 20]
func TestXesRedis_IncrBy(t *testing.T) {
	ret, err := xredis.IncrBy("key_%v", []interface{}{97}, 20)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [Del key_100 key_99 key_98]
func TestXesRedis_Del(t *testing.T) {
	ret, err := xredis.Del("key_100", nil, "key_99", nil, "key_98", nil)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [ZAdd key_96 1 lily 2 mini 3 lilei]
func TestXesRedis_ZAdd(t *testing.T) {
	zParams := []interface{}{1, "lily", 2, "mini", 3, "lilei"}
	ret, err := xredis.ZAdd("key_%v", []interface{}{96}, zParams)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [ZScore key_96 lilei]
func TestXesRedis_ZScore(t *testing.T) {
	ret, err := xredis.ZScore("key_%v", []interface{}{96}, "lilei")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [ZIncrBy key_96 2 lilei]
func TestXesRedis_ZIncrBy(t *testing.T) {
	ret, err := xredis.ZIncrBy("key_%v", []interface{}{96}, 2, "lilei")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret) //5
}

//redis cmd [ZRange key_96 0 -1 withscores]
func TestXesRedis_ZRangeWithScores(t *testing.T) {
	ret, err := xredis.ZRangeWithScores("key_%v", []interface{}{96}, 0, -1)
	if err != nil {
		t.Error(err)
	}
	//[{1 lily} {2 mini} {5 lilei}]
	t.Log(ret)
}

//redis cmd [ZCount key_96 0 3]
func TestXesRedis_ZCount(t *testing.T) {
	ret, err := xredis.ZCount("key_%v", []interface{}{96}, 0, 3)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [ZRange key_96 0 -1]
func TestXesRedis_ZRange(t *testing.T) {
	ret, err := xredis.ZRange("key_%v", []interface{}{96}, 0, -1)
	if err != nil {
		t.Error(err)
	}
	//[lily mini lilei]
	t.Log(ret)
}

//redis cmd [ZRangeByScore key_96 0 3]
func TestXesRedis_ZRangeByScore(t *testing.T) {
	ret, err := xredis.ZRangeByScore("key_%v", []interface{}{96}, 0, 3)
	if err != nil {
		t.Error(err)
	}
	// [lily mini]
	t.Log(ret)
}

//redis cmd [ZRangeByScore key_96 0 3 withscores]
func TestXesRedis_ZRangeByScoreWithSocres(t *testing.T) {
	ret, err := xredis.ZRangeByScoreWithSocres("key_%v", []interface{}{96}, 0, 3)
	if err != nil {
		t.Error(err)
	}
	// [{1 lily} {2 mini}]
	t.Log(ret)
}

//redis cmd [ZRank key_96 lilei]
func TestXesRedis_ZRank(t *testing.T) {
	ret, err := xredis.ZRank("key_%v", []interface{}{96}, "lilei")
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [LPush key_95 1 2 3 4 5]
func TestXesRedis_LPush(t *testing.T) {
	ret, err := xredis.LPush("key_%v", []interface{}{95}, []interface{}{1, 2, 3, 4, 5})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [LLen key_95]
func TestXesRedis_LLen(t *testing.T) {
	ret, err := xredis.LLen("key_%v", []interface{}{95})
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}

//redis cmd [LTrim key_95 1 -1]
func TestXesRedis_LTrim(t *testing.T) {
	ret, err := xredis.LTrim("key_%v", []interface{}{95}, 1, -1)
	if err != nil {
		t.Error(err)
	}
	t.Log(ret)
}
