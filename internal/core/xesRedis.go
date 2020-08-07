package core

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	redo "github.com/tal-tech/xredis/handler"
	"github.com/tal-tech/routinePool"
	"github.com/tal-tech/xtools/commonutil"
	"github.com/tal-tech/xtools/confutil"
	"github.com/tal-tech/xtools/jsutil"
	"github.com/tal-tech/xtools/kafkautil"
	"context"
	"github.com/tal-tech/loggerX"
	"github.com/go-redis/redis"
	"github.com/spf13/cast"
)

var interPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 30)
	},
}

func init() {
	read := "Read"
	write := "Write"

	redisCmd.Lock()

	redisCmd.rMap["Get"] = read
	redisCmd.rMap["ZScore"] = read
	redisCmd.rMap["ZRange"] = read
	redisCmd.rMap["ZRevRange"] = read
	redisCmd.rMap["HGet"] = read
	redisCmd.rMap["Exists"] = read

	redisCmd.rMap["Set"] = write
	redisCmd.rMap["HMSet"] = write
	redisCmd.rMap["Incr"] = write
	redisCmd.rMap["IncrBy"] = write
	redisCmd.rMap["Del"] = write
	redisCmd.rMap["ZAdd"] = write
	redisCmd.rMap["ZIncrBy"] = write
	redisCmd.rMap["LPush"] = write
	redisCmd.rMap["Expire"] = write
	redisCmd.rMap["IncrByFloat"] = write
	redisCmd.rMap["Decr"] = write
	redisCmd.rMap["DecrBy"] = write
	redisCmd.rMap["HIncrBy"] = write

	redisCmd.Unlock()
}

//SetCtx will set ctx into redis client.
func (this *XesRedis) SetCtx(ctx context.Context) {
	this.ctx = ctx
}

//GetCtx returns redis client's ctx.
func (this *XesRedis) GetCtx() context.Context {
	return this.ctx
}

//SetPipeLiner will set pipeLiners into redis client.
func (this *XesRedis) SetPipeLiner(pip map[string]*PipelineIns) {
	this.pipeLiners = pip
}

//GetPipeLiner returns redis client's pipeLiners.
func (this *XesRedis) GetPipeLiner() map[string]*PipelineIns {
	return this.pipeLiners
}

//SetPipeLiner will set handler into redis client.
func (this *XesRedis) SetHandler(handler redo.XesRedisHandler) {
	this.handler = handler
}

//GetKey return redis client's key or keyName.
func (this *XesRedis) GetKey() string {
	if this.key == "" {
		return this.keyName
	}
	return this.key
}

//GetKeyName returns redis client's keyName.
func (this *XesRedis) GetKeyName() string {
	return this.keyName
}

//SetInstanceIP will set redis server host into redis client.
func (this *XesRedis) SetInstanceIP(ip string) {
	this.instanceIP = ip
}

//GetKeyParams returns redis client's key params.
func (this *XesRedis) GetKeyParams() []interface{} {
	return this.keyParams
}

//GetClient returns a redis client. key is redis instance's name.
func GetClient(key string) *redis.Client {
	selectors := GetSelectors()
	selectorIns, ok := selectors[key]
	if !ok {
		return nil
	}
	server := selectorIns.Select()
	return GetRedisClient(server)
}

//async switch.
func (this *XesRedis) Go() *XesRedis {
	this.loggerTag = getCallFuncName()

	this.async = true
	return this
}

//var golimit *coreutil.GoLimit.
var once sync.Once
var rp *routinepool.RoutinePool

//Execute redis command asynchronously.
func (this *XesRedis) goRoutine() {
	waitGroup := commonutil.GetWaitGroup(this.ctx)
	if waitGroup != nil {
		waitGroup.Add(1)
	}
	//init RoutinePool.
	once.Do(func() {
		queueSize := confutil.GetConf("RoutinePool", "queuesize")
		waitInterval := confutil.GetConf("RoutinePool", "waitinterval")
		normalSize := confutil.GetConf("RoutinePool", "normalsize")
		maxSize := confutil.GetConf("RoutinePool", "maxsize")

		duration, err := time.ParseDuration(waitInterval)
		if err != nil {
			panic(err)
		}
		//Restricted goroutine specification.
		rp = routinepool.NewCachedRoutinePool(cast.ToInt64(queueSize), duration,
			cast.ToInt64(normalSize), cast.ToInt64(maxSize))
	})

	result := rp.ExecuteFunc(func() {
		if waitGroup != nil {
			defer waitGroup.Done()
		}
		//Executed the redis command.
		res := this.exec()
		if res.err != nil {
			//this.String() called inside this.exec().
			logger.Ex(this.ctx, this.loggerTag, this.keyName+" "+this.cmd+" xredisErr: %v", res.err)
		}
	})
	if !result {
		if waitGroup != nil {
			defer waitGroup.Done()
		}
		logger.Ex(this.ctx, this.loggerTag, this.keyName+" "+this.cmd+" routine execute fail, redis info:%s",
			this.String())
		return
	}
}

//Choosing pipeline. and setting command into the pipeline.
func (this *XesRedis) choosePipeline(cmd *redis.Cmd) RedisResult {
	if pip, ok := this.pipeLiners[this.instance]; ok {
		pip.pipeliner.Process(cmd)
		pip.used = true
	} else {
		return RedisResult{err: logger.NewError("[choosePipeline] GetClient is nil")}
	}
	return RedisResult{err: nil}
}

//setInfo will set redis command info into redis client.
func (this *XesRedis) setInfo(keyName string, keyParams []interface{}, cmd string, args []interface{}) {
	this.keyName = keyName
	this.keyParams = keyParams
	this.key = this.handler.GetKey(this)
	this.instance = this.handler.GetInstance(this)
	this.cmd = cmd
	this.args = args
}

//Printing slowLog.
func (this *XesRedis) slowLog(start time.Time) {
	threshold := GetSlowThreshold()
	if threshold <= time.Duration(0) {
		return
	}
	cost := time.Now().Sub(start)
	if cost > threshold {
		logger.Ix(this.ctx, "xredis slowLog", "redis instance %s, cmd %v, key %v, args %v, cost %v", this.instance, this.cmd, this.key, this.args, cost)
	}
	return
}

//Warning redis big key.
func (this *XesRedis) warnSize(v interface{}) {
	maxSize := GetWarnKeySize()
	if maxSize <= 0 {
		return
	}
	var size int
	switch vv := v.(type) {
	case []byte:
		size = len(vv)
	default:
		size = len(fmt.Sprint(vv))
	}
	if size > maxSize {
		logger.Ix(this.ctx, "xredis warnSize", "redis instance %s, cmd %v, key %v, args %v, val szie %d", this.instance, this.cmd, this.key, this.args, size)
	}
	return
}

//Executed the redis command.
func (this *XesRedis) exec() RedisResult {
	defer this.slowLog(time.Now())

	fields := this.args.([]interface{})
	newcmdInter := interPool.Get()
	if newcmdInter == nil {
		return RedisResult{err: logger.NewError("[exec] object got from interPool is nil")}
	}

	var ok bool
	var newcmd []interface{}
	if newcmd, ok = newcmdInter.([]interface{}); !ok {
		return RedisResult{err: logger.NewError("[exec] object got from interPool is not of []interface{} type")}
	}

	newcmd = append(newcmd, this.cmd)
	if this.key != "" {
		newcmd = append(newcmd, this.key)
	}
	for _, v := range fields {
		newcmd = append(newcmd, v)
	}

	logger.Dx(this.ctx, "xredis exec()", "redis instance %s, redis cmd %v", this.instance, newcmd)

	cmd := redis.NewCmd(newcmd...)
	if this.goPipeLine {
		return this.choosePipeline(cmd)
	}

	client := GetClient(this.instance)
	if client == nil {
		logger.Wx(this.ctx, "xredis.exec.ClientNil", "GetClient is nil, Instance : %v", this.instance)
		return RedisResult{err: logger.NewError("[exec] GetClient is nil")}
	}

	err := client.Process(cmd)
	var res RedisResult
	res.value, _ = cmd.Result()
	res.err = err
	if err != nil {
		if err == redis.Nil {
			logger.Dx(this.ctx, "xredis.exec.RedisNil", "redis cmd %v, err msg %v", newcmd, err)
		} else {
			logger.Ex(this.ctx, "xredis.exec.RedisError", "redis cmd %+v, err %v, info: %+v", newcmd, err, this.String())
		}
	}
	this.warnSize(res.value)
	args := cmd.Args()
	args = args[:0]
	interPool.Put(args)

	return res
}

//Initialize pipeLiners in the redis instance。
func (this *XesRedis) OpenPipeLine() {
	this.goPipeLine = true
	this.pipeLiners = make(map[string]*PipelineIns, len(GetSelectors()))
	for k, _ := range GetSelectors() {
		client := GetClient(k)
		if client == nil {
			logger.Ex(this.ctx, "xredis.OpenPipeLine", "GetClient fail:%v", k)
			continue
		}
		pip := &PipelineIns{client.Pipeline(), false}
		this.pipeLiners[k] = pip
	}
}

//SetPipeLineFlag will set goPipeLine flag into redis client.
func (this *XesRedis) SetPipeLineFlag() {
	this.goPipeLine = true
}

//Executing commands in the PipeLine.
func (this *XesRedis) ExecPipeLine() (ret []RedisResult, errs []error) {
	this.goPipeLine = false

	var wg sync.WaitGroup
	var lo sync.RWMutex

	for _, pipe := range this.pipeLiners {
		if !pipe.used {
			continue
		}
		wg.Add(1)
		go func(pip *PipelineIns) {
			defer wg.Done()
			cmder, err := pip.pipeliner.Exec()
			if err != nil && err != redis.Nil {
				logger.Ex(this.ctx, "xredis.ExecPipeLine", "Pipeline execute error %v", err)
				lo.Lock()
				errs = append(errs, err)
				lo.Unlock()
				return
			}
			for _, cmd := range cmder {
				var result RedisResult
				result.err = cmd.Err()
				if result.err != nil && result.err != redis.Nil {
					logger.Ex(this.ctx, "xredis.ExecPipeLine", "Pipeline execute %v command args %v, err %v", cmd.Name(), cast.ToStringSlice(cmd.Args()), cmd.Err())
				} else {
					tmpcmd, ok := cmd.(*redis.Cmd)
					if ok {
						result.value, _ = tmpcmd.Result()
					} else {
						result.err = logger.NewError("xredis.ExecPipeLine assert cmd fail")
					}
				}
				lo.Lock()
				ret = append(ret, result)
				if result.err != nil {
					errs = append(errs, result.err)
				}
				lo.Unlock()
				args := cmd.Args()
				args = args[:0]
				interPool.Put(args)
			}
		}(pipe)
	}
	wg.Wait()
	return ret, errs
}

//Sending err msg to Error kafka.
func (this *XesRedis) sendKafka(topic string, msg []byte) {
	err := kafkautil.Send2Proxy(topic, msg)
	if err != nil {
		logger.Ex(this.ctx, "SendKafkaError", topic+" %v", err)
	}
}

//Format redis execution information.
func (this *XesRedis) String() (back string) {
	rmap := make(map[string]interface{}, 10)
	keyparams := []string{}
	for _, k := range this.GetKeyParams() {
		keyparams = append(keyparams, cast.ToString(k))
	}
	args := []string{}
	if f, ok := this.args.([]interface{}); ok {
		for _, k := range f {
			args = append(args, cast.ToString(k))
		}
	}
	rmap["instance"] = this.instance
	rmap["instanceip"] = this.instanceIP
	rmap["keyname"] = this.keyName
	rmap["keyparams"] = keyparams
	rmap["cmd"] = this.cmd
	rmap["key"] = this.key
	rmap["args"] = args
	rmap["pipeliners"] = this.pipeLiners
	rmap["gopipeline"] = this.goPipeLine
	rmap["async"] = this.async
	rmap["loggertag"] = this.loggerTag
	str, err := jsutil.Json.MarshalToString(rmap)
	if err != nil {
		return str
	}
	logger.Ex(this.ctx, "xredis", "String str %v", str)
	if GetEnableKafka() {
		this.sendKafka("xes_redis_err", []byte(str))
	}
	return str
}

//getCallFuncName returns funcname.
func getCallFuncName() (funcname string) {
	pc, _, _, ok := runtime.Caller(2)

	if ok {
		funcname = runtime.FuncForPC(pc).Name()
		funcname = filepath.Ext(funcname)
		funcname = strings.TrimPrefix(funcname, ".")
	}
	return
}
//getRedisCmdMap return the value of redisCmd map.
func getRedisCmdMap(key string) (value string) {
	redisCmd.RLock()
	value = redisCmd.rMap[key]
	redisCmd.RUnlock()

	return value
}
