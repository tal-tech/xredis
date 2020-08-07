package core

import (
	"strconv"
	"sync"
	"time"
	"github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
	"github.com/go-redis/redis"
	"github.com/spf13/cast"
)

var (
	RedisConfMap map[string]string

	enableKafka bool

	slowThreshold time.Duration

	warnKeySize int

	enableStub bool
)

func newRedisClient(server string, option redis.Options) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:        server,
		Password:    option.Password, // no password set
		DB:          option.DB,       // use default DB
		PoolSize:    option.PoolSize,
		IdleTimeout: option.IdleTimeout,
		ReadTimeout: option.ReadTimeout,
		MaxRetries:  option.MaxRetries,
	})
	_, err := client.Ping().Result()
	if err != nil {
		logger.E("Redis Error", "Get connect error:%v,server:%v", err, server)
	}
	return client
}

// Factory Method
var instance = struct {
	sync.RWMutex
	redisInstances map[string]*redis.Client
}{redisInstances: make(map[string]*redis.Client, 0)}

var selectors map[string]*Selector
var lock sync.RWMutex

func init() {
	selectors = make(map[string]*Selector)
	initRedis()
}

//GetRedisConf returns redis server config.
func GetRedisConf() map[string]string {
	return RedisConfMap
}

//Initialize redis instance by weight configuration.
func initRedis() {
	enableKafka = cast.ToBool(confutil.GetConf("", "enableKafka"))
	enableStub = cast.ToBool(confutil.GetConf("Stub", "enable"))
	slowTime := cast.ToInt(confutil.GetConfDefault("RedisLog", "slowTime", "300"))
	slowThreshold = time.Duration(slowTime) * time.Millisecond
	warnKeySize = cast.ToInt(confutil.GetConfDefault("RedisLog", "warnKeySize", "500"))
	RedisConfMap = confutil.GetConfStringMap("Redis")
	confMapList := confutil.GetConfArrayMap("Redis")
	for k, v := range confMapList {
		options := redis.Options{
			Password:    "",
			DB:          0,
			PoolSize:    100,
			IdleTimeout: 240 * time.Second,
			ReadTimeout: 5 * time.Second,
			MaxRetries:  0,
		}
		if p := confutil.GetConf("RedisConfig", k+".poolsize"); p != "" {
			if rt, err := strconv.Atoi(p); err == nil {
				options.PoolSize = rt
			} else {
				logger.E("xredis Error", "poolsize strconv.Atoi(%+v) error:%v", p, err)
			}
		}
		if p := confutil.GetConf("RedisConfig", k+".db"); p != "" {
			if rt, err := strconv.Atoi(p); err == nil {
				options.DB = rt
			} else {
				logger.E("xredis Error", ".db strconv.Atoi(%+v) error:%v", p, err)
			}
		}
		if p := confutil.GetConf("RedisConfig", k+".idletimeout"); p != "" {
			if rt, err := strconv.Atoi(p); err == nil {
				options.IdleTimeout = time.Second * time.Duration(rt)
			} else {
				logger.E("xredis Error", ".idletimeout strconv.Atoi(%+v) error:%v", p, err)
			}
		}
		if p := confutil.GetConf("RedisConfig", k+".readtimeout"); p != "" {
			if rt, err := strconv.Atoi(p); err == nil {
				options.ReadTimeout = time.Second * time.Duration(rt)
			} else {
				logger.E("xredis Error", ".readtimeout strconv.Atoi(%+v) error:%v", p, err)
			}
		}
		if p := confutil.GetConf("RedisConfig", k+".password"); p != "" {
			options.Password = p
		}
		if p := confutil.GetConf("RedisConfig", k+".maxretries"); p != "" {
			if rt, err := strconv.Atoi(p); err == nil {
				options.MaxRetries = rt
			} else {
				logger.E("xredis Error", ".maxretries strconv.Atoi(%+v) error:%v", p, err)
			}
		}
		selectorIns := new(Selector)
		for _, s := range v {
			instance.redisInstances[s] = newRedisClient(s, options)
			selectorIns.Servers = append(selectorIns.Servers, &Weighted{Server: s, Weight: 1, EffectiveWeight: 1})
		}
		selectors[k] = selectorIns
	}
}

//Add redis instance by options.
func AddRedis(key, addr string, options redis.Options) {
	instance.Lock()
	instance.redisInstances[addr] = newRedisClient(addr, options)
	instance.Unlock()
	selectorIns := new(Selector)
	selectorIns.Servers = append(selectorIns.Servers, &Weighted{Server: addr, Weight: 1, EffectiveWeight: 1})
	lock.Lock()
	selectors[key] = selectorIns
	lock.Unlock()
}

//GetRedisClient return redis client specified by redis server host.
func GetRedisClient(server string) *redis.Client {
	instance.RLock()
	ins, ok := instance.redisInstances[server]
	instance.RUnlock()
	if ok && ins != nil {
		return ins
	}
	return nil
}

//SetInstance can set redis client into redisInstances Map.
func SetInstance(server string, client *redis.Client) {
	instance.Lock()
	instance.redisInstances[server] = client
	instance.Unlock()
}

//GetSlowThreshold returns the value of slowThreshold.
func GetSlowThreshold() time.Duration {
	return slowThreshold
}

//GetEnableKafka returns the value of enableKafka.
func GetEnableKafka() bool {
	return enableKafka
}

//GetEnableStub returns the value of enableStub.
func GetEnableStub() bool {
	return enableStub
}

//GetWarnKeySize returns the value of warnKeySize.
func GetWarnKeySize() int {
	return warnKeySize
}

//GetSelectors returns redis clients map with Weight configuration.
func GetSelectors() map[string]*Selector {
	selectorMap := make(map[string]*Selector, len(selectors))
	selectorMap = selectors
	return selectorMap
}

type Selector struct {
	Servers []*Weighted
}

//Select a redis server address according to weight configuration.
func (s *Selector) Select() string {
	ss := s.Servers
	if len(ss) == 0 {
		return ""
	}
	w := nextWeighted(ss)
	if w == nil {
		return ""
	}
	return w.Server
}

//redis server Weighted struct.
type Weighted struct {
	Server          string
	Weight          int
	CurrentWeight   int
	EffectiveWeight int
}

//Weighting algorithm Refer to https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35
func nextWeighted(servers []*Weighted) (best *Weighted) {
	total := 0

	for i := 0; i < len(servers); i++ {
		w := servers[i]

		if w == nil {
			continue
		}

		w.CurrentWeight += w.EffectiveWeight
		total += w.EffectiveWeight
		if w.EffectiveWeight < w.Weight {
			w.EffectiveWeight++
		}

		if best == nil || w.CurrentWeight > best.CurrentWeight {
			best = w
		}

	}

	if best == nil {
		return nil
	}

	best.CurrentWeight -= total
	return best
}
