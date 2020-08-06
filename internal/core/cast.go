package core

import (
	"errors"
	"time"
	"github.com/go-redis/redis"
	"github.com/spf13/cast"
)

//ToStringStringMap will force the result into map[string]string type.
func (res RedisResult) ToStringStringMap() (map[string]string, error) {
	if valueArr, ok := res.value.([]interface{}); ok {
		back := make(map[string]string)
		for i := 0; i < len(valueArr); i = i + 2 {
			back[cast.ToString(valueArr[i])] = cast.ToString(valueArr[i+1])
		}
		return back, res.err
	}
	return cast.ToStringMapString(res.value), res.err
}

//ToStringIntMap will force the result into map[string]int64 type.
func (res RedisResult) ToStringIntMap() (map[string]int64, error) {
	if re, ok := res.value.(map[string]int64); !ok {
		return nil, errors.New("类型转换无效")
	} else {
		return re, res.err
	}
}

//ToStringStructMap will force the result into map[string]struct{} type.
func (res RedisResult) ToStringStructMap() (map[string]struct{}, error) {
	if re, ok := res.value.(map[string]struct{}); !ok {
		return nil, errors.New("类型转换无效")
	} else {
		return re, res.err
	}
}

//ToZSlice will force the result into []redis.Z type.
func (res RedisResult) ToZSlice() ([]redis.Z, error) {
	slice := cast.ToSlice(res.value)
	var result []redis.Z
	for i := 0; i < len(slice); i = i + 2 {
		result = append(result, redis.Z{cast.ToFloat64(slice[i+1]), slice[i]})
	}
	return result, res.err

}

func (res RedisResult) ToSlice() (back []interface{}, err error) {
	return cast.ToSlice(res.value), res.err
}

func (res RedisResult) ToInt64() (back int64, err error) {
	return cast.ToInt64(res.value), res.err
}

func (res RedisResult) ToTime() (back time.Time, err error) {
	return cast.ToTime(res.value), res.err
}

func (res RedisResult) ToBool() (back bool, err error) {
	return cast.ToBool(cast.ToInt(res.value)), res.err
}

func (res RedisResult) ToString() (back string, err error) {
	return cast.ToString(res.value), res.err
}

func (res RedisResult) ToFloat64() (back float64, err error) {
	return cast.ToFloat64(res.value), res.err
}

func (res RedisResult) ToStringSlice() (back []string, err error) {
	return cast.ToStringSlice(res.value), res.err
}

func (res RedisResult) ToBoolSlice() (back []bool, err error) {
	return cast.ToBoolSlice(res.value), res.err
}

func (res RedisResult) Error() error {
	return res.err
}
