package core

import (
	"context"
)

type XesRedisBase interface {
	//get context
	GetCtx() context.Context
	//set redis client IP
	SetInstanceIP(string)
	//get redis key Name
	GetKeyName() string
	//get redis key's params
	GetKeyParams() []interface{}
	//get redis key
	GetKey() string
}
