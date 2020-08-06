package common

import (
	"crypto/md5"
	"fmt"
	"sort"
	"github.com/tal-tech/xtools/confutil"
)

const KETAMA_POINTS_PER_SERVER uint32 = 160
const KETAMA_POINTS_PER_HASH uint32 = 4

type Ketama struct {
	SortedHV      []uint32
	HashMapIndex  map[uint32]int
	HashMapServer map[uint32]string
}

var KMap map[string]*Ketama

//Reading config file to loading redis hosts...
func init() {
	KMap = make(map[string]*Ketama, 0)
	hashmap := confutil.GetConfArrayMap("TwemProxyHash")
	for k, list := range hashmap {
		KMap[k] = NewKetama(list)
	}
}

//GetKetamaIndex returns Redis instance's index specified by keyNamein the config of tweproxyhash and redis server's ketama_hash.
func GetKetamaIndex(name string, hash uint32) int {
	ins, ok := KMap[name]
	if !ok {
		return 0
	}
	i := sort.Search(len(ins.SortedHV), func(i int) bool { return ins.SortedHV[i] >= hash })
	if i >= len(ins.SortedHV) {
		i = 0
	}
	return ins.HashMapIndex[ins.SortedHV[i]]
}

//GetKetamaServer returns redis server host specified by keyName in the config of tweproxyhash and redis server's ketama_hash.
func GetKetamaServer(name string, hash uint32) string {
	ins, ok := KMap[name]
	if !ok {
		return ""
	}
	i := sort.Search(len(ins.SortedHV), func(i int) bool { return ins.SortedHV[i] >= hash })
	if i >= len(ins.SortedHV) {
		i = 0
	}
	return ins.HashMapServer[ins.SortedHV[i]]
}

// NewKetama returns a Ketama struct specified by redis server hosts.
func NewKetama(servers []string) *Ketama {
	ins := new(Ketama)
	ins.SortedHV = make([]uint32, 0)
	ins.HashMapIndex = make(map[uint32]int, 0)
	ins.HashMapServer = make(map[uint32]string, 0)
	var j, k uint32
	for i, server := range servers {
		for j = 0; j < KETAMA_POINTS_PER_SERVER/KETAMA_POINTS_PER_HASH; j++ {
			for k = 0; k < KETAMA_POINTS_PER_HASH; k++ {
				host := fmt.Sprintf("%s-%d", server, j)
				value := ketama_hash(host, k)
				ins.HashMapIndex[value] = i
				ins.HashMapServer[value] = server
			}
		}
	}
	for k, _ := range ins.HashMapServer {
		ins.SortedHV = append(ins.SortedHV, k)
	}
	sort.Slice(ins.SortedHV, func(i, j int) bool { return ins.SortedHV[i] < ins.SortedHV[j] })
	return ins
}

// ketama_ Hash returns the hash value specified by redis server host and array's subscript.
func ketama_hash(host string, alignment uint32) uint32 {
	h := md5.New()
	h.Write([]byte(host))
	results := h.Sum(nil)
	return (uint32(results[3+alignment*4]&0xFF)<<24 | uint32(results[2+alignment*4]&0xFF)<<16 | uint32(results[1+alignment*4]&0xFF)<<8 | uint32(results[0+alignment*4]&0xFF))
}
