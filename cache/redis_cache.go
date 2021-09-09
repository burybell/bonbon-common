package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Null = ""
const Nil = redis.Nil
const TypeMatchError = "type match error"

type Options struct {
	AppName string
	NameSpace string
	Addr []string
	Password string
	DB int
	MaxRetries int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
	DialTimeout time.Duration
	ReadTimeout time.Duration
	WriteTimeout time.Duration
	PoolSize int
	MinIdleConn int
	MaxConnAge time.Duration
	PoolTimeout time.Duration
	IdleTimeout time.Duration
	IdleCheckFrequency time.Duration
	readOnly bool
}

// Outcome 统一结果返回值
type Outcome struct {
	Error error
	Primordial interface{}
}

func (oc *Outcome) GetInt64() (int64,error) {
	if it,ok := oc.Primordial.(int64);ok {
		return it, nil
	} else if str,ok := oc.Primordial.(string);ok {
		value, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	}
	return 0, errors.New(TypeMatchError)
}

func (oc *Outcome) GetString() (string,error) {
	if str,ok := oc.Primordial.(string);ok {
		return str, nil
	}
	return Null, errors.New(TypeMatchError)
}

func (oc *Outcome) GetFloat64() (float64,error) {
	if flot,ok := oc.Primordial.(float64);ok {
		return flot, nil
	} else if str,ok := oc.Primordial.(string);ok {
		value, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	}
	return 0, errors.New(TypeMatchError)
}

func (oc *Outcome) GetBool() (bool,error) {
	if bol,ok := oc.Primordial.(bool);ok {
		return bol, nil
	} else if str,ok := oc.Primordial.(string);ok {
		value, err := strconv.ParseBool(str)
		if err != nil {
			return false, err
		}
		return value, nil
	}
	return false, errors.New(TypeMatchError)
}

func (oc *Outcome) Unmarshal(v interface{}) error {
	if str,ok := oc.Primordial.(string);ok {
		err := json.Unmarshal([]byte(str), v)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New(TypeMatchError)
}

func (oc *Outcome) GetMap() (map[string]string,error) {
	if mp,ok := oc.Primordial.(map[string]string);ok {
		return mp,nil
	} else if str,ok := oc.Primordial.(string);ok {
		var mp map[string]string
		err := json.Unmarshal([]byte(str), &mp)
		if err != nil {
			return nil, err
		}
		return mp, nil
	}
	return nil,errors.New(TypeMatchError)
}

func (oc *Outcome) GetArray() ([]string,error) {
	if arr,ok := oc.Primordial.([]string);ok {
		return arr,nil
	} else if str,ok := oc.Primordial.(string);ok {
		var arr []string
		err := json.Unmarshal([]byte(str), &arr)
		if err != nil {
			return nil, err
		}
		return arr, nil
	}
	return nil,errors.New(TypeMatchError)
}

type Cache interface {
	Ping() bool
	Expire(key string, duration time.Duration) *Outcome

	Get(key string) *Outcome
	GetSet(key string, value interface{}) *Outcome
	Set(key string,value interface{},expiration time.Duration) *Outcome
	SetNX(key string,value interface{},expiration time.Duration) *Outcome
	Del(keys ...string) *Outcome
	Exists(keys ...string) *Outcome

	Decr(key string) *Outcome
	DecrBy(key string, decrement int64) *Outcome
	Incr(key string) *Outcome
	IncrBy(key string, increment int64) *Outcome


	MGet(keys ...string) *Outcome
	MSet(pairs ...interface{}) *Outcome

	HGet(key string,field string) *Outcome
	HSet(key, field string, value interface{}) *Outcome
	HDel(key string, fields ...string) *Outcome
	HExists(key string,field string) *Outcome

	HGetAll(key string) *Outcome
	HKeys(key string) *Outcome
	HLen(key string) *Outcome


	HIncrBy(key string,field string,incr int64) *Outcome
	HIncrByFloat(key, field string, incr float64) *Outcome

}

var (
	redisClient *RedisClient
	once sync.Once
)

// RedisClient Redis客户端
type RedisClient struct {
	Cache
	opt *Options
	ctx context.Context
	single *redis.Client
	cluster *redis.ClusterClient
	flag bool
}

// InitRedisClient 初始化
func InitRedisClient(opt *Options) error {
	var err error
	once.Do(func() {
		if opt == nil {
			err = errors.New("options is null")
			return
		} else {
			client := new(RedisClient)
			client.opt = opt
			client.ctx = context.Background()
			if len(opt.Addr) <= 0 {
				err = errors.New("addr is null")
				return
			} else if len(opt.Addr) == 1 {
				client.single = redis.NewClient(&redis.Options{
					Network:            "tcp",
					Addr:               opt.Addr[0],
					Password:           opt.Password,
					DB:                 opt.DB,
					MaxRetries:         opt.MaxRetries,
					MinRetryBackoff:    opt.MinRetryBackoff,
					MaxRetryBackoff:    opt.MaxRetryBackoff,
					DialTimeout:        opt.DialTimeout,
					ReadTimeout:        opt.ReadTimeout,
					WriteTimeout:       opt.WriteTimeout,
					PoolSize:           opt.PoolSize,
					MinIdleConns:       opt.MinIdleConn,
					MaxConnAge:         opt.MaxConnAge,
					PoolTimeout:        opt.PoolTimeout,
					IdleTimeout:        opt.IdleTimeout,
					IdleCheckFrequency: opt.IdleCheckFrequency,
				})
				client.flag = true
			} else {
				client.cluster = redis.NewClusterClient(&redis.ClusterOptions{
					Addrs:              opt.Addr,
					MaxRedirects:       opt.MaxRetries,
					ReadOnly:           opt.readOnly,
					Password:           opt.Password,
					MaxRetries:         opt.MaxRetries,
					MinRetryBackoff:    opt.MinRetryBackoff,
					MaxRetryBackoff:    opt.MaxRetryBackoff,
					DialTimeout:        opt.DialTimeout,
					ReadTimeout:        opt.ReadTimeout,
					WriteTimeout:       opt.WriteTimeout,
					PoolSize:           opt.PoolSize,
					MinIdleConns:       opt.MinIdleConn,
					MaxConnAge:         opt.MaxConnAge,
					PoolTimeout:        opt.PoolTimeout,
					IdleTimeout:        opt.IdleTimeout,
					IdleCheckFrequency: opt.IdleCheckFrequency,
				})
				client.flag = false
			}
			redisClient = client
			return
		}

	})
	return err
}

func GetRedis() *RedisClient {
	return redisClient
}

// Runner 获取一个redis可执行对象
func (rc *RedisClient) Runner() redis.Cmdable {
	var capable interface{}
	if rc.flag {
		capable = rc.single
	} else {
		capable = rc.cluster
	}
	return capable.(redis.Cmdable)
}

// GetKey 获取统一Key
func (rc *RedisClient) GetKey(raw interface{}) string {
	return fmt.Sprintf("%s-%s-%v",
		rc.opt.AppName,rc.opt.NameSpace,raw)
}

// GetKeys 获取多个统一key
func (rc *RedisClient) GetKeys(raw ...interface{}) []string {
	keys := make([]string, 0 ,len(raw))
	for i := range raw {
		keys = append(keys, rc.GetKey(raw[i]))
	}
	return keys
}


// GetValue 自动序列化
func (rc *RedisClient) GetValue(raw interface{}) interface{} {
	switch reflect.TypeOf(raw).Kind() {
	case reflect.Struct,reflect.Slice,reflect.Map,reflect.Array,reflect.Ptr:
		marshal, err := json.Marshal(raw)
		if err != nil {
			return nil
		}
		return string(marshal)
	default:
		return raw
	}
}

// GetValues 自动序列化多个值
func (rc *RedisClient) GetValues(raw []interface{}) []interface{} {
	values := make([]interface{}, 0, len(raw))
	for i := range raw {
		values = append(values, rc.GetValue(raw[i]))
	}
	return values
}

// Drift 获取一个摆动值，防止缓存雪崩
func (rc *RedisClient) Drift(duration time.Duration) time.Duration {
	drift := rand.Int63n(60)
	return time.Duration(duration.Nanoseconds() + drift)
}

// Outcome 生成统一返回值
func (rc *RedisClient) Outcome(value interface{},err error) *Outcome {
	if err == nil {
		return &Outcome{
			Error:      nil,
			Primordial: value,
		}
	} else {
		return &Outcome{
			Error:      err,
			Primordial: nil,
		}
	}
}

// Ping 测试连接
func (rc *RedisClient) Ping() bool {
	ping := rc.Runner().Ping()
	if strings.Contains(ping.String(),"PONG") && ping.Err() == redis.Nil {
		return true
	}
	return true
}

// Expire 延期 返回bool
func (rc RedisClient) Expire(key string, duration time.Duration) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().Expire(hook, duration)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// Get 获取值 返回string
func (rc *RedisClient) Get(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().Get(hook)
	return rc.Outcome(cmd.Val(),cmd.Err())
}

// GetSet key不存在则set 返回string
func (rc *RedisClient) GetSet(key string, value interface{}) *Outcome  {
	hook := rc.GetKey(key)
	cmd := rc.Runner().GetSet(hook, value)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// Set set值 返回string
func (rc *RedisClient) Set(key string,value interface{},expiration time.Duration) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().Set(hook, rc.GetValue(value), rc.Drift(expiration))
	return rc.Outcome(cmd.Val(), cmd.Err())
}


// SetNX setNx 返回bool
func (rc *RedisClient) SetNX(key string,value interface{},expiration time.Duration) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().SetNX(hook, rc.GetValue(value), rc.Drift(expiration))
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// Del 删除key 返回int64
func (rc *RedisClient) Del(keys ...string) *Outcome {
	hooks := rc.GetKeys(keys)
	cmd := rc.Runner().Del(hooks...)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// Exists 判断存在多少个键 返回int64
func (rc *RedisClient) Exists(keys ...string) *Outcome {
	hooks := rc.GetKeys(keys)
	cmd := rc.Runner().Exists(hooks...)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// Decr 自减1 返回int64
func (rc *RedisClient) Decr(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().Decr(hook)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// DecrBy 自减多 返回int64
func (rc RedisClient) DecrBy(key string, decrement int64) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().DecrBy(hook, decrement)
	return rc.Outcome(cmd.Val(), cmd.Err())
}


// Incr 自减1  返回int64
func (rc *RedisClient) Incr(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().Incr(hook)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// IncrBy 自减多  返回int64
func (rc RedisClient) IncrBy(key string, decrement int64) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().IncrBy(hook, decrement)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// MGet 批量get 返回[]interface{}
func (rc *RedisClient) MGet(keys ...string) *Outcome {
	hooks := rc.GetKeys(keys)
	cmd := rc.Runner().MGet(hooks...)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// MSet 批量set 返回string
func (rc *RedisClient) MSet(pairs ...interface{}) *Outcome {
	kvs := make([]interface{},0, len(pairs)/2 + 1)
	for i := 0; i < len(pairs); i++ {
		kvs = append(kvs, rc.GetKey(pairs[i]))
		kvs = append(kvs, rc.GetValue(pairs[i+1]))
		i++
	}
	cmd := rc.Runner().MSet(pairs)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HGet 获取hash的值 返回string
func (rc *RedisClient) HGet(key string,field string) *Outcome  {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HGet(hook, field)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HSet 给hash设置值 返回bool
func (rc *RedisClient) HSet(key, field string, value interface{}) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HSet(hook, field, rc.GetValue(value))
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HDel 删除hash的key 返回int64
func (rc *RedisClient) HDel(key string, fields ...string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HDel(hook, fields...)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HExists 判断hash是否存在field 返回bool
func (rc *RedisClient) HExists(key string,field string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HExists(hook, field)
	return rc.Outcome(cmd.Val(), cmd.Err())
}


// HGetAll 获取hash的所有值 返回map[string]string
func (rc *RedisClient) HGetAll(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HGetAll(hook)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HKeys 获取hash的所有key 返回[]string
func (rc *RedisClient) HKeys(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HKeys(hook)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HLen 获取hash的长度 返回int64
func (rc *RedisClient) HLen(key string) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HLen(hook)
	return rc.Outcome(cmd.Val(), cmd.Err())
}


// HIncrBy 增长hash的value 返回int64
func (rc *RedisClient) HIncrBy(key string,field string,incr int64) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HIncrBy(hook,field, incr)
	return rc.Outcome(cmd.Val(), cmd.Err())
}

// HIncrByFloat 增长hash的value 返回float64
func (rc *RedisClient) HIncrByFloat(key, field string, incr float64) *Outcome {
	hook := rc.GetKey(key)
	cmd := rc.Runner().HIncrByFloat(hook,field, incr)
	return rc.Outcome(cmd.Val(), cmd.Err())
}