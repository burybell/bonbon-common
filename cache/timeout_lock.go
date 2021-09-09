package cache

import (
	"time"
)

type TimeoutLocker struct {
	TimeOut time.Duration
	ReUse bool
}

// NewTimeOutLock 实例化一个超时锁
func NewTimeOutLock(timeout time.Duration, reuse bool) *TimeoutLocker {
	return &TimeoutLocker{
		TimeOut: timeout,
		ReUse:         reuse,
	}
}

func (tl *TimeoutLocker) Lock(name string, topic string) bool {
	redis := GetRedis()
	value := redis.Get(name)
	if value.Error != nil {
		if value.Error == Nil {
			nx := redis.SetNX(name, topic, tl.TimeOut)
			if bol, err := nx.GetBool(); bol && err == nil {
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	} else {
		if str, err := value.GetString(); err == nil && str != topic {
			return false
		} else {
			if !tl.ReUse {
				return false
			} else {
				expire := redis.Expire(name, tl.TimeOut)
				if bol, err := expire.GetBool();bol && err == nil {
					after := redis.Get(name)
					if after.Error == nil {
						if str, err := after.GetString(); err == nil && str == topic {
							return true
						}
					}
					return false
				} else {
					nx := redis.SetNX(name, topic, tl.TimeOut)
					if bol, err := nx.GetBool(); bol && err == nil {
						return true
					} else {
						return false
					}
				}
			}
		}
	}
}