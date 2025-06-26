package eager

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrEagerLockFailed = errors.New("eager lock: failed to acquire lock")
)

type Lock struct {
	retries  int           // 加锁失败时的重试次数
	interval time.Duration // 每次重试之间的等待时间

	// 用于保存所有锁的状态，内部是一个带读写锁的 map
	register struct {
		sync.RWMutex
		m map[string]int64 // key 是锁名，value 是锁的过期时间（UnixNano 时间戳）
	}
}

// 创建并返回一个新的 Lock 实例，初始化重试次数、重试间隔和锁注册表
func New() *Lock {
	return &Lock{
		retries:  3,
		interval: 5 * time.Second,
		register: struct {
			sync.RWMutex
			m map[string]int64
		}{m: make(map[string]int64)},
	}
}

// 尝试多次加锁（带重试机制）
func (e *Lock) LockWithRetries(key string, value int64) error {
	for i := 0; i <= e.retries; i++ {
		err := e.Lock(key, value)
		if err == nil {
			//成功拿到锁，返回
			return nil
		}

		time.Sleep(e.interval)
	}
	return ErrEagerLockFailed
}

// 尝试加锁一次（不重试）
func (e *Lock) Lock(key string, value int64) error {
	e.register.Lock()
	defer e.register.Unlock()
	timeout, exist := e.register.m[key]
	if !exist || time.Now().UnixNano() > timeout {
		e.register.m[key] = value
		return nil
	}
	return ErrEagerLockFailed
}
