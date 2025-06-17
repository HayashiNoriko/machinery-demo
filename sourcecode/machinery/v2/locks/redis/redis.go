package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"demo/sourcecode/machinery/v2/config"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type Lock struct {
	rclient  redis.UniversalClient
	retries  int
	interval time.Duration
}

func New(cnf *config.Config, addrs []string, db, retries int) Lock {
	if retries <= 0 {
		return Lock{}
	}
	lock := Lock{retries: retries}

	var password string

	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1] // addr is the last one without @
	}

	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}

	if cnf.Redis != nil && cnf.Redis.SentinelPassword != "" {
		ropt.SentinelPassword = cnf.Redis.SentinelPassword
	}

	if cnf.Redis != nil && cnf.Redis.ClusterEnabled {
		lock.rclient = redis.NewClusterClient(ropt.Cluster())
	} else {
		lock.rclient = redis.NewUniversalClient(ropt)
	}

	return lock
}

func (r Lock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, unixTsToExpireNs)
		if err == nil {
			// 成功拿到锁，返回
			return nil
		}

		time.Sleep(r.interval)
	}
	return ErrRedisLockFailed
}

// key：锁的唯一标识
// unixTsToExpireNs：锁过期时间（纳秒）
func (r Lock) Lock(key string, unixTsToExpireNs int64) error {
	now := time.Now().UnixNano()
	expiration := time.Duration(unixTsToExpireNs + 1 - now)
	ctx := context.Background()

	// 1. 尝试原子加锁
	// SetNX 是 Redis 的“set if not exists”命令，只有当 key 不存在时才会设置，并带上过期时间
	success, err := r.rclient.SetNX(ctx, key, unixTsToExpireNs, expiration).Result()
	if err != nil {
		return err
	}

	if !success {
		// 2. 锁已存在，判断是否超时
		// SetNX 失败，success==false，说明锁已被其他客户端持有

		// 读取当前锁的过期时间（timeout）
		v, err := r.rclient.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		// 判断当前时间（now）是否已经超过这个过期时间（timeout），即锁是否已超时
		if timeout != 0 && now > int64(timeout) {
			// 3. 锁超时，尝试抢锁
			// 锁已超时，使用 GetSet 原子操作尝试将锁的过期时间设置为新的时间戳，并获取旧的时间戳
			newTimeout, err := r.rclient.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			// 再次判断当前时间是否大于旧的时间戳（防止并发情况下其他客户端已抢到锁）
			if now > int64(curTimeout) {
				// 如果是，则说明当前客户端成功抢到锁，并设置新的过期时间
				// success to acquire lock with get set
				// set the expiration of redis key
				r.rclient.Expire(ctx, key, expiration)
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
