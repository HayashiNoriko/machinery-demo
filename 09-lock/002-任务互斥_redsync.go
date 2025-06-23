// 实现任务互斥

// 本例使用 redsync 来实现任务互斥

// 比上一例的 lock 更灵活，可以手动 Unlock

package main

import (
	myutils "demo/01-myutils"
	"os"
	"time"

	"demo/sourcecode/machinery/v2/tasks"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

var rs *redsync.Redsync

// 互斥任务
func OnlyOne2() error {
	// 1. 获取锁
	// 分布式锁不能设置为永不过期。但我们可以把过期时间设置得非常大，例如 1 天
	mutex := rs.NewMutex("my_lock", redsync.WithExpiry(24*time.Hour))
	if err := mutex.Lock(); err != nil {
		// 获取锁失败，可以直接返回错误（不重试了），也可以重试
		// 重试的话分两种，一种是自动重试（有限重试），一种是手动重试（无限重试），这个在 06-002 有讲
		// 这里我们就用无限重试吧
		// 重试时间设置为 1 纳秒，表示立即重试
		return tasks.NewErrRetryTaskLater("custom retry error", 1*time.Nanosecond)
	}

	// 手动解锁
	defer mutex.Unlock()

	// 2. 任务逻辑
	time.Sleep(3 * time.Second) // 模拟任务耗时
	// 将结果写入文件
	file, _ := os.OpenFile("onlyone2_output", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	defer file.Close()

	file.WriteString("OnlyOne2 " + time.Now().Format("2006-01-02 15:04:05") + "\n")

	return nil
}

func main2() {
	server := myutils.MyServer()

	// 1. 创建 rclient
	rclient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pool := redsyncgoredis.NewPool(rclient)

	// 2. 初始化 Redsync
	rs = redsync.New(pool)

	// 3. 注册任务
	server.RegisterTask("OnlyOne2", OnlyOne2)

	// 4. 创建任务签名
	signature := &tasks.Signature{
		Name: "OnlyOne2",
	}

	// 5. 连续发送五个 OnlyOne 任务
	server.SendTask(tasks.CopySignature(signature))
	server.SendTask(tasks.CopySignature(signature))
	server.SendTask(tasks.CopySignature(signature))

	// 6. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
