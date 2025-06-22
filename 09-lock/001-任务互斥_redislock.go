// 实现任务互斥

// machinery 中没有“对外”提供任务互斥的功能
// 但 machinery 内部有两把锁，对内使用
// 一把是 server.lock，只用在 RegisterPeriodicTask 时，防止同一任务被多个 server 发送
// 一把是 backend.redsync，用在 TriggerChord 时，防止 chord 被多个 worker 触发
// 所以，如果想实现任务互斥，只能在 taskFunc 中想办法实现
// 至于要用什么锁？可以偷懒用 machinery 内部的这两把吗？
// 很遗憾，不可以。因为它们都是私有成员变量，外部无法获取
// 但是 machinery 给我们提供了 New server.lock 的方法，我们可以利用，这是一个方法
// 或者就用 redis 的 redsync，这是另一个方法

// 本例使用 machinery 的 lock 来实现任务互斥

package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/config"
	redislock "demo/sourcecode/machinery/v2/locks/redis"
	"demo/sourcecode/machinery/v2/tasks"
)

// 全局 lock
var lock redislock.Lock

// 互斥任务
func OnlyOne() error {
	// 1. 获取锁
	// 这个任务每三秒只能执行一次
	// 注意第二个参数（锁的过期时间）是指未来某时刻的绝对时间戳，而不是一段时长，需要用 time.Now().Add()
	err := lock.Lock("OnlyOne", time.Now().Add(3*time.Second).UnixNano())
	if err != nil {
		return err
	}

	// 2. 任务逻辑
	fmt.Println("正在执行 OnlyOne 任务")

	return nil
}
func main() {
	server := myutils.MyServer()

	// 1. 初始化 lock
	lock = redislock.New(&config.Config{}, []string{"localhost:6379"}, 0, 3)

	// 2. 注册任务
	server.RegisterTask("OnlyOne", OnlyOne)

	// 3. 创建任务签名
	signature := &tasks.Signature{
		Name:       "OnlyOne",
		RetryCount: 3, // 重试 3 次
	}

	// 4. 连续发送三个 OnlyOne 任务
	server.SendTask(tasks.CopySignature(signature))
	server.SendTask(tasks.CopySignature(signature))
	server.SendTask(tasks.CopySignature(signature))

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
