// 发送重试任务
// 分为有限重试和无限重试
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func main2() {
	server := myutils.MyServer()

	// 注册这个重试的任务
	server.RegisterTask("Retry", RetryFunc)

	// 1. 创建任务签名
	signature := &tasks.Signature{
		Name:         "Retry",
		Args:         []tasks.Arg{},
		RetryCount:   3, // 设置重试次数
		RetryTimeout: 3, // 设置重试时间间隔（初始值，后续会根据 fib 数列递增）
	}

	// 2. 发送任务
	server.SendTask(signature)

	// 3. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}

func RetryFunc() error {
	fmt.Println("任务开始。。。")
	time.Sleep(3 * time.Second) // 模拟任务耗时
	fmt.Println("任务结束。。。")
	return tasks.NewErrRetryTaskLater("custom retry error", 5*time.Second) // 返回这个 error，会触发无限重试（可以自定义重试间隔为 5 秒）
	// return fmt.Errorf("some error")                                        // 返回其他 error，会触发有限重试（前提是要有重试次数哦）

}
