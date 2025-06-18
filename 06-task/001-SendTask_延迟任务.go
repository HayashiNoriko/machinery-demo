// 发送延迟任务
package main

import (
	myutils "demo/01-myutils"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func main1() {
	server := myutils.MyServer()

	// 1. 创建任务签名
	// 1.1. 设置任务计划执行时间（ETA），将其延迟到 5 秒后
	eta := time.Now().UTC().Add(time.Second * 5)
	// 1.2. 创建 signature 结构体
	signature := &tasks.Signature{
		Name: "Add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 2},
			{Type: "int64", Value: 3},
			{Type: "int64", Value: 4},
		},
		ETA: &eta, // 正确
		// ETA: &time.Now().UTC().Add(time.Second * 5), // 错误写法
	}

	// 2. 发送任务
	server.SendTask(signature)

	// 3. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()

	// 5 秒内可以在 redis 中看到 delayed_tasks
}
