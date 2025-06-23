// 注册周期任务
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

func main7() {
	server := myutils.MyServer()

	// 1. 注册周期任务
	signature := &tasks.Signature{
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: "output_periodic_task",
			},
			{
				Type:  "string",
				Value: "hello",
			},
		},
	}

	// 2. 发送周期任务，每分钟执行一次
	server.RegisterPeriodicTask("*/1 * * * *", "fxy", signature)

	// 3.创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
