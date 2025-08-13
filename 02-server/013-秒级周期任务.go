// 改了源码，让 cron 支持秒级
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
)

func main13() {
	server := myutils.MyServer()

	// 1. 注册周期任务
	signature := &tasks.Signature{
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "hello",
			},
		},
	}

	// 2. 发送周期任务，每秒执行一次
	err := server.RegisterPeriodicTask("*/1 * * * * *", "fxy", signature)
	if err != nil {
		fmt.Println(err)
	}

	// 3.创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
