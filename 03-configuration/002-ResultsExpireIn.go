package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

func main2() {
	server := myutils.MyServer()

	server.GetConfig().ResultsExpireIn = 10 // 设置任务结果 10 秒过期。。。

	server.SendTask(&tasks.Signature{
		Name: "Add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 2,
			},
		},
	})

	worker := server.NewWorker("worker1", 10)
	worker.Launch()
}
