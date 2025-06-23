// 发送普通任务
package main

import (
	myutils "demo/01-myutils"

	"demo/sourcecode/machinery/v2/tasks"
)

func main2() {
	server := myutils.MyServer()

	// 1.任务发布前的钩子函数
	server.SetPreTaskHandler(func(signature *tasks.Signature) {
		println("===pre task handler===")
	})

	// 2.发布任务
	// 创建任务签名
	signature := &tasks.Signature{
		Name: "Add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 2},
			{Type: "int64", Value: 3},
			{Type: "int64", Value: 4},
		},
	}
	server.SendTask(signature)

	// 3.创建一个 worker 去执行任务
	// worker := server.NewWorker("myworker", 10)
	// worker.Launch()
}
