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

	// 3.发布一个未注册的任务会怎样？
	// 如果 worker 检查发现设置了允许忽略，那么就忽略这个任务，不再入队（当然，taskState 会一直存在于 backend 中，状态永远为 PENDING）
	// 如果没有设置，那么任务会被 worker 取出后，又重新放回队列
	signature2 := &tasks.Signature{
		Name:                        "NotRegistered",
		Args:                        []tasks.Arg{},
		IgnoreWhenTaskNotRegistered: false, // 不允许忽略
	}
	server.SendTask(signature2)

	// 4.发布一个空任务会怎样？
	// 结论：相当于是未注册的任务，可以发布成功
	// 但如果有 worker 的话，worker 会不断地取出它又放回，因此 IgnoreWhenTaskNotRegistered 默认是 false 的
	server.SendTask(&tasks.Signature{})

	// 5.创建一个 worker 去执行任务
	// worker := server.NewWorker("myworker", 10)
	// worker.Launch()
}
