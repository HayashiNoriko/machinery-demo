// 发送套娃任务，用 Jaeger 观察 tracing
// 使用 SendTaskWithContext，而非 SendTask

package main

import (
	"context"
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2"
	"demo/sourcecode/machinery/v2/tasks"
)

var server *machinery.Server

func MyTask1(ctx context.Context) error {
	signature := &tasks.Signature{
		Name: "MyTask2",
	}
	server.SendTaskWithContext(ctx, signature)
	return nil
}
func MyTask2(ctx context.Context) error {
	signature := &tasks.Signature{
		Name: "MyTask3",
	}
	server.SendTaskWithContext(ctx, signature)
	return nil
}
func MyTask3(ctx context.Context) error {
	return nil
}

func main1() {
	server = myutils.MyServer()

	// 0. 初始化jaeger
	cleanup, err := myutils.SetupTracer("myjaeger")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	// 1. 注册任务
	server.RegisterTask("MyTask1", MyTask1)
	server.RegisterTask("MyTask2", MyTask2)
	server.RegisterTask("MyTask3", MyTask3)

	// 2.发布任务
	// 创建任务签名
	signature := &tasks.Signature{
		Name: "MyTask1",
	}
	server.SendTask(signature)

	// 3.创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
