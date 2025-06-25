// 发送任务组，用 Jaeger 观察 tracing
// 使用 SendGroup 即可观察到 10 个 MyTask 并行，位于 SendGroup 下
package main

import (
	"context"
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func MyTask(ctx context.Context) error {
	return nil
}

func main2() {
	server := myutils.MyServer()
	server.RegisterTask("MyTask", MyTask)

	// 0. 初始化jaeger
	cleanup, err := myutils.SetupTracer("myjaeger")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "MyTask",
		}
	}

	// 2. 创建一个 Group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 Group 失败", err)
		return
	}

	// 3. 发送 Group
	server.SendGroup(group, 10)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
