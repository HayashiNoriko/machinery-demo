// 发送chain，用 Jaeger 观察 tracing
// 发现支持得不是很好，Jaeger 里只展示了 1 条 SendChain + 10 条 SendTask，但它们之间并没有任何联系
package main

import (
	"context"
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main3() {
	server := myutils.MyServer()
	server.RegisterTask("MyTask", MyTask)

	// 0. 初始化jaeger
	cleanup, err := myutils.SetupTracer("myjaeger333")
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

	// 2. 创建一个 Chain
	chain, err := tasks.NewChain(signatures...)
	if err != nil {
		fmt.Println("创建 Chain 失败", err)
		return
	}

	// 3. 发送 Chain
	server.SendChainWithContext(context.Background(), chain)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
