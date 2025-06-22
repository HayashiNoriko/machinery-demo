// 发送链式任务
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main() {
	server := myutils.MyServer()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "Print",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: fmt.Sprintf("hello world %d", i),
				},
			},
		}
	}

	// 2. 创建一个 Chain
	chain, err := tasks.NewChain(signatures...)
	if err != nil {
		fmt.Println("创建 Chain 失败", err)
		return
	}

	// 3. 发送 Chain
	server.SendChain(chain)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
