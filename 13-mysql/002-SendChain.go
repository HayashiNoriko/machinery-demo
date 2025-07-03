// 发送链式任务
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

func main2() {
	server := myutils.MyMySQLServer()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
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
		}
	}

	// 2. 创建一个 Chain
	chain, err := tasks.NewChain(signatures...)
	if err != nil {
		fmt.Println("创建 Chain 失败", err)
		return
	}

	// 3. 发送 Chain
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		fmt.Println("发送 Chain 失败")
		return
	}

	go func(chainAsyncResult *result.ChainAsyncResult) {
		refs, err := chainAsyncResult.Get(1 * time.Second)
		if err != nil {
			fmt.Println("Get 失败,err: ", err.Error())
			return
		}
		fmt.Println("result:", tasks.HumanReadableResults(refs))
	}(chainAsyncResult)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
