package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
)

func main10() {
	server := myutils.MyServer()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "Print",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: myutils.GetLogPath(),
				},
				{
					Type:  "string",
					Value: fmt.Sprintf("hello world %d", i),
				},
			},
		}
	}

	// 不用创建 chain！

	// 2. 发送周期 Chain
	err := server.RegisterPeriodicChain("*/1 * * * *", "register_chain", signatures...)
	if err != nil {
		fmt.Println("发送周期 Chain 失败", err)
		return
	}

	// 3. 新建 worker 来执行任务
	worker := server.NewWorker("worker1", 10)
	worker.Launch()
}
