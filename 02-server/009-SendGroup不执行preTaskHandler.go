// 发送 Group，不会对每个任务执行 preTaskHandler
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main9() {
	server := myutils.MyServer()

	// 1. 设置 server 的发布任务前回调
	server.SetPreTaskHandler(func(signature *tasks.Signature) {
		myutils.Log("发布任务前回调")
	})

	// 2. 创建10个任务签名
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

	// 3. 创建一个 Group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 Group 失败", err)
		return
	}

	// 4. 发送 Group
	server.SendGroup(group, 10)

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
