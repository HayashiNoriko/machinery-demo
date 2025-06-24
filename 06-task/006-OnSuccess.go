// 任务的成功回调
// 成功回调函数，可以有多个，执行顺序不一定

package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
)

func main() {
	server := myutils.MyServer()

	// 1. 创建回调任务的签名（10 个）
	cb_signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(cb_signatures); i++ {
		cb_signatures[i] = &tasks.Signature{
			Name: "Print",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: myutils.GetLogPath(),
				},
				{
					Type:  "string",
					Value: fmt.Sprintf("Onsuccess %d", i),
				},
			},
		}
	}

	// 2. 创建主任务签名
	signature := &tasks.Signature{
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "Hello world!",
			},
		},
		// 设置成功回调
		// 注意，回调函数的执行顺序不一定和切片中的顺序完全一致
		OnSuccess: cb_signatures,
	}

	// 4. 发送任务
	server.SendTask(signature)

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
