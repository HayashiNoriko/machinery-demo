// copy signature
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func main3() {
	server := myutils.MyServer()

	// 1. 创建任务签名
	// 1.2. 创建 signature 结构体
	signature := &tasks.Signature{
		Name: "Add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 2},
			{Type: "int64", Value: 3},
			{Type: "int64", Value: 4},
		},
	}

	// 2. 复制签名（发送任务前）
	copySignature1 := tasks.CopySignature(signature)

	// 3. 发送任务
	server.SendTask(signature)

	// 3. 确保任务已经发送
	time.Sleep(1 * time.Second)

	// 3. 复制签名（发送任务后）
	copySignature2 := tasks.CopySignature(signature)

	// 4. 打印三个签名
	fmt.Println(copySignature1)
	fmt.Println(signature)
	fmt.Println(copySignature2)

	// 结论：CopySignature 会把原 signature 一丝不动地复制，包括 UUID 等
	// 使用的时候要注意，在发送任务前复制

	// 5. 创建一个 worker 去执行任务
	// worker := server.NewWorker("myworker", 10)
	// worker.Launch()
}
