// 发送普通任务
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

func main1() {
	server := myutils.MyMySQLServer()

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
	asyncResult, err := server.SendTask(signature)
	if err != nil {
		fmt.Println("任务发送失败: ", err.Error())
		return
	}

	// 2.1. 测试 GetState
	// go func(asyncResult *result.AsyncResult) {
	// 	for {
	// 		fmt.Println("任务状态", asyncResult.GetState().State)
	// 		if asyncResult.GetState().IsCompleted() {
	// 			break
	// 		}
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }(asyncResult)

	// 2.2. 测试 Get
	go func(asyncResult *result.AsyncResult) {
		refs, err := asyncResult.Get(1 * time.Second)
		if err != nil {
			fmt.Println("Get 失败,err: ", err.Error())
			return
		}
		fmt.Println(tasks.HumanReadableResults(refs))
	}(asyncResult)

	// 3.创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
