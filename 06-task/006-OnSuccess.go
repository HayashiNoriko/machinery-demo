// 任务的成功回调
// OnSuccess 是一个切片，所以可以同时执行多个成功回调，执行的顺序不可控
// 主任务成功后，主任务的返回值（除 err 外）会作为参数，append 到 OnSuccess 中每个 signature.Args 的最后面

package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
)

// 主任务
func MainTask() (string, error) {
	myutils.Log("主任务执行")
	return "主任务返回值", nil
}

// 成功回调
// 第一个参数用来接收主任务的返回值
func OnSuc(msg, msgFromMain string) error {
	myutils.Log(fmt.Sprintf("成功回调执行, msg: %s,  msgFromMain: %s", msg, msgFromMain))
	return nil
}

func main6() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MainTask", MainTask)
	server.RegisterTask("OnSuc", OnSuc)

	// 2. 创建回调任务的签名（10 个）
	cb_signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(cb_signatures); i++ {
		cb_signatures[i] = &tasks.Signature{
			Name: "OnSuc",
			// 注意，我们只需要传入第 1 个参数
			// 第 2 个（及之后的）参数会在主任务成功后、发送本 OnSuc 前，自动传入主任务的返回值
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: fmt.Sprintf("我是 %d 号回调函数", i),
				},
			},
		}
	}

	// 3. 创建主任务签名
	signature := &tasks.Signature{
		Name: "MainTask",
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
