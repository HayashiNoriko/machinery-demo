// 任务的失败回调/错误回调
// OnSuccess 是一个切片，所以可以同时执行多个成功回调，执行的顺序不可控
// 主任务失败后，主任务返回的 err 的信息(string)会作为参数，append 到 OnSuccess 中每个 signature.Args 的最前面

package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"errors"
	"fmt"
)

// 主任务
func MainTask2() error {
	myutils.Log("主任务执行")
	return errors.New("某个错误")
}

// 失败回调/错误回调
// 第一个参数用来接收主任务返回来的错误
func OnErr(errStringFromMain string, msg string) error {
	myutils.Log(fmt.Sprintf("失败回调执行, errStringFromMain: %s,  msg: %s", errStringFromMain, msg))
	return nil
}

func main7() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MainTask2", MainTask2)
	server.RegisterTask("OnErr", OnErr)

	// 2. 创建回调任务的签名（10 个）
	cb_signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(cb_signatures); i++ {
		cb_signatures[i] = &tasks.Signature{
			Name: "OnErr",
			// 注意，我们只需要传入第 2 个参数
			// 第 1 个参数会在主任务失败后、发送本 OnErr 前，自动传入主任务返回的错误的描述（注意，已经帮我们提取了 errors 中的信息 string，不是 error 本身）
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
		Name: "MainTask2",
		// 设置失败回调
		// 注意，回调函数的执行顺序不一定和切片中的顺序完全一致
		OnError: cb_signatures,
	}

	// 4. 发送任务
	server.SendTask(signature)

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
