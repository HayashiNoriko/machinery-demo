// server 的钩子 + worker 的钩子 + sig 的回调
// 执行顺序(任务失败)：server发布钩子 -> worker前置钩子 -> sig主任务 -> worker错误钩子 -> server发布钩子 -> worker后置钩子 -> (从此处开始消费失败回调任务)worker前置钩子 -> sig失败回调 -> worker后置钩子
// 执行顺序(任务成功)：server发布钩子 -> worker前置钩子 -> sig主任务 ->                  server发布钩子 -> worker后置钩子 -> (从此处开始消费成功回调任务)worker前置钩子 -> sig成功回调- > worker后置钩子

package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

// 定义主任务
func MyTask3() error {
	myutils.Log("主任务执行")
	// return nil
	return fmt.Errorf("某个错误")
}

// 成功回调
func OnSuc3() error {
	myutils.Log("成功回调")
	return nil
}

// 失败回调
func OnErr3(errStringFromMain string) error {
	myutils.Log("失败回调")
	return nil
}

func main3() {
	server := myutils.MyServer()

	// 0. 设置 server 的“发布任务前”钩子
	server.SetPreTaskHandler(func(signature *tasks.Signature) {
		myutils.Log(fmt.Sprintf("server 发布任务前置钩子执行，signature 为: %+v", signature))
	})

	// 1. 注册任务
	server.RegisterTask("MyTask3", MyTask3)
	server.RegisterTask("OnSuc3", OnSuc3)
	server.RegisterTask("OnErr3", OnErr3)

	// 2. 发布任务
	signature := &tasks.Signature{
		Name: "MyTask3",
		OnSuccess: []*tasks.Signature{
			{Name: "OnSuc3"},
		},
		OnError: []*tasks.Signature{
			{Name: "OnErr3"},
		},
	}
	server.SendTask(signature)

	// 3.创建一个 worker
	worker := server.NewWorker("myworker", 10)

	// 3.1. 设置 worker 的任务执行前置钩子
	worker.SetPreTaskHandler(func(signature *tasks.Signature) {
		myutils.Log(fmt.Sprintf("前置钩子执行，signature 为: %+v", signature))
	})

	// 3.2. 设置 worker 的任务执行后置钩子
	worker.SetPostTaskHandler(func(signature *tasks.Signature) {
		myutils.Log(fmt.Sprintf("后置钩子执行，signature 为: %+v", signature))
	})

	// 3.2. 设置 worker 的任务执行错误钩子
	worker.SetErrorHandler(func(err error) {
		myutils.Log(fmt.Sprintf("错误钩子执行，err 为: %s", err.Error()))
	})

	// 4. 启动 worker
	worker.Launch()
}
