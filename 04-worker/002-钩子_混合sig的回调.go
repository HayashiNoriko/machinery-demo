// server 的钩子 + worker 的钩子 + sig 的回调
// 执行顺序(任务失败)：worker前置钩子 -> sig主任务 -> worker错误钩子 -> worker后置钩子 -> (从此处开始消费失败回调任务)worker前置钩子 -> sig失败回调 -> worker后置钩子
// 执行顺序(任务成功)：worker前置钩子 -> sig主任务 ->                  worker后置钩子 -> (从此处开始消费成功回调任务)worker前置钩子 -> sig成功回调- > worker后置钩子
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

// 定义主任务
func MyTask2() error {
	myutils.Log("主任务执行")
	return nil
	// return fmt.Errorf("某个错误")
}

// 成功回调
// 注意，最后几个参数必须是接收主任务成功后的返回值
// 但由于本例中的主任务 MyTask2 没有返回值，因此这里不用接收。其他时候要注意。
func OnSuc() error {
	myutils.Log("成功回调")
	return nil
}

// 失败回调
// 第一个参数必须是接收主任务失败后返回的错误信息
func OnErr(errStringFromMain string) error {
	myutils.Log("失败回调")
	return nil
}

func main2() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MyTask2", MyTask2)
	server.RegisterTask("OnSuc", OnSuc)
	server.RegisterTask("OnErr", OnErr)

	// 2. 发布任务
	signature := &tasks.Signature{
		Name: "MyTask2",
		OnSuccess: []*tasks.Signature{
			{Name: "OnSuc"},
		},
		OnError: []*tasks.Signature{
			{Name: "OnErr"},
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
