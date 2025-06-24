// worker 的钩子
// 执行顺序：前置钩子 -> 主任务 -> 错误钩子(如果任务失败且 Set 了错误钩子) -> 后置钩子
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

// 定义主任务
func MyTask() error {
	myutils.Log("主任务执行")
	// return nil
	return fmt.Errorf("某个错误")
}

func main1() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MyTask", MyTask)

	// 2. 发布任务
	signature := &tasks.Signature{
		Name: "MyTask",
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
