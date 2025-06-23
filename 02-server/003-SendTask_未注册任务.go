// 发送一个未注册的任务
package main

import (
	myutils "demo/01-myutils"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

func main3() {
	server := myutils.MyServer()

	// 1.发布一个未注册的任务
	// 结论请看企微笔记
	signature := &tasks.Signature{
		Name:                        "NotRegistered",
		IgnoreWhenTaskNotRegistered: true, // 修改这行观察结果
	}
	asyncResult, err := server.SendTask(signature)
	go func(asyncResult *result.AsyncResult, err error) {
		if err != nil {
			myutils.Output(err.Error())
		}
		for {
			asyncResult.GetWithTimeout(2*time.Second, 500*time.Millisecond)
			myutils.Output("asyncResult.GetWithTimeout中、、、")
		}

	}(asyncResult, err)

	// 2. 发布一个空任务会怎样？
	// 结论：可以发布成功。相当于是未注册的任务，IgnoreWhenTaskNotRegistered 默认是 false 的
	// server.SendTask(&tasks.Signature{})

	// 3. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
