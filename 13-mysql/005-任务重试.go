// 发送重试任务
// RETRY 状态很难观察到，因为任务一旦进入 RETRY 状态，就会立马被重新发送到 broker 中，状态立马变成 PENDING 了
// 只能在源码中，server.SendTask 中，修改任务状态为 PENDING 前面，sleep 几秒钟，才可以清晰地观察到 RETRY 状态
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"os"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func MyTask() error {
	fmt.Println("执行任务中...")
	time.Sleep(5 * time.Second)
	// return fmt.Errorf("some error")
	return tasks.NewErrRetryTaskLater("retry error", 3*time.Second)
}

func main5() {
	server := myutils.MyMySQLServer()

	// 注册任务
	server.RegisterTask("MyTask", MyTask)

	// worker:
	if len(os.Args) == 2 && os.Args[1] == "worker" {
		// 创建一个 worker 去执行任务
		worker := server.NewWorker("myworker", 10)
		worker.Launch()
	}

	// sender:
	// 发布任务
	signature := &tasks.Signature{
		Name:         "MyTask",
		RetryCount:   3,
		RetryTimeout: 1,
	}
	asyncResult, err := server.SendTask(signature)
	if err != nil {
		fmt.Println("任务发送失败: ", err.Error())
		return
	}

	// GetState
	for {
		fmt.Println("任务状态", asyncResult.GetState().State)
		time.Sleep(1 * time.Second)
	}
}
