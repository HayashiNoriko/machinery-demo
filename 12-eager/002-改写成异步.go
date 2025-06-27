// 把上一例中的代码改写一下，变成异步
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/brokers/eager"

	"github.com/google/uuid"
)

func main2() {
	server := myutils.MyEagerServer()
	broker := server.GetBroker()

	// 0. 注册任务
	server.RegisterTask("MyTask", MyTask)

	// 1. assign worker
	worker := server.NewWorker("myworker", 10)
	mode := broker.(eager.Mode)
	mode.AssignWorker(worker)

	// 2. 发送任务，自己生成 UUID，后续凭借此 UUID 获取任务状态
	signature := &tasks.Signature{
		UUID: "task_" + uuid.New().String(),
		Name: "MyTask",
	}

	// 3. 设置任务状态为 PENDING
	// 注意，虽然 SendTask 中也会设置，但我们提前设置，确保后续 GetState 时，任务状态必定存在
	if err := server.GetBackend().SetStatePending(signature); err != nil {
		fmt.Println("Set state pending error:", err)
	}

	// 4. 起一个新协程，发送任务，阻塞变非阻塞
	go server.SendTask(signature)

	// 5. 根据自己生成的 UUID 获取任务状态
	for {
		taskState, err := server.GetBackend().GetState(signature.UUID)
		if err != nil {
			fmt.Println("获取任务状态失败！")
			return
		}

		fmt.Println("任务状态：", taskState.State)
		if taskState.IsCompleted() {
			break
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Println("程序退出")
}
