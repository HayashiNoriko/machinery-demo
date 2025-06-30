// eager 模式 SendGroup
// 任务可以执行
// 但发送任务之后，要获取任务的状态和结果非常麻烦
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"

	"demo/sourcecode/machinery/v2/brokers/eager"

	"github.com/google/uuid"
)

func main6() {
	server := myutils.MyEagerServer()
	broker := server.GetBroker()

	// 1. assign worker
	worker := server.NewWorker("myworker", 10)
	mode := broker.(eager.Mode)
	mode.AssignWorker(worker)

	// 2. 构建多个 signature
	signatures := []*tasks.Signature{}
	for i := 0; i < 10; i++ {
		signature := &tasks.Signature{
			UUID: "task_" + uuid.New().String(),
			Name: "Print",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: myutils.GetLogPath(),
				},
				{
					Type:  "string",
					Value: fmt.Sprintf("我是第 %d 个任务", i),
				},
			},
		}
		signatures = append(signatures, signature)
	}

	// 3. 【问题】设置所有任务的状态为 PENDING，繁琐
	for _, signature := range signatures {
		if err := server.GetBackend().SetStatePending(signature); err != nil {
			fmt.Println("Set state pending error:", err)
		}
	}

	// 4. 创建一个 Group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 group 失败")
		return
	}

	// 5. 起一个新协程，发送任务，阻塞变非阻塞
	go server.SendGroup(group, 10)

	// 6. 获取状态？
	// 只能遍历 signatures 切片，分别从 backend 中查询状态和结果，相当麻烦

	select {}
}
