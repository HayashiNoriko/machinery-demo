// eager 模式 SendChain
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

func main5() {
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

	// 3. 设置第一个任务的状态为 PENDING
	if err := server.GetBackend().SetStatePending(signatures[0]); err != nil {
		fmt.Println("Set state pending error:", err)
	}

	// 4. 创建一个 Chain
	chain, err := tasks.NewChain(signatures...)
	if err != nil {
		fmt.Println("创建 chain 失败")
		return
	}

	// 5. 起一个新协程，发送任务，阻塞变非阻塞
	go server.SendChain(chain)

	// 6. 获取状态？
	// 没有 ChainAsyncResult，无法 Get，是个大问题
	// 只能对 signatures 切片，依次从 backend 中查询状态和结果，相当麻烦

	select {}
}
