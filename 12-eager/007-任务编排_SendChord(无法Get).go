// eager 模式 SendChord
// 任务可以执行
// 但发送任务之后，要获取任务的状态和结果非常麻烦
// 而且还会导致 chord 被多次发送，因为多个子任务同时执行成功，而 eager 模式没有对 TriggerChord 方法上锁，且永远返回 true
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"

	"demo/sourcecode/machinery/v2/brokers/eager"

	"github.com/google/uuid"
)

func main7() {
	server := myutils.MyEagerServer()
	broker := server.GetBroker()

	// 1. assign worker
	worker := server.NewWorker("myworker", 10)
	mode := broker.(eager.Mode)
	mode.AssignWorker(worker)

	// 2. 构建多个 signature，以及 chordCallback 的 signature
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
	chordCallback := &tasks.Signature{
		// UUID: "task_" + uuid.New().String(), // chordcallback 不用设置 uuid，它会在最后执行
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "我是 chordcallback",
			},
		},
	}

	// 3. 【问题】设置所有任务的状态为 PENDING，繁琐
	for _, signature := range signatures {
		if err := server.GetBackend().SetStatePending(signature); err != nil {
			fmt.Println("Set state pending error:", err)
		}
	}

	// 4. 创建一个 Chord
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 group 失败")
		return
	}

	chord, err := tasks.NewChord(group, chordCallback)
	if err != nil {
		fmt.Println("创建 chord 失败")
		return
	}

	// 5. 起一个新协程，发送任务，阻塞变非阻塞
	go server.SendChord(chord, 10)

	// 6. 获取状态？
	// 没有 chordAsyncResult，无法 Get，是个大问题
	// 只能对 signatures 切片，依次从 backend 中查询状态和结果，相当麻烦

	select {}
}
