// 发送任务组，用 Jaeger 观察 tracing
// 发现支持得不是很好，Jaeger 里只展示了
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main4() {
	server := myutils.MyServer()
	server.RegisterTask("MyTask", MyTask)

	// 0. 初始化jaeger
	cleanup, err := myutils.SetupTracer("myjaeger666")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "MyTask",
		}
	}

	// 2. 创建一个 Group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 Group 失败", err)
		return
	}

	// 3. 创建一个 Chord
	chordCallback := &tasks.Signature{Name: "MyTask"}
	chord, err := tasks.NewChord(group, chordCallback)
	if err != nil {
		fmt.Println("创建 Chord 失败", err)
		return
	}

	// 4. 发送 Chord
	server.SendChord(chord, 10)

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
