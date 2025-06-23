// 发送和弦任务
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main6() {
	server := myutils.MyServer()

	// 1. 创建10个任务签名
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "Print",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: "output_chord.txt",
				},
				{
					Type:  "string",
					Value: fmt.Sprintf("hello world %d", i),
				},
			},
		}
	}

	// 2. 创建 chordCallback（也是一个 signature）
	chordCallback := &tasks.Signature{
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: "output_chord.txt",
			},
			{
				Type:  "string",
				Value: fmt.Sprintf("chord call back!!!"),
			},
		},
	}

	// 3. 创建一个 Ghord
	// 3.1. 先创建 group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 Group 失败", err)
		return
	}
	// 3.2. 再创建 chord
	chord, err := tasks.NewChord(group, chordCallback)
	if err != nil {
		fmt.Println("创建 Chord 失败", err)
		return
	}

	// 3. 发送 Chord
	server.SendChord(chord, 10)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
