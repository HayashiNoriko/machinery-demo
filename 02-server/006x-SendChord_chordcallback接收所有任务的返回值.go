// 发送和弦任务（接收前面所有任务的返回值）
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

func main6x() {
	server := myutils.MyServer()

	// 1. 创建10个任务签名，每个任务执行 1 + 1 = 2
	signatures := make([]*tasks.Signature, 10)
	for i := 0; i < len(signatures); i++ {
		signatures[i] = &tasks.Signature{
			Name: "Add",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 1,
				},
				{
					Type:  "int64",
					Value: 1,
				},
			},
		}
	}

	// 2. 创建 chordCallback，会在那 10 个任务都执行完后执行，并且会接收 10 个任务的返回值
	chordCallback := &tasks.Signature{
		Name: "Add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 0,
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
	chordAsyncResult, err := server.SendChord(chord, 10)
	if err != nil {
		fmt.Println("发送 Chord 失败", err)
		return
	}
	go func(chordAsyncResult *result.ChordAsyncResult) {
		refs, err := chordAsyncResult.Get(1 * time.Second)
		if err != nil {
			fmt.Println("Get 失败,err: ", err.Error())
			return
		}
		fmt.Println("result:", tasks.HumanReadableResults(refs)) // result: 20
	}(chordAsyncResult)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
