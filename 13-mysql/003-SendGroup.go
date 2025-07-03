// 发送group
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

func main3() {
	server := myutils.MyMySQLServer()

	// 1. 创建10个任务签名
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
					Value: 2,
				},
			},
		}
	}

	// 2. 创建一个 Group
	group, err := tasks.NewGroup(signatures...)
	if err != nil {
		fmt.Println("创建 Group 失败", err)
		return
	}

	// 3. 发送 Group
	asyncResults, err := server.SendGroup(group, 10)
	if err != nil {
		fmt.Println("发送 Group 失败")
		return
	}

	go func(asyncResults []*result.AsyncResult) {
		for _, ar := range asyncResults {
			refs, err := ar.Get(1 * time.Second)
			if err != nil {
				fmt.Println("Get 失败,err: ", err.Error())
				return
			}
			fmt.Println("result:", tasks.HumanReadableResults(refs))
		}
	}(asyncResults)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
