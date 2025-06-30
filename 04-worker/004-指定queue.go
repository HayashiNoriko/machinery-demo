// 在终端分别输入参数 worker1 和 worker2，启动两个 worker，分别监听不同的 queue
// (注意 worker的 consumerTag 对于 goredis 实现的 broker 来说没有什么用)
// 然后分别输入 sender1 和 sender2，可以向两个 queue 发送任务
// 观察输出
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"os"
)

func main4() {
	server := myutils.MyServer()

	if len(os.Args) == 2 {
		param := os.Args[1]
		switch param {
		case "worker1":
			// 启动 worker1
			worker := server.NewCustomQueueWorker("worker1", 10, "my_queue1")
			worker.Launch()
		case "worker2":
			// 启动 worker2
			worker := server.NewCustomQueueWorker("worker2", 10, "my_queue2")
			worker.Launch()
		case "sender1":
			// 向 my_queue1 发送任务
			signature := &tasks.Signature{
				Name: "Print",
				Args: []tasks.Arg{
					{
						Type:  "string",
						Value: myutils.GetLogPath(),
					},
					{
						Type:  "string",
						Value: "my_queue1",
					},
				},
				RoutingKey: "my_queue1",
			}
			server.SendTask(signature)
		case "sender2":
			// 向 my_queue2 发送任务
			signature := &tasks.Signature{
				Name: "Print",
				Args: []tasks.Arg{
					{
						Type:  "string",
						Value: myutils.GetLogPath(),
					},
					{
						Type:  "string",
						Value: "my_queue2",
					},
				},
				RoutingKey: "my_queue2",
			}
			server.SendTask(signature)
		}
	}
}
