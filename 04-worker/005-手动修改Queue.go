package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2"
	"demo/sourcecode/machinery/v2/tasks"
	"time"
)

func main5() {
	server := myutils.MyServer()

	// 发送 10 个任务到 queue1
	signature1 := &tasks.Signature{
		Name:       "Print",
		RoutingKey: "queue1",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "queue1",
			},
		},
	}
	for i := 0; i < 10; i++ {
		server.SendTask(tasks.CopySignature(signature1))
	}

	// 发送 10 个任务到 queue2
	signature2 := &tasks.Signature{
		Name:       "Print",
		RoutingKey: "queue2",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "queue2",
			},
		},
	}
	for i := 0; i < 10; i++ {
		server.SendTask(tasks.CopySignature(signature2))
	}

	// 初始化 worker 的 Queue 为 queue1
	worker := server.NewCustomQueueWorker("myworker", 10, "queue1")

	// 5 秒后，切换 worker 的 Queue 为 queue2
	go func(worker *machinery.Worker) {
		time.Sleep(time.Second * 5)
		worker.Queue = "queue2"
	}(worker)

	worker.Launch()
}
