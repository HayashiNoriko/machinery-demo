// 生产者
package main

import (
	"fmt"
	"time"

	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

// 先输入 worker，启动 worker 消费者
// 再不加参数，启动生产者
func main() {
	server := myutils.MyServer()

	// 任务 1，长任务，发送一次，观察 worker 的强制退出情况
	signature1 := &tasks.Signature{
		Name: "LongTask",
	}

	// 任务 2，打印任务，会一直发送，观察 worker 的优雅退出情况
	signature2 := &tasks.Signature{
		Name: "Print",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: myutils.GetLogPath(),
			},
			{
				Type:  "string",
				Value: "hello world",
			},
		},
	}

	// 任务 3，退出 worker 的任务
	signature3 := &tasks.Signature{
		Name:       "Quit",
		RoutingKey: "quit_queue",
	}

	// 发送长任务，未指定 RoutingKey，发布到默认队列，由 normal_worker 消费
	asyncResult, _ := server.SendTask(signature1)

	// 发送打印任务，未指定 RoutingKey，发布到默认队列，由 normal_worker 消费
	// 每秒钟发送一次
	go func() {
		for {
			server.SendTask(tasks.CopySignature(signature2))
			time.Sleep(time.Second * 1)
		}
	}()

	// 检测到长任务运行超时，发送退出任务，由 quit_worker 消费
	_, err := asyncResult.GetWithTimeout(10*time.Second, 200*time.Millisecond)
	if err == result.ErrTimeoutReached {
		fmt.Println("检测到长任务运行超时，发送退出任务，由 quit_worker 消费")
		server.SendTask(signature3)
	}

	time.Sleep(time.Second * 999999)
}
