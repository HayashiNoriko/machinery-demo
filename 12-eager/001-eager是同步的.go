// 基本的发送任务，SendTask，发送即执行，执行完成后才返回
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/brokers/eager"
)

func MyTask() error {
	fmt.Println("任务开始执行，大概需要 5 秒")
	time.Sleep(5 * time.Second)
	fmt.Println("任务执行完毕")
	return nil
}

func main1() {
	server := myutils.MyEagerServer()
	broker := server.GetBroker()

	// 0. 注册任务
	server.RegisterTask("MyTask", MyTask)

	// 1. 创建一个 worker，并 assign 到 broker
	// 对于 eager 的 broker，consumerTag 和 concurrency 都没有什么用了
	worker := server.NewWorker("myworker", 10)

	// 对于 eager 的 worker，不再需要我们执行它的 Launch，只需要将其 assign 到 broker 上，因为 broker.Publish 会立即拉起执行单元，执行任务
	// mode 是 eager 给我们提供的一个接口，作用是让我们使用它的 AssignWorker 方法
	mode := broker.(eager.Mode)
	mode.AssignWorker(worker)

	// 2. 发送任务
	signature := &tasks.Signature{
		Name: "MyTask",
	}
	asyncResult, _ := server.SendTask(signature) // 阻塞执行任务，执行完毕才返回

	// 我想要通过 asyncResult 随时获取异步任务的状态（PENDING、STARTED、SUCCESS...）
	// 但是执行到这一步时，任务已经完成了，我看不到中间状态
	fmt.Println("任务状态", asyncResult.GetState().State)

	fmt.Println("程序退出")
}
