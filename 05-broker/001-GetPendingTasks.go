// 获取普通任务队列
package main

import (
	myutils "demo/01-myutils"
	"fmt"

	"demo/sourcecode/machinery/v2/tasks"
)

func main1() {
	server := myutils.MyServer()

	// 1. 发送三个任务，存储在普通任务队列中
	server.SendTask(&tasks.Signature{}) // 使用默认的队列名，在 config.DefaultQueue 中设置
	server.SendTask(&tasks.Signature{
		RoutingKey: "my_custom_queue", // 自定义队列名
	})
	server.SendTask(&tasks.Signature{
		RoutingKey: "my_custom_queue", // 自定义队列名
	})

	// 关于队列名，目前有两个地方可以设置，一是在 config.DefaultQueue 中设置；二是每次发送任务时，在 Signature 结构体中设置，比较灵活
	// 在 Publish 的时候，会先 AdjustRoutingKey：
	// 如果 Signature 中设置了 RoutingKey，则以 Signature 中的为准；否则将其修改为 config.DefaultQueue

	// 2. 要通过 broker 来获取哦
	pendingTasks1, err := server.GetBroker().GetPendingTasks("my_default_tasks")
	if err != nil {
		fmt.Println(err)
	}
	pendingTasks2, err := server.GetBroker().GetPendingTasks("my_custom_queue")
	if err != nil {
		fmt.Println(err)
	}

	// 3. 打印结果
	fmt.Println(pendingTasks1)
	fmt.Println(pendingTasks2)

}
