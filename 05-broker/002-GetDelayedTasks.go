// 获取延迟任务队列
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func main2() {
	server := myutils.MyServer()

	// 1. 发送三个延迟任务，存储在延迟任务队列中
	// 注意，延迟任务队列名只有一个地方可以设置：config.Redis.DelayedTasksKey
	// 在创建 broker 的时候，就会去检查是否有设置这个 config.Redis.DelayedTasksKey
	// 如果没有设置，那么会使用框架默认的 "delayed_tasks"
	// 因此，延迟任务队列名的设置没有那么灵活，在 broker 实例创建的时候就已经定下来了，全局只有一个
	eta := time.Now().UTC().Add(time.Second * 100)

	server.SendTask(&tasks.Signature{
		ETA: &eta,
	})
	server.SendTask(&tasks.Signature{
		ETA: &eta,
	})
	server.SendTask(&tasks.Signature{
		ETA: &eta,
	})

	// 2. 要通过 broker 来获取哦
	delayedTasks, err := server.GetBroker().GetDelayedTasks()
	if err != nil {
		fmt.Println(err)
	}

	// 3. 打印结果
	fmt.Println(delayedTasks)

}
