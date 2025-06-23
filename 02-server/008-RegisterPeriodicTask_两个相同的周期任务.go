// 发送两个相同的周期任务
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

func main8() {
	server := myutils.MyServer()

	// 1. 注册周期任务，传入一个空任务（懒得新建 signature 了）
	// 只要不启动 worker，就会一直存在于 machinery_tasks 中
	server.RegisterPeriodicTask("*/1 * * * *", "any_name", &tasks.Signature{})

	// 2. 测试一下，是否能注册两个同名的周期任务
	// 结果是不能，因为 name+spec 唯一标识锁
	server.RegisterPeriodicTask("*/1 * * * *", "any_name", &tasks.Signature{})

	// 1 分钟后，可以看到 redis 中生成的锁名："machinery_lock_003-RegisterPeriodicTaskfxy*/1 * * * *"
	// 以及每隔 1 分钟，redis（backend）中就会增加一个 taskState，以及 machinery_tasks 中也会多一个 PENDING 的 signature

	select {}
}
