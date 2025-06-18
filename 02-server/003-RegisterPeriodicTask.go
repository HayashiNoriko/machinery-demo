// 测试一下 lock 相关的东西
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

func main() {
	server := myutils.MyServer()

	// 1. 注册定时任务，传入一个空任务试试，应该可以发布成功
	// 但只要不启动 worker，就会一直存在于 machinery_tasks 中
	server.RegisterPeriodicTask("*/1 * * * *", "fxy", &tasks.Signature{})

	select {}
}
