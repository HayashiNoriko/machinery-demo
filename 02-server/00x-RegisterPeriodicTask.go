// 测试一下 注册周期任务
package main

import (
	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

func main666() {
	server := myutils.MyServer()

	// 1. 注册周期任务
	server.RegisterPeriodicTask("*/1 * * * *", "fxy", &tasks.Signature{})

	select {}
}
