// 本例我们可以停止 redis 服务，测试一下 broker 的连接重试
package main

import (
	myutils "demo/01-myutils"
)

func main3() {
	server := myutils.MyServer()

	// server.broker.retry 默认是 true 的
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
