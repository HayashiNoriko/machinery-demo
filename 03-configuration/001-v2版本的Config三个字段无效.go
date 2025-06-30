package main

import myutils "demo/01-myutils"

func main1() {
	server := myutils.MyServer()

	// v1 版本中，会根据这三个字段去创建真实的连接
	// v2 版本中，不再使用这三个字段去创建连接了，而是我们自己创建好 broker、backend、lock 实例，传入到 NewServer 中
	// Broker 和 ResultBackend 字段只用于在启动 worker 时打印日志信息（通过 RedactURL 脱敏处理后），所以理论上输入什么都不会影响 worker 正常执行
	// Lock 字段好像是真没用了
	server.GetConfig().Broker = "broker666://broker666"
	server.GetConfig().ResultBackend = "backend666://backend666"
	server.GetConfig().Lock = "lock666://lock666"

	worker := server.NewWorker("worker1", 10)
	worker.Launch()
}
