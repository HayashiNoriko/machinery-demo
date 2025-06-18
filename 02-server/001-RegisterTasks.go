// 任务注册的相关方法
// 只注册，不发布
package main

import (
	myutils "demo/01-myutils"
	"fmt"
)

func main1() {
	server := myutils.MyServer()

	// 任务已经在 myutils.MyServer 中注册过了，以后就都不注册了

	// 1.获取所有任务名
	names := server.GetRegisteredTaskNames()
	fmt.Println(names)

	// 2.根据任务名获取任务函数
	// 获取到 interface{}
	addFuncRaw, _ := server.GetRegisteredTask("Add")
	// 断言为需要的类型
	addFunc := addFuncRaw.(func(args ...int64) (int64, error))
	// 调用
	res, _ := addFunc(1, 2, 3, 4)
	fmt.Println(res)

	// 3.检查某个任务名是否被注册到该 server 实例中
	fmt.Println("Add", server.IsTaskRegistered("Add"))                     // true
	fmt.Println("Periodic", server.IsTaskRegistered("Periodic"))           // true
	fmt.Println("NotRegistered", server.IsTaskRegistered("NotRegistered")) // false

}
