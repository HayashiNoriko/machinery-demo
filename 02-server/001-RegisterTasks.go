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

	// 4. 注册任务的要求！
	// （1）第二个参数必须是函数类型
	// （2）至少有一个返回值
	// （3）最后一个返回值必须是 error 类型

	// 注册一些不符合要求的任务，试试，会返回错误

	// 4.1 第二个参数不是函数类型
	err := server.RegisterTask("taskname", 123)
	fmt.Println(err) // Task must be a func type

	// 4.2 没有返回值
	err = server.RegisterTask("taskname", func() {
		fmt.Println("do something")
	})
	fmt.Println(err) // Task must return at least a single value

	// 4.3 最后一个返回值不是 error 类型
	err = server.RegisterTask("taskname", func() (int64, int64) {
		return 1, 2
	})
	fmt.Println(err) // Last return value of a task must be error

}
