// 获取异步任务结果
package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

func LongTimeTask() error {
	fmt.Println("执行长时间任务，需要 10 秒")
	time.Sleep(time.Second * 10)
	return nil
}

func main() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("LongTimeTask", LongTimeTask)

	// 2. 发送任务
	signature := &tasks.Signature{
		Name: "LongTimeTask",
		Args: []tasks.Arg{},
	}

	asyncResult, _ := server.SendTask(signature)

	// 3. 非阻塞获取异步任务的状态
	go func() {
		for {
			fmt.Println("异步任务状态：", asyncResult.GetState().State)
			time.Sleep(time.Second)
		}
	}()

	// 4. 休眠 5 秒，观察任务的 PENDING 状态（此时还没有被 worker 取出呢）
	time.Sleep(5 * time.Second)

	// 5. 阻塞获取异步任务的结果
	// 等待任务执行结束，这个过程中任务一直是 STARTED 状态，执行成功后，变成 SUCCESS 状态，Get 方法才返回
	go func() {
		res, _ := asyncResult.Get(time.Second)
		fmt.Println("异步任务结果：", res)
	}()

	// 6. 启动 worker 消费任务
	// 启动 worker 后，任务变为 RECEIVED 状态（时间很短，应该观察不到），然后马上变成 STARTED 状态
	worker := server.NewWorker("worker1", 10)
	worker.Launch()
}
