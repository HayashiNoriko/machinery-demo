// 大部分情况下，业务逻辑“不可中断”，那么超时后只能放弃等待、返回错误给 redis，但业务的 goroutine 还会继续执行，这也是 go 生态下的通用限制，Go 语言本身也无法强制 kill 掉一个正在运行的 goroutine
// 你可以在外部感知到超时（比如 ctx 超时后，外层 select 能收到），但业务逻辑本身不会自动停止，它会继续运行直到自然结束

// 这个 demo 用于演示业务逻辑“不可中断”的情况
// 只感知超时，不强制终止 goroutine
// 对超时的感知逻辑，我们就不再在 taskFunc 中做了，直接用 asyncResult.GetWithTimeout() 来感知超时

package main

import (
	myutils "demo/01-myutils"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
)

// 定义任务超时错误，上一例中已经定义了
// var ErrTaskTimeout = errors.New("task execution timed out")

// 实际执行的业务函数
// 就跟其他普通的函数没什么区别
// 耗时 10 秒
func MyTask() error {
	time.Sleep(10 * time.Second) // 模拟业务耗时
	fmt.Println("MyTask 全部执行完成")
	return nil
}

func main5() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MyTask", MyTask)

	// 2. 创建任务签名
	signature := &tasks.Signature{
		Name: "MyTask",
	}

	// 3. 发送任务
	asyncResult, err := server.SendTask(signature)
	if err != nil {
		fmt.Println("SendTask error:", err)
		return
	}

	// 4. 另起一个 goroutine 去感知超时（避免 GetWithTimeout 阻塞主流程）
	go func(asyncResult *result.AsyncResult) {
		// 感知超时，时间定为 5 秒
		res, err := asyncResult.GetWithTimeout(5*time.Second, 500*time.Millisecond)
		if res == nil && err == result.ErrTimeoutReached {
			// 感知到了，但任务本身还是会继续执行
			fmt.Println("MyTask 超时了")
			// do something...
		}
	}(asyncResult)

	// 5. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
