// 任务超时检测，注意，只能检测/感知，无法强制终止一个 goroutine！go 语言本身就不支持

// 如果想要实现从外部中断 goroutine 的效果，那么只能退而求其次：业务逻辑必须支持“可中断”，可以被拆分成多个子任务，然后每执行一个子任务后，就检查 ctx.Done()，及时 return
// 但也不是十全十美的，因为执行子任务的过程中可能超时，但子任务无法立即退出

// 这个 demo 用于演示业务逻辑“可中断”的情况

package main

import (
	myutils "demo/01-myutils"
	"errors"
	"fmt"
	"time"

	"demo/sourcecode/machinery/v2/tasks"
)

// 定义任务超时错误
var ErrTaskTimeout = errors.New("task execution timed out")

// 实际执行的业务函数
// 业务可中断，我们把它拆分成 100 个子业务，每个子业务耗时 100 毫秒
// 整个任务大概耗时 10 秒
func MyTaskWithTimeout(timeout int64) error {
	timeChan := time.After(time.Duration(timeout) * time.Second)

	for i := 0; i < 100; i++ {
		select {
		case <-timeChan:
			fmt.Println("任务执行超时")
			return ErrTaskTimeout
		default:
			time.Sleep(100 * time.Millisecond) // 模拟任务耗时
			fmt.Println("MyTaskWithTimeout 子业务", i, "执行完成")
		}
	}

	fmt.Println("MyTaskWithTimeout 全部执行完成")
	return nil
}

func main4() {
	server := myutils.MyServer()

	// 1. 注册任务
	server.RegisterTask("MyTaskWithTimeout", MyTaskWithTimeout)

	// 2. 创建任务签名
	signature := &tasks.Signature{
		Name: "MyTaskWithTimeout",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 5, // 设定超时时间为 5 秒
			},
		},
	}

	// 3. 发送任务
	server.SendTask(signature)

	// 4. 创建一个 worker 去执行任务
	worker := server.NewWorker("myworker", 10)
	worker.Launch()
}
