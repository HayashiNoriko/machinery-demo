// 消费者，两个 worker

package main

import (
	"errors"
	"fmt"
	"time"

	myutils "demo/01-myutils"
)

// quit_worker 给自己的 errorsChan 发送退出信号，结束掉进程
var errorsChan = make(chan error)

// 先输入 worker，启动 worker 消费者
// 再不加参数，启动生产者
func main() {
	server := myutils.MyServer()
	server.RegisterTask("LongTask", LongTask)
	server.RegisterTask("Quit", Quit)

	// 用来正常工作的 worker，监听默认队列
	go func() {
		normal_worker := server.NewWorker("normal_worker", 0)
		normal_worker.Launch()
	}()

	// 用来退出的 worker，监听 quit_queue 队列
	// quit_worker := server.NewCustomQueueWorker("quit_worker", 1, "quit_queue")
	// quit_worker.LaunchAsync(errorsChan)
	// <-errorsChan
	time.Sleep(time.Second * 10)
	return

}

// 一个长时间运行的任务
func LongTask() error {
	for {
		fmt.Println("LongTask...")
		time.Sleep(1 * time.Second)
	}
	return nil
}

// 退出 worker 的任务，自己给自己发信号
// 因为 quit_worker 和 normal_worker 运行在同一进程中，所以可以一起退出
func Quit() error {
	errorsChan <- errors.New("被要求 quit")
	return nil
}
