// 通过一个简单的 demo 程序来学习 machinery 的基本使用

package main

import (
	"fmt"
	"os"
	"time"

	"demo/sourcecode/machinery/v2"
	redisbackend "demo/sourcecode/machinery/v2/backends/redis"
	redisbroker "demo/sourcecode/machinery/v2/brokers/redis"
	"demo/sourcecode/machinery/v2/config"
	eagerlock "demo/sourcecode/machinery/v2/locks/eager"
	"demo/sourcecode/machinery/v2/tasks"
)

// 第四步：运行程序
// 先输入 worker，启动 worker 消费者
// 再不加参数，启动生产者
func main() {
	if len(os.Args) == 2 && os.Args[1] == "worker" { // 启动worker
		if err := worker(); err != nil {
			panic(err)
		}
	}
	TestPeriodicTask() // 触发一个定时任务（定时任务由客户端控制，客户端退出定时就会结束）
	TestAdd()          // 触发一个异步任务
	time.Sleep(time.Second * 1000)
}

/* 触发执行Add异步任务 */
func TestAdd() {
	server, _ := startServer() // 调用异步任务 Add 函数，执行 1+4=5这个逻辑
	signature := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 4,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}
	asyncResult, _ := server.SendTask(signature) // 任务可以通过将Signature的实例传递给Server实例来调用
	// results, _ := asyncResult.Get(time.Millisecond * 1) // 可以执行同步阻塞调用来等待任务结果

	// 死循环，获取任务状态（PENDING、RECEIVED、STARTED、RETRY、SUCCESS、FAILURE 等)
	for {
		fmt.Println(asyncResult.GetState().State)
		time.Sleep(1 * time.Second)
	}
}

/* 触发执行periodicTask异步任务 */
func TestPeriodicTask() {
	server, _ := startServer()
	signature := &tasks.Signature{
		Name: "periodicTask",
		Args: []tasks.Arg{},
	}
	// 每分钟执行一次periodicTask函数，验证发现不支持秒级别定时任务
	err := server.RegisterPeriodicTask("*/1 * * * ?", "periodic-task", signature)
	if err != nil {
		fmt.Println(err)
	}
}

// 第一步：添加两个方法：Add、PeriodicTask
func Add(args ...int64) (int64, error) {
	fmt.Println("执行Add方法...")

	time.Sleep(time.Second * 10) // 模拟耗时

	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	fmt.Println("sum=", sum)
	return sum, nil
}

func PeriodicTask() error {
	fmt.Println("执行周期任务PeriodicTask...")
	return nil
}

// 第二步：配置Server并注册任务
func startServer() (*machinery.Server, error) {
	cnf := &config.Config{
		DefaultQueue: "machinery_tasks",
		// 这两个其实可以不用写，重要的是后面传入的 broker、backend 实例
		Broker:          "redis://123456@localhost:6379",
		ResultBackend:   "redis://localhost:6379",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	// 创建服务器实例
	broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
	backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	// 注册异步任务
	tasksMap := map[string]interface{}{
		"add":          Add,
		"periodicTask": PeriodicTask,
	}
	return server, server.RegisterTasks(tasksMap)
}

// 第三步：启动Worker
func worker() error {
	//消费者的标记
	consumerTag := "machinery_worker"

	server, err := startServer()
	if err != nil {
		return err
	}

	//第二个参数并发数, 0表示不限制
	worker := server.NewWorker(consumerTag, 0)

	//钩子函数
	errorhandler := func(err error) {}
	pretaskhandler := func(signature *tasks.Signature) {}
	posttaskhandler := func(signature *tasks.Signature) {}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)

	// 这个方法会阻塞当前 goroutine，worker 会一直监听任务队列并消费任务，直到进程被终止
	return worker.Launch()
}
