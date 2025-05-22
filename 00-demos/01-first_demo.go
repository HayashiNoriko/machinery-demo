// 通过一个简单的 demo 程序来学习 machinery 的基本使用

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/tasks"
)

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
	asyncResult, _ := server.SendTask(signature)        // 任务可以通过将Signature的实例传递给Server实例来调用
	results, _ := asyncResult.Get(time.Millisecond * 5) // 您还可以执行同步阻塞调用来等待任务结果
	for _, result := range results {
		fmt.Println(result.Interface())
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
	asyncResult, _ := server.SendTask(signature)
	fmt.Println(asyncResult)
}

// 第一：配置Server并注册任务
func startServer() (*machinery.Server, error) {
	cnf := &config.Config{
		DefaultQueue:    "machinery_tasks",
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

// 第二步：启动Worker
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

// 第三步：添加异步执行函数
func Add(args ...int64) (int64, error) {
	println("############# 执行Add方法 #############")
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

// 第四步：添加一个周期性任务
func PeriodicTask() error {
	fmt.Println("################ 执行周期任务PeriodicTask #################")
	return nil
}
