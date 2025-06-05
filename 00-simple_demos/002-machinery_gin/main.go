// 通过 gin 给 machinery 服务器发送任务
package main

import (
	"demo/99-others/jobs"
	"demo/sourcecode/machinery/v2"
	redisbackend "demo/sourcecode/machinery/v2/backends/redis"
	redisbroker "demo/sourcecode/machinery/v2/brokers/redis"
	"demo/sourcecode/machinery/v2/config"
	eagerlock "demo/sourcecode/machinery/v2/locks/eager"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"

	// "os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

var MyServer *machinery.Server

func main() {
	// // 0. 启动 gin 服务器
	// if len(os.Args) == 2 && os.Args[1] == "gin" {
	// 	StartGin()
	// 	return
	// }

	// 1. 创建 machinery 服务器实例
	cnf := &config.Config{
		DefaultQueue:    "machinery_gin_tasks",
		ResultsExpireIn: 3600, // 1 小时后清理 redis 中的数据
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
	broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
	backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
	lock := eagerlock.New()
	MyServer = machinery.NewServer(cnf, broker, backend, lock)

	// 2. 注册任务
	MyServer.RegisterTasks(map[string]interface{}{
		"add":      jobs.Add,
		"periodic": jobs.Periodic,
	})

	// 4. 启动 gin 服务器
	go func() {
		r := gin.Default()
		// 发送 Add 任务
		r.GET("/add/:arg1/:arg2", func(c *gin.Context) {
			arg1 := c.Param("arg1")
			arg2 := c.Param("arg2")
			fmt.Println("received arg1:", arg1, "arg2:", arg2)
			a, _ := strconv.ParseInt(arg1, 10, 64)
			b, _ := strconv.ParseInt(arg2, 10, 64)

			signature := &tasks.Signature{
				Name: "add",
				Args: []tasks.Arg{
					{
						Type:  "int64",
						Value: a,
					},
					{
						Type:  "int64",
						Value: b,
					},
				},
			}

			asyncResult, err := MyServer.SendTask(signature)
			if err != nil {
				fmt.Println(err)
			}
			res, _ := asyncResult.Get(1 * time.Second)
			c.JSON(200, gin.H{"task_id": asyncResult.Signature.UUID, "task_result": res[0].Int()})
		})

		// 发送 Periodic 任务
		r.GET("/periodic/:arg", func(c *gin.Context) {
			arg := c.Param("arg")
			fmt.Println("received periodic", arg)
			signature := &tasks.Signature{
				Name: "periodic",
				Args: []tasks.Arg{},
			}

			// 每分钟执行一次 period 方法
			err := MyServer.RegisterPeriodicTask("*/1 * * * ?", arg, signature)
			if err != nil {
				fmt.Println(err)
			}

			c.String(200, "periodic task registered")
		})
		r.Run(":8080")
	}()

	// 3. 启动 Worker
	consumerTag := "machinery_gin_worker"
	worker := MyServer.NewWorker(consumerTag, 10)
	worker.Launch()

}
