// 通过 gin 给服务器发送任务，模拟调用 ALB api
package main

import (
	"demo/00-simple_demos/005-alb_demo/asynctasks"
	"demo/00-simple_demos/005-alb_demo/ginhandlers"
	"demo/00-simple_demos/005-alb_demo/tables"
	"demo/sourcecode/machinery/v2"
	redisbackend "demo/sourcecode/machinery/v2/backends/redis"
	redisbroker "demo/sourcecode/machinery/v2/brokers/redis"
	"demo/sourcecode/machinery/v2/config"
	redislock "demo/sourcecode/machinery/v2/locks/redis"
	"demo/sourcecode/machinery/v2/tasks"

	"github.com/gin-gonic/gin"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db      *gorm.DB          // gorm 数据库连接实例
	mserver *machinery.Server // machinery 服务器实例
	worker  *machinery.Worker // machinery 消费者实例
)

func main() {
	// 1. 初始化 mysql 连接
	initDB()

	// 2. 创建 machinery 服务器实例
	initMachineryServer()

	// 3. 创建 machinery 消费者实例
	initMachineryWorker()

	// 3. 启动 gin 服务器
	go initGin()

	// 4. 启动 machinery 的消费者 Worker
	worker.Launch()
}

func initDB() {
	dsn := "root:Root123456!@tcp(127.0.0.1:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	db.AutoMigrate(&tables.Lb{})
	db.AutoMigrate(&tables.Task{})
}

func initMachineryServer() {
	asynctasks.Init(db)

	cnf := &config.Config{
		DefaultQueue:    "alb_tasks",
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
	lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

	// 创建 machinery 服务器实例
	mserver = machinery.NewServer(cnf, broker, backend, lock)

	// 注册任务
	mserver.RegisterTasks(map[string]interface{}{
		"CreateLoadBalancer": asynctasks.CreateLoadBalancer,
		"DeleteLoadBalancer": asynctasks.DeleteLoadBalancer,
	})

	// 设置 server 任务发布前钩子，在 SendTask 中，生成 UUID 之后、Publish 之前，将任务状态信息存入数据库
	mserver.SetPreTaskHandler(func(signature *tasks.Signature) {
		task := tables.Task{
			TaskId:   signature.UUID,
			TaskName: signature.Name,
			State:    "未执行",
			Results:  nil,
			Err:      "",
		}

		// 添加这个 Task 行
		db.Create(&task)
	})
}

func initMachineryWorker() {
	worker = mserver.NewWorker("myworker", 10)

	// 设置任务执行前钩子，将任务状态信息更新为“执行中”
	worker.SetPreTaskHandler(func(signature *tasks.Signature) {
		result := db.Model(&tables.Task{}).Where("task_id = ?", signature.UUID).Update("State", "执行中")
		if result.Error != nil {
			// 处理 gorm 修改数据表失败的情况...
			panic(result.Error)
		}
	})

	// 设置任务执行后钩子，将任务状态信息更新为“执行成功”/“执行失败”，或者不变（RETRY 的特殊情况）
	worker.SetPostTaskHandler(func(signature *tasks.Signature) {
		taskState, err := mserver.GetBackend().GetState(signature.UUID)
		if err != nil {
			// 处理 backend 获取任务状态失败的情况...
			panic(err)
		}

		// 更新任务状态
		if taskState.IsSuccess() {
			result := db.Model(&tables.Task{}).Where("task_id = ?", signature.UUID).Update("State", "执行成功")
			if result.Error != nil {
				// 处理修改数据表失败的情况...
				panic(result.Error)
			}
		}
		if taskState.IsFailure() {
			result := db.Model(&tables.Task{}).Where("task_id = ?", signature.UUID).Update("State", "执行失败")
			if result.Error != nil {
				// 处理修改数据表失败的情况...
				panic(result.Error)
			}
		}

		// 如果既不是 SUCCESS、又不是 FAILURE，那么就只能是 RETRY，RETRY 视为“执行中”，则无需修改
	})

}

func initGin() {
	ginhandlers.Init(mserver, db)

	r := gin.Default()

	// 用户创建一台实例
	r.GET("/createLoadBalancer", ginhandlers.CreateLoadBalancer)

	// 用户删除一台实例
	r.GET("/deleteLoadBalancer", ginhandlers.DeleteLoadBalancer)

	// 用户获取异步任务列表
	r.GET("/listAsyncJobs", ginhandlers.ListAsyncJobs)

	r.Run(":8080")
}
