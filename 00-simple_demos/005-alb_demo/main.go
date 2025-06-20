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

	"github.com/gin-gonic/gin"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db      *gorm.DB          // gorm 数据库连接实例
	mserver *machinery.Server // machinery 服务器实例
)

func main() {
	// 1. 初始化 mysql 连接
	initDB()

	// 2. 创建 machinery 服务器实例
	initMachinery()

	// 3. 启动 gin 服务器
	go initGin()

	// 4. 启动 machinery 的消费者 Worker
	worker := mserver.NewWorker("myworker", 10)
	worker.Launch()
}

func initDB() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	db.AutoMigrate(&tables.Lb{})
	db.AutoMigrate(&tables.Task{})
}

func initMachinery() {
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
