// 通过 gin 给服务器发送任务，模拟调用 ALB api
package main

import (
	"demo/sourcecode/machinery/v2"
	redisbackend "demo/sourcecode/machinery/v2/backends/redis"
	"demo/sourcecode/machinery/v2/backends/result"
	redisbroker "demo/sourcecode/machinery/v2/brokers/redis"
	"demo/sourcecode/machinery/v2/config"
	redislock "demo/sourcecode/machinery/v2/locks/redis"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
	"sync"
	"time"

	// "os"

	"github.com/gin-gonic/gin"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db       *gorm.DB          // gorm 数据库连接实例
	mserver  *machinery.Server // machinery 服务器实例
	signalWG sync.WaitGroup    // 同步
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
	db.AutoMigrate(&Lb{})
	db.AutoMigrate(&Task{})
}

func initMachinery() {
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
	mserver.RegisterTask("CreateLoadBalancer", CreateLoadBalancer)
}

func initGin() {
	r := gin.Default()

	// 用户创建一台实例，返回 taskId
	// 用户后续可以通过 taskId 查询任务状态
	r.GET("/createLoadBalancer", func(c *gin.Context) {
		userId := c.Query("userId")
		if userId == "" {
			c.JSON(400, gin.H{
				"message": "userId 为空",
			})
			return
		}

		// 1. 发送 CreateLoadBalancer 任务给 machinery
		signature := &tasks.Signature{
			Name: "CreateLoadBalancer",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: userId,
				},
			},
		}

		// 2. 得到 asyncResult
		asyncResult, err := mserver.SendTask(signature)
		if err != nil {
			fmt.Println(err)
			c.JSON(500, gin.H{
				"message": "服务器发送异步任务失败",
			})
			return
		}

		// 3. 从 asyncResult 中获取 taskId
		taskId := asyncResult.Signature.UUID

		// 4. 更新 Task 和 Lb 表
		go CreateLoadBalancerRow(asyncResult)

		// 5. 返回 taskId
		c.JSON(200, gin.H{
			"message": "ok",
			"taskId":  taskId,
		})
	})

	r.Run(":8080")
}

// 阻塞等待 asyncResult 完成后，更新 Task 和 Lb 表
// Task 表：立即更新，添加一行新记录（把 redis 中的记录迁移到 mysql 中）
// Lb 表：要新增的行已经在 CreateLoadBalancer 异步任务中新增了。现在我们需要，再修改行（把）
func CreateLoadBalancerRow(asyncResult *result.AsyncResult) error {
	// 从 Tasks 表中，根据 taskId 查询到 task 行
	taskId := asyncResult.Signature.UUID
	var task Task
	db.First(&task, "task_id = ?", taskId)

	// 阻塞，从 asyncResult 中获取结果（lbId）
	res, _ := asyncResult.Get(time.Second)
	lbId := res[0].String()

	// 根据 asyncResult 的状态，判断任务成功或失败
	task.TaskState = asyncResult.GetState().State
	// 更新 Lb 表
	if task.TaskState == "SUCCESS" {
		db.Model(&Lb{}).Where("lb_id = ?", lbId).Update("LbState", "Active")
	} else {
		db.Model(&Lb{}).Where("lb_id = ?", lbId).Update("LbState", "CreateFailed")
	}

	// 不管成功或失败，都更新 task 行
	db.Save(&task) // 更新全部字段
	// db.Model(&task).Update("State", task.State) // 更新单个字段

	return nil
}
