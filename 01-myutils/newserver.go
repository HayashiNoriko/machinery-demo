package myutils

import (
	"demo/sourcecode/machinery/v2"
	redisbackend "demo/sourcecode/machinery/v2/backends/redis"
	redisbroker "demo/sourcecode/machinery/v2/brokers/redis"
	"demo/sourcecode/machinery/v2/config"
	redislock "demo/sourcecode/machinery/v2/locks/redis"

	mysqlbackend "demo/13-mysql/backend"
	eagerbackend "demo/sourcecode/machinery/v2/backends/eager"
	eagerbroker "demo/sourcecode/machinery/v2/brokers/eager"
	eagerlock "demo/sourcecode/machinery/v2/locks/eager"
)

func MyServer() *machinery.Server {
	// 创建 config
	cnf := &config.Config{
		DefaultQueue:    "my_default_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
			DelayedTasksKey:        "my_delayed_queue",
		},
	}

	// 创建服务器实例
	broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
	backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
	lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

	server := machinery.NewServer(cnf, broker, backend, lock)

	// 注册任务
	server.RegisterTasks(map[string]interface{}{
		"Add":      Add,
		"Periodic": Periodic,
		"Print":    Print,
	})

	return server

}

// 使用 eager 作为 broker、backend、lock
func MyEagerServer() *machinery.Server {
	// 创建 config
	cnf := &config.Config{
		DefaultQueue: "my_default_tasks",
	}

	// 创建服务器实例
	broker := eagerbroker.New()
	backend := eagerbackend.New()
	lock := eagerlock.New()

	server := machinery.NewServer(cnf, broker, backend, lock)

	// 注册任务
	server.RegisterTasks(map[string]interface{}{
		"Add":      Add,
		"Periodic": Periodic,
		"Print":    Print,
	})

	return server
}

// 使用 mysql 作为 backend，其他依然使用 redis
func MyMySQLServer() *machinery.Server {
	// 创建 config
	cnf := &config.Config{
		DefaultQueue:    "my_default_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
			DelayedTasksKey:        "my_delayed_queue",
		},
	}

	// 创建服务器实例
	broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
	backend := mysqlbackend.New(cnf, "root:Root123456!@tcp(127.0.0.1:3306)", "testdb")
	lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

	server := machinery.NewServer(cnf, broker, backend, lock)

	// 注册任务
	server.RegisterTasks(map[string]interface{}{
		"Add":      Add,
		"Periodic": Periodic,
		"Print":    Print,
	})

	return server
}
