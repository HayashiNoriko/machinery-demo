package main

import (
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"
	"time"
)

func TaskPeriodic() {
	signature := &tasks.Signature{
		Name: "periodic",
		Args: []tasks.Arg{},
	}

	// 每分钟执行一次 period 方法
	err := MyServer.RegisterPeriodicTask("*/1 * * * ?", "period-task", signature)
	if err != nil {
		fmt.Println(err)
	}

	asyncResult, _ := MyServer.SendTask(signature)
	res, _ := asyncResult.Get(1 * time.Second)
	fmt.Println(res)
}
