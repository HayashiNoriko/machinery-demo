package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// 异步任务，用户创建一台实例，需要发送给 machinery 框架
func CreateLoadBalancer(userId string, taskId string) (string, error) {

	// 新增一条 Lb 表记录
	lbId := "alb_" + uuid.New().String()
	db.Create(&Lb{
		LbId:    lbId,
		UserId:  userId,
		TaskId:  taskId,
		LbState: "Provisioning", // Active、InActive、Provisioning、Configuring、CreateFailed
	})

	// 模拟创建实例耗时
	fmt.Println("创建实例中...")
	time.Sleep(time.Second * 15)

	// 创建好了，得到 lbId，作为 asyncResult 返回给 machinery

	return lbId, nil
}
