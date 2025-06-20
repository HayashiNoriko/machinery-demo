package asynctasks

import (
	"demo/00-simple_demos/005-alb_demo/tables"
	"fmt"
	"time"
)

// 用户创建一台实例（machinery 异步任务）
func CreateLoadBalancer(userId string, lbId string) error {

	// 新增一行 Lb 表记录
	lb := &tables.Lb{
		LbId:    lbId,
		UserId:  userId,
		LbState: "Provisioning", // Active、InActive、Provisioning、Configuring、CreateFailed
	}
	db.Create(lb)

	// 模拟创建实例耗时
	fmt.Println("创建实例中...")
	time.Sleep(time.Second * 10)

	// 创建好了，修改这行表记录
	lb.LbState = "Active"
	db.Save(lb)

	return nil
}
