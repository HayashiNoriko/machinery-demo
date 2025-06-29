package asynctasks

import (
	"demo/00-simple_demos/005-alb_demo/tables"
	"fmt"
	"time"
)

// 用户创建一台实例（machinery 异步任务）
func CreateLoadBalancer(userId string, lbId string) error {
	// 模拟创建实例耗时
	fmt.Println("创建实例中...")
	time.Sleep(time.Second * 10)

	// 创建好了，修改这行表记录
	result := db.Model(&tables.Lb{}).Where("lb_id = ?", lbId).Update("LbState", "Active")
	if result.Error != nil {
		// 处理修改数据表失败的情况...
		return result.Error
	}

	return nil
}
