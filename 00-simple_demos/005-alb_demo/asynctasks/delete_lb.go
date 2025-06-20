package asynctasks

import (
	"demo/00-simple_demos/005-alb_demo/tables"
	"fmt"
	"time"
)

// 用户删除一台实例（machinery 异步任务）
func DeleteLoadBalancer(userId string, lbId string) error {

	// 查询这行 Lb 表记录
	var lb tables.Lb
	db.First(&lb, "lb_id = ?", lbId) // 通过 lb_id 字段查询

	// 检测是否存在这行记录
	if lb.LbId == "" {
		return fmt.Errorf("lb_id %s not found", lbId)
	}

	// 模拟删除实例耗时
	fmt.Println("删除实例中...")

	// 先把状态改为 InActive
	time.Sleep(time.Second * 5)
	lb.LbState = "InActive"
	db.Save(&lb)

	// 删除这行记录
	time.Sleep(time.Second * 5)
	db.Delete(&lb)

	return nil
}
