package ginhandlers

import (
	"demo/00-simple_demos/005-alb_demo/tables"
	"demo/sourcecode/machinery/v2/backends/result"
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// 用户创建一台实例，返回 lbId、taskId
// 用户后续可以通过 lbId 查询实例状态，也可以通过 taskId 查询异步任务状态
func CreateLoadBalancer(c *gin.Context) {
	// 1. 获取参数
	userId := c.Query("userId")
	if userId == "" {
		c.JSON(400, gin.H{
			"message": "userId 为空",
		})
		return
	}

	// 2. 立即生成 lbId，返回给用户
	lbId := "alb_" + uuid.New().String()

	// 3. 发送 CreateLoadBalancer 任务给 machinery
	signature := &tasks.Signature{
		Name: "CreateLoadBalancer",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: userId,
			},
			{
				Type:  "string",
				Value: lbId,
			},
		},
	}

	// 4. 得到 asyncResult
	asyncResult, err := mserver.SendTask(signature)
	if err != nil {
		fmt.Println(err)
		c.JSON(500, gin.H{
			"message": "服务器发送异步任务失败",
		})
		return
	}

	// 3. 更新 Task 和 Lb 表
	go CreateLoadBalancerTask(asyncResult, userId, lbId)

	// 4. 返回 lbId
	c.JSON(200, gin.H{
		"message": "ok",
		"lbId":    lbId,
		"taskId":  asyncResult.Signature.UUID,
	})
}

// 阻塞等待 asyncResult 完成后，更新 Task 表
// 添加一行新记录（把 redis 中的记录迁移到 mysql 中）
func CreateLoadBalancerTask(asyncResult *result.AsyncResult, userId, lbId string) {

	// 插入一行 Task 记录
	insertTask(asyncResult, userId)

	// 更新 Lb 行，因为 CreateLoadBalancer 可能意外失败、突然返回，因此需要在外部确保 Lb 行的 LbState 是正确的
	// 如果是 SUCCESS，那么 Lb 表中的状态是正确的（在 CreateLoadBalancer 中已经被正确写入了），就不需要修改
	// 兜底
	if asyncResult.GetState().State == "FAILURE" {
		db.Model(&tables.Lb{}).Where("lb_id = ?", lbId).Update("LbState", "CreateFailed")
	}

	return
}
