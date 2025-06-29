package ginhandlers

import (
	"demo/00-simple_demos/005-alb_demo/tables"
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

	// 2. 立即生成 lbId，插入 LB 表中，返回给用户
	// 用户可以从 mysql 中立即查询到
	lbId := "alb_" + uuid.New().String()
	lb := &tables.Lb{
		LbId:    lbId,
		UserId:  userId,
		LbState: "Provisioning",
	}
	db.Create(lb)

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

	_, err := mserver.SendTask(signature) // signature 的 UUID 会被修改
	if err != nil {
		fmt.Println(err)
		c.JSON(500, gin.H{
			"message": "服务器发送异步任务失败",
		})
		return
	}

	// 4. 返回 lbId 和 taskId
	c.JSON(200, gin.H{
		"message": "ok",
		"lbId":    lbId,
		"taskId":  signature.UUID,
	})
}
