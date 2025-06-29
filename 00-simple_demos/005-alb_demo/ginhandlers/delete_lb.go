package ginhandlers

import (
	"demo/sourcecode/machinery/v2/tasks"
	"fmt"

	"github.com/gin-gonic/gin"
)

// 用户删除一台实例，返回 taskId
// 用户后续可以通过 lbId 查询实例状态，也可以通过 taskId 查询异步任务状态
func DeleteLoadBalancer(c *gin.Context) {
	// 1. 获取参数
	userId := c.Query("userId")
	if userId == "" {
		c.JSON(400, gin.H{
			"message": "userId 为空",
		})
		return
	}
	lbId := c.Query("lbId")
	if lbId == "" {
		c.JSON(400, gin.H{
			"message": "lbId 为空",
		})
		return
	}

	// 2. 发送 DeleteLoadBalancer 任务给 machinery
	signature := &tasks.Signature{
		Name: "DeleteLoadBalancer",
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

	_, err := mserver.SendTask(signature)
	if err != nil {
		fmt.Println(err)
		c.JSON(500, gin.H{
			"message": "服务器发送异步任务失败",
		})
		return
	}

	// 3. 返回 taskId
	taskId := signature.UUID
	c.JSON(200, gin.H{
		"message": "ok",
		"taskId":  taskId,
	})
}
