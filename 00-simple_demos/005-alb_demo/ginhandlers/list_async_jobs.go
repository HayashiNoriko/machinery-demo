package ginhandlers

import (
	"demo/00-simple_demos/005-alb_demo/tables"

	"github.com/gin-gonic/gin"
)

func ListAsyncJobs(c *gin.Context) {
	// 1. 获取参数
	taskIds := c.QueryArray("taskIds")
	if len(taskIds) == 0 {
		c.JSON(400, gin.H{
			"message": "taskIds 为空",
		})
		return
	}

	// 2. 查询所有 taskIds 对应的任务
	var tasks []tables.Task
	if err := db.Where("task_id IN ?", taskIds).Find(&tasks).Error; err != nil {
		c.JSON(500, gin.H{
			"message": "数据库查询失败",
			"error":   err.Error(),
		})
		return
	}

	// 3. 返回 tasks
	c.JSON(200, gin.H{
		"message":    "ok",
		"asyncTasks": tasks,
	})
}
