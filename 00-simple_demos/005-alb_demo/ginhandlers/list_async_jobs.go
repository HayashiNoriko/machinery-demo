package ginhandlers

import (
	"demo/00-simple_demos/005-alb_demo/tables"

	taskspkg "demo/sourcecode/machinery/v2/tasks"

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

	// 2. 遍历 taskIds，去 mysql 和 redis 中查
	tasks := make([]tables.Task, len(taskIds))
	for index, taskId := range taskIds {
		// 先去 mysql 中查
		db.Where("task_id = ?", taskId).First(&tasks[index])

		// mysql 中没有查到，去 redis 中查
		if tasks[index].TaskId == "" {
			redisTask, err := mserver.GetBackend().GetState(taskId)
			if err != nil {
				c.JSON(500, gin.H{
					"message": "查询 redis 失败",
				})
				return
			}

			// []TaskResult ---> []reflect.Value
			reflectValues, err := taskspkg.ReflectTaskResults(redisTask.Results)
			if err != nil {
				c.JSON(500, gin.H{
					"message": "转换 redisTask.Results 失败",
				})
				return
			}

			// []reflect.Value ---> []interface{}
			results := make([]interface{}, len(redisTask.Results))
			for i, reflectValue := range reflectValues {
				results[i] = reflectValue.Interface()
			}

			tasks[index] = tables.Task{
				TaskId:     redisTask.TaskUUID,
				TaskName:   redisTask.TaskName,
				TaskState:  redisTask.State,
				Results:    results,
				Err:        redisTask.Error,
				CreateTime: redisTask.CreatedAt.UnixMilli(),
			}
		}
	}

	// 3. 返回 tasks
	c.JSON(200, gin.H{
		"message":    "ok",
		"asyncTasks": tasks,
	})
}
