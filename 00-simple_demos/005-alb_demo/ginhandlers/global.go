package ginhandlers

import (
	"demo/00-simple_demos/005-alb_demo/tables"
	"demo/sourcecode/machinery/v2"
	"demo/sourcecode/machinery/v2/backends/result"
	"time"

	"gorm.io/gorm"
)

var (
	db      *gorm.DB          // gorm 数据库连接实例
	mserver *machinery.Server // machinery 服务器实例
)

func Init(mserver_ *machinery.Server, db_ *gorm.DB) {
	mserver = mserver_
	db = db_
}

func insertTask(asyncResult *result.AsyncResult, userId string) {
	// 阻塞，从 asyncResult 中获取结果（等待异步任务完成）
	res, err := asyncResult.Get(time.Second)

	// 常规做法，虽然这里 res 为空，但是其他异步任务的 res 不一定为空
	result := make([]interface{}, len(res))
	for i, v := range res {
		result[i] = v.Interface()
	}

	// 新建一个 Task 行
	task := tables.Task{
		TaskId:    asyncResult.Signature.UUID,
		UserId:    userId,
		TaskName:  asyncResult.Signature.Name,
		TaskState: asyncResult.GetState().State,
		Result:    result,
		Err:       err,
	}

	// 添加这个 Task 行
	db.Create(&task)
}
