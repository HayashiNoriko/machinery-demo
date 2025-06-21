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

func insertTask(asyncResult *result.AsyncResult) {
	// 阻塞，从 asyncResult 中获取结果（等待异步任务完成）
	res, err := asyncResult.Get(time.Second)

	// []reflect.Value ---> []interface{}
	results := make([]interface{}, len(res))
	for i, v := range res {
		results[i] = v.Interface()
	}

	// 新建一个 Task 行
	errString := ""
	if err != nil {
		errString = err.Error()
	}
	task := tables.Task{
		TaskId:     asyncResult.Signature.UUID,
		TaskName:   asyncResult.Signature.Name,
		TaskState:  asyncResult.GetState().State,
		Results:    results,
		Err:        errString,
		CreateTime: asyncResult.GetState().CreatedAt.UnixMilli(),
	}

	// 添加这个 Task 行
	db.Create(&task)
}
