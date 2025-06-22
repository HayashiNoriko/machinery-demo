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

func insertTask(asyncResult *result.AsyncResult) error {
	// 定时阻塞，从 backend 中获取结果（等待异步任务完成）
	// 具体的超时时间（第一个参数），需要谨慎设置
	res, err := asyncResult.GetWithTimeout(time.Hour, time.Second)

	// GetWithTimeout 返回错误，有两种情况
	// 一种是 ErrTimeoutReached，超时了（第一个参数设置的值）
	// 另一种是任务 FAILURE 了（第二个参数设置的值），err 保存任务的错误信息
	if err == result.ErrTimeoutReached {
		// 如果是超时，那么可以检查一下任务状态
		// _, err := mserver.GetBackend().GetState(asyncResult.Signature.UUID)
		// 在真实的业务场景下，需要根据不同的 err 处理不同的情况（backend 的 rclient 出错、json 反序列化出错。。。等等等等）
		// 模拟 demo 中就不再详细判断了
		return err
	}

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

	return nil
}
