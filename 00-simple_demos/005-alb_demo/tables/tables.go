package tables

import (
	"gorm.io/gorm"
)

// Lb 表，存储用户创建的 Lb 实例
type Lb struct {
	gorm.Model
	LbId    string `gorm:"column:lb_id"`
	UserId  string `gorm:"column:user_id"`
	LbState string `gorm:"column:lb_state"` // Active、InActive、Provisioning、Configuring、CreateFailed
}

// Task 表，用户执行的各种异步任务（创建/删除 Lb 实例等）的信息
type Task struct {
	gorm.Model
	TaskId   string        `gorm:"column:task_id;primaryKey"`
	TaskName string        `gorm:"column:task_name"`
	State    string        `gorm:"column:state"` // 未执行、执行中、执行成功、执行失败（4 种）
	Results  []interface{} `gorm:"column:results;serializer:json"`
	Err      string        `gorm:"column:err;serializer:json"`
}
