package tables

import (
	"gorm.io/gorm"
)

// Lb 表，存储用户创建的 Lb 实例
type Lb struct {
	gorm.Model
	LbId    string `gorm:"column:lb_id"`
	UserId  string `gorm:"column:user_id"`
	LbState string `gorm:"column:lb_state"`
	// TaskId  string `gorm:"column:task_id"`
}

// Task 表，用户执行的各种异步任务（创建/删除 Lb 实例等）的信息
type Task struct {
	gorm.Model
	TaskId    string        `gorm:"column:task_id;primaryKey"`
	UserId    string        `gorm:"column:user_id"`
	TaskName  string        `gorm:"column:task_name"`
	TaskState string        `gorm:"column:task_state"`
	Result    []interface{} `gorm:"column:result;serializer:json"`
	Err       error         `gorm:"column:err;serializer:json"`
}
