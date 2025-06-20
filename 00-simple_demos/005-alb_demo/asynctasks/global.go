package asynctasks

import (
	"gorm.io/gorm"
)

var (
	db *gorm.DB // gorm 数据库连接实例
)

func Init(db_ *gorm.DB) {
	db = db_
}
