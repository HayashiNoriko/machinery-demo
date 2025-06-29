package ginhandlers

import (
	"demo/sourcecode/machinery/v2"

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
