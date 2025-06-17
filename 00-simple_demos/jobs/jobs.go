package jobs

import (
	"fmt"
	// "time"
)

// 定义一个普通任务
func Add(args ...int64) (int64, error) {
	// fmt.Println("执行 Add 方法")
	sum := int64(0)
	// for _, arg := range args {
	// 	sum += arg
	// }
	// fmt.Println("Add 方法执行结束")
	return sum, nil
}

// 定义一个周期性任务
func Periodic() error {
	fmt.Println("执行某个周期性任务")
	return nil
}
