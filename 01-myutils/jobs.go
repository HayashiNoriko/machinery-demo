package myutils

import (
	"fmt"
	"os"
	"time"
)

// 定义一个普通任务
func Add(args ...int64) (int64, error) {
	fmt.Println("---执行 Add 方法---")
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	fmt.Println("===Add 方法执行结束===")
	return sum, nil
}

// 定义一个周期性任务
func Periodic() error {
	fmt.Println("---执行某个周期性任务---")
	time.Sleep(1 * time.Second)
	fmt.Println("===周期性任务执行结束===")
	return nil
}

// 定义一个打印任务
func Print(filename, msg string) error {
	file, _ := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	defer file.Close()

	file.WriteString(msg + "\n")

	time.Sleep(1 * time.Second)

	return nil
}
