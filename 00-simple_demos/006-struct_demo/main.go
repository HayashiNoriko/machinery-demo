// 支持自定义 struct 类型

package main

import (
	"fmt"
	"os"
	"time"

	myutils "demo/01-myutils"
	"demo/sourcecode/machinery/v2/tasks"
)

// 先输入 worker，启动 worker 消费者
// 再不加参数，启动生产者
func main() {
	server := myutils.MyServer()
	server.RegisterTask("Task", Task)
	tasks.RegisterType("Person", Person{})
	tasks.RegisterType("[]Person", []Person{{}})

	// 启动 worker 的情况
	if len(os.Args) == 2 && os.Args[1] == "worker" { // 启动worker
		worker := server.NewWorker("my_worker", 0)
		err := worker.Launch()
		panic(err)
	}

	// 以下是启动生产者的情况

	signature := &tasks.Signature{
		Name: "Task",
		Args: []tasks.Arg{
			{
				Type:  "Person",
				Value: Person{Name: "John", Age: 20},
			},
		},
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		panic(err)
	}

	results, err := asyncResult.Get(time.Millisecond * 200)
	if err != nil {
		panic(err)
	}

	fmt.Printf("main 中, results=%+v\n", results[0])

	time.Sleep(time.Second * 3600)
}

type Person struct {
	Name string
	Age  int
}

// 自定义 Task，参数是 struct
func Task(person Person) (Person, error) {
	fmt.Println("进入Task方法...")
	fmt.Printf("收到了 person 参数：%v \n", person)
	fmt.Println("离开Task方法...")
	return person, nil
}
