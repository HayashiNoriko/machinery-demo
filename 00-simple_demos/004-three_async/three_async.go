package three_async

import (
	"container/list"
	"demo/00-simple_demos/jobs"
	"sync"
)

var taskN int = 3000000
var workerN int = 1000

// 第一种，模拟 fork 子进程
// 异步结果
type AsyncResult struct {
	status bool
	res    int64
}

// 异步调用 Add
func asyncAdd(ar *AsyncResult, args ...int64) {
	res, _ := jobs.Add(args...)
	ar.status = true
	ar.res = res
}

func fork() {
	signalWG := sync.WaitGroup{}
	workers := make(chan struct{}, workerN)
	for i := 0; i < workerN; i++ {
		workers <- struct{}{}
	}

	for i := 0; i < taskN; i++ {

		// 生产
		signalWG.Add(1)
		ar := &AsyncResult{false, 0}
		// 消费
		go asyncAdd(ar, 1, 2, 3)
		go func(ar *AsyncResult) {
			for {
				if ar.status {
					<-workers

					// fmt.Println("Add 异步调用结果：", ar.res)
					signalWG.Done()

					workers <- struct{}{}

					break
				}
			}
		}(ar)

	}

	signalWG.Wait()
}

// 第二种，使用消息队列
type Task struct {
	taskFunc func(args ...int64) (int64, error)
	args     []int64
}

func mq() {
	deliveries := make(chan *Task, 1000)
	workers := make(chan struct{}, workerN)
	for i := 0; i < workerN; i++ {
		workers <- struct{}{}
	}

	signalWG := sync.WaitGroup{}

	// 生产者
	go func() {
		for i := 0; i < taskN; i++ {
			task := &Task{
				taskFunc: jobs.Add,
				args:     []int64{1, 2, 3},
			}

			signalWG.Add(1)

			deliveries <- task
		}
		close(deliveries) // 生产完毕后关闭channel
	}()

	// 消费者
	// 当 channel 为空且关闭时，range 才会退出
	for task := range deliveries {
		<-workers
		go func() {
			task.taskFunc(task.args...)
			// fmt.Println("Add 异步调用结果：", res)
			signalWG.Done()
			workers <- struct{}{}
		}()
	}

	signalWG.Wait()

}

// 第三种，使用通知
func notify() {
	signalWG := sync.WaitGroup{}
	workers := make(chan struct{}, workerN)
	for i := 0; i < workerN; i++ {
		workers <- struct{}{}
	}

	notifyChan := make(chan struct{}, 1000)
	tasks := list.New()
	mu := sync.Mutex{}

	// 生产者
	go func() {
		for i := 0; i < taskN; i++ {
			mu.Lock()

			task := &Task{
				taskFunc: jobs.Add,
				args:     []int64{1, 2, 3},
			}

			tasks.PushBack(task)
			signalWG.Add(1)

			mu.Unlock()
			notifyChan <- struct{}{}
		}
		close(notifyChan)
	}()

	// 消费者
	for range notifyChan {
		mu.Lock()

		taskElem := tasks.Front()
		tasks.Remove(taskElem)
		task := taskElem.Value.(*Task)

		<-workers
		go func(task *Task) {
			task.taskFunc(task.args...)
			signalWG.Done()
			workers <- struct{}{}
		}(task)

		mu.Unlock()

	}

	signalWG.Wait()
}
