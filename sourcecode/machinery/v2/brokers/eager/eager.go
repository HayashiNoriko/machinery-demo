package eager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"demo/sourcecode/machinery/v2/brokers/iface"
	"demo/sourcecode/machinery/v2/common"
	"demo/sourcecode/machinery/v2/tasks"
)

// Broker represents an "eager" in-memory broker
type Broker struct {
	worker iface.TaskProcessor
	common.Broker
}

// New creates new Broker instance
func New() iface.Broker {
	return new(Broker)
}

// Mode interface with methods specific for this broker
type Mode interface {
	AssignWorker(p iface.TaskProcessor)
}

// eager 模式下没有实际的消费循环，直接返回 true, nil，表示“已启动”
// StartConsuming enters a loop and waits for incoming messages
func (eagerBroker *Broker) StartConsuming(consumerTag string, concurrency int, p iface.TaskProcessor) (bool, error) {
	return true, nil
}

// eager 模式下没有实际的消费过程，所以这里是空实现
// StopConsuming quits the loop
func (eagerBroker *Broker) StopConsuming() {
	// do nothing
}

// 发布即执行
// Publish places a new message on the default queue
func (eagerBroker *Broker) Publish(ctx context.Context, task *tasks.Signature) error {
	// 1. 检查 worker 是否已分配，如果没有则报错
	if eagerBroker.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}

	// 2. 将任务序列化为 JSON，再反序列化回来（模拟消息队列的行为）
	// faking the behavior to marshal input into json
	// and unmarshal it back
	message, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return fmt.Errorf("JSON unmarshal error: %s", err)
	}

	// 直接调用 worker 的 Process 方法同步处理任务（本地直接执行）
	// blocking call to the task directly
	return eagerBroker.worker.Process(signature)
}

// 分配 worker 给 broker
// AssignWorker assigns a worker to the eager broker
func (eagerBroker *Broker) AssignWorker(w iface.TaskProcessor) {
	eagerBroker.worker = w
}
