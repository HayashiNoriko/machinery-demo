package common

import (
	"errors"
	"sync"

	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/retry"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type registeredTaskNames struct {
	sync.RWMutex // 读写锁，保证并发安全
	items        []string
}

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames registeredTaskNames
	retry               bool           // 是否启用重试机制。为 true 时，broker 会在消费消息失败时尝试重试
	retryFunc           func(chan int) // 重试逻辑的函数指针，负责实现具体的重试行为。参数为一个通道，用于接收停止信号
	retryStopChan       chan int       // 通知重试逻辑停止的通道。当需要停止重试时（如关闭 broker），会向该通道发送信号
	stopChan            chan int       // 通知 broker 停止消费消息的通道。关闭该通道后，消费协程会收到通知并停止工作
}

// NewBroker creates new Broker instance
func NewBroker(cnf *config.Config) Broker {
	return Broker{
		cnf:           cnf,
		retry:         true,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
	}
}

// GetConfig returns config
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

// GetRetry ...
func (b *Broker) GetRetry() bool {
	return b.retry
}

// GetRetryFunc ...
func (b *Broker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan ...
func (b *Broker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan ...
func (b *Broker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// 把当前所有已注册的任务名 names 同步给 broker，然后 broker 内部直接简单粗暴地赋值 items = names
// 这里其实还可以优化，让 broker 支持添加或移除单个任务，而不是每次都整体替换
// SetRegisteredTaskNames sets registered task names
func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *Broker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) {
	// 如果没有设置 retryFunc，那么 machinery 自动帮我们设置为 retry 包中的 Closure 方法
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *Broker) StopConsuming() {
	// 1. 禁用重试机制
	// Do not retry from now on
	b.retry = false

	// 2. 停止重试闭包
	// Stop the retry closure earlier
	select {

	// 尝试向重试停止通道发送停止信号
	case b.retryStopChan <- 1:
		log.WARNING.Print("Stopping retry closure.")

		// 如果上面的 case 要阻塞（没有接受者），那么就不执行它，而是来执行 default 分支，相当于放弃发送
		// 因此 default 分支可以防止阻塞主流程
	default:

	}

	// 3. 关闭主停止通道
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	log.WARNING.Print("Stop channel")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *Broker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}
