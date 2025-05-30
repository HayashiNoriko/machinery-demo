package tasks

import (
	"fmt"
	"time"
)

// 自定义错误类型，用于表示“任务失败，但可以在一段时间后重试”
// ErrRetryTaskLater ...
type ErrRetryTaskLater struct {
	name, msg string        // name：任务名，msg：错误信息
	retryIn   time.Duration // 多久后可以重试
}

// 返回距离现在多久后可以重试任务
// RetryIn returns time.Duration from now when task should be retried
func (e ErrRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

// 实现了 Go 的 error 接口，可以作为普通错误使用
// Error implements the error interface
func (e ErrRetryTaskLater) Error() string {
	return fmt.Sprintf("Task error: %s Will retry in: %s", e.msg, e.retryIn)
}

// NewErrRetryTaskLater returns new ErrRetryTaskLater instance
func NewErrRetryTaskLater(msg string, retryIn time.Duration) ErrRetryTaskLater {
	return ErrRetryTaskLater{msg: msg, retryIn: retryIn}
}

// 只要实现了 RetryIn() 方法的错误类型，都被认为是“可重试错误”
// Retriable is interface that retriable errors should implement
type Retriable interface {
	RetryIn() time.Duration
}
