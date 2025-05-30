package result

import (
	"errors"
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v2/backends/iface"
	"github.com/RichardKnop/machinery/v2/tasks"
)

var (
	// ErrBackendNotConfigured ...
	ErrBackendNotConfigured = errors.New("Result backend not configured")
	// ErrTimeoutReached ...
	ErrTimeoutReached = errors.New("Timeout reached")
)

// AsyncResult represents a task result
type AsyncResult struct {
	Signature *tasks.Signature
	taskState *tasks.TaskState
	backend   iface.Backend
}

// ChordAsyncResult represents a result of a chord
type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           iface.Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      iface.Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(signature *tasks.Signature, backend iface.Backend) *AsyncResult {
	return &AsyncResult{
		Signature: signature,
		taskState: new(tasks.TaskState), // 空结构体
		backend:   backend,
	}
}

// NewChordAsyncResult creates ChordAsyncResult instance
func NewChordAsyncResult(groupTasks []*tasks.Signature, chordCallback *tasks.Signature, backend iface.Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(tasks []*tasks.Signature, backend iface.Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChainAsyncResult{
		asyncResults: asyncResults,
		backend:      backend,
	}
}

// 刷新并返回任务状态，并根据当前状态决定是否返回结果或错误
// 它是一个非阻塞的状态探测方法，常用于轮询任务执行进度
// Touch the state and don't wait
func (asyncResult *AsyncResult) Touch() ([]reflect.Value, error) {
	// 1. 检查 backend 是否配置
	if asyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	// 2. 刷新任务状态
	asyncResult.GetState()

	// 3. AMQP 后端下的状态清理
	// 如果使用的是 AMQP 后端，并且任务已完成，则调用 PurgeState 删除该任务的状态，释放资源
	// Purge state if we are using AMQP backend
	if asyncResult.backend.IsAMQP() && asyncResult.taskState.IsCompleted() {
		asyncResult.backend.PurgeState(asyncResult.taskState.TaskUUID)
	}

	// 4. 根据任务状态返回不同结果
	// 如果任务失败，则返回错误
	if asyncResult.taskState.IsFailure() {
		return nil, errors.New(asyncResult.taskState.Error)
	}
	// 如果任务成功，将结果反射为[]reflect.Value
	if asyncResult.taskState.IsSuccess() {
		return tasks.ReflectTaskResults(asyncResult.taskState.Results)
	}
	// 如果任务还未完成，返回 nil，便于上层轮询等待
	return nil, nil
}

// 死循环，每隔一段时间去调用 Touch，直到返回值不为 nil
// Get returns task results (synchronous blocking call)
func (asyncResult *AsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	for {
		results, err := asyncResult.Touch()

		if results == nil && err == nil {
			time.Sleep(sleepDuration)
		} else {
			return results, err
		}
	}
}

// 比 Get 多了一个 timeout，因此不会死循环，调用 Touch 的同时检测 timeout 是否达到，如果达到了，返回 ErrTimeoutReached 错误
// GetWithTimeout returns task results with a timeout (synchronous blocking call)
func (asyncResult *AsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	timeout := time.NewTimer(timeoutDuration)

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			results, err := asyncResult.Touch()

			if results == nil && err == nil {
				time.Sleep(sleepDuration)
			} else {
				return results, err
			}
		}
	}
}

// 从数据库中获取最新任务状态，据此刷新 asyncResult 中的任务状态，并返回
// GetState returns latest task state
func (asyncResult *AsyncResult) GetState() *tasks.TaskState {
	if asyncResult.taskState.IsCompleted() {
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(asyncResult.Signature.UUID)
	if err == nil {
		asyncResult.taskState = taskState
	}

	return asyncResult.taskState
}

// 遍历 chainAsyncResult 中的每一个 asyncResult，按顺序依次调用 Get()，一个一个执行
// 返回最后一个 asyncResult 的结果
// Get returns results of a chain of tasks (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	for _, asyncResult := range chainAsyncResult.asyncResults {
		results, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return results, err
}

// 遍历 chordAsyncResult 中的每一个 asyncResult，按顺序依次调用 Get()，一个一个执行
// 最后再调用 callback 的 Get，并返回这个结果
// Get returns result of a chord (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var err error
	for _, asyncResult := range chordAsyncResult.groupAsyncResults {
		_, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return chordAsyncResult.chordAsyncResult.Get(sleepDuration)
}

// GetWithTimeout returns results of a chain of tasks with timeout (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	ln := len(chainAsyncResult.asyncResults)
	lastResult := chainAsyncResult.asyncResults[ln-1]

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:

			// touch 全部 asyncResult
			for _, asyncResult := range chainAsyncResult.asyncResults {
				_, err = asyncResult.Touch()
				if err != nil {
					return nil, err
				}
			}

			// touch 最后一个 asyncResult
			results, err = lastResult.Touch()
			if err != nil {
				return nil, err
			}
			// 如果 result 不为空，则说明最后一个任务执行成功，也证明前面的任务都执行成功了，则可以直接返回
			if results != nil {
				return results, err
			}
			// 否则继续等待
			time.Sleep(sleepDuration)
		}
	}
}

// 跟 chain 的区别是，chord 的最后一个 task 是 chord callback，其他流程相同
// GetWithTimeout returns result of a chord with a timeout (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			for _, asyncResult := range chordAsyncResult.groupAsyncResults {
				_, errcur := asyncResult.Touch()
				if errcur != nil {
					return nil, err
				}
			}

			results, err = chordAsyncResult.chordAsyncResult.Touch()
			if err != nil {
				return nil, nil
			}
			if results != nil {
				return results, err
			}
			time.Sleep(sleepDuration)
		}
	}
}
