package eager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"demo/sourcecode/machinery/v2/backends/iface"
	"demo/sourcecode/machinery/v2/common"
	"demo/sourcecode/machinery/v2/config"
	"demo/sourcecode/machinery/v2/tasks"
)

// ErrGroupNotFound ...
type ErrGroupNotFound struct {
	groupUUID string
}

// NewErrGroupNotFound returns new instance of ErrGroupNotFound
func NewErrGroupNotFound(groupUUID string) ErrGroupNotFound {
	return ErrGroupNotFound{groupUUID: groupUUID}
}

// Error implements error interface
func (e ErrGroupNotFound) Error() string {
	return fmt.Sprintf("Group not found: %v", e.groupUUID)
}

// ErrTasknotFound ...
type ErrTasknotFound struct {
	taskUUID string
}

// NewErrTasknotFound returns new instance of ErrTasknotFound
func NewErrTasknotFound(taskUUID string) ErrTasknotFound {
	return ErrTasknotFound{taskUUID: taskUUID}
}

// Error implements error interface
func (e ErrTasknotFound) Error() string {
	return fmt.Sprintf("Task not found: %v", e.taskUUID)
}

// Backend represents an "eager" in-memory result backend
type Backend struct {
	common.Backend
	groups     map[string][]string // 记录 groupUUID 到该组所有 taskUUID 的映射
	tasks      map[string][]byte   // 记录 taskUUID 到任务状态（序列化后的 JSON）的映射
	stateMutex sync.Mutex
}

// New creates EagerBackend instance
func New() iface.Backend {
	return &Backend{
		Backend: common.NewBackend(new(config.Config)),
		groups:  make(map[string][]string),
		tasks:   make(map[string][]byte),
	}
}

// 初始化一个任务组，插入到 backend.groups 中
// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	tasks := make([]string, 0, len(taskUUIDs))
	// copy every task
	tasks = append(tasks, taskUUIDs...)

	b.groups[groupUUID] = tasks
	return nil
}

// 判断一个 group 下的所有任务是否都已完成（遍历所有 taskUUID，检查状态）
// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks, ok := b.groups[groupUUID]
	if !ok {
		return false, NewErrGroupNotFound(groupUUID)
	}

	var countSuccessTasks = 0
	for _, v := range tasks {
		t, err := b.GetState(v)
		if err != nil {
			return false, err
		}

		if t.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// 获取一个 group 下所有任务的状态，返回 TaskState 切片
// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	taskUUIDs, ok := b.groups[groupUUID]
	if !ok {
		return nil, NewErrGroupNotFound(groupUUID)
	}

	ret := make([]*tasks.TaskState, 0, groupTaskCount)
	for _, taskUUID := range taskUUIDs {
		t, err := b.GetState(taskUUID)
		if err != nil {
			return nil, err
		}

		ret = append(ret, t)
	}

	return ret, nil
}

// 标记 chord 是否应该被触发。eager 模式下直接返回 true, nil，即总是允许触发
// 虽然总是返回 true，但实际上在 worker 中，只有在 group 中所有任务都完成时，才会来判断这个方法
// 也就是说只有在 group 中最后一个任务完成时，才会来判断这个方法，直接返回 true，那么就可以触发 chord 回调，且只会触发一次
// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	state := tasks.NewPendingTaskState(signature)
	return b.updateState(state)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	state := tasks.NewReceivedTaskState(signature)
	return b.updateState(state)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	state := tasks.NewStartedTaskState(signature)
	return b.updateState(state)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	state := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(state)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	state := tasks.NewFailureTaskState(signature, err)
	return b.updateState(state)
}

// 获取指定 taskUUID 的任务状态
// 先从 tasks map 取出序列化的状态数据，再反序列化为 TaskState。
// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	tasktStateBytes, ok := b.tasks[taskUUID]
	if !ok {
		return nil, NewErrTasknotFound(taskUUID)
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(tasktStateBytes))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal task state %v", b)
	}

	return state, nil
}

// 删除指定 taskUUID 的任务状态
// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	_, ok := b.tasks[taskUUID]
	if !ok {
		return NewErrTasknotFound(taskUUID)
	}

	delete(b.tasks, taskUUID)
	return nil
}

// 删除 group 的元数据（即从 groups map 中删除该 group）
// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	_, ok := b.groups[groupUUID]
	if !ok {
		return NewErrGroupNotFound(groupUUID)
	}

	delete(b.groups, groupUUID)
	return nil
}

// 内部方法，把任务状态序列化为 JSON 并存入 tasks map
// 用 stateMutex 保证并发安全
func (b *Backend) updateState(s *tasks.TaskState) error {
	// simulate the behavior of json marshal/unmarshal
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()
	msg, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("Marshal task state error: %v", err)
	}

	b.tasks[s.TaskUUID] = msg
	return nil
}
