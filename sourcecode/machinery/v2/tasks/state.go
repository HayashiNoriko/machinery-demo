package tasks

import "time"

const (
	// StatePending - initial state of a task
	StatePending = "PENDING"
	// StateReceived - when task is received by a worker
	StateReceived = "RECEIVED"
	// StateStarted - when the worker starts processing the task
	StateStarted = "STARTED"
	// StateRetry - when failed task has been scheduled for retry
	StateRetry = "RETRY"
	// StateSuccess - when the task is processed successfully
	StateSuccess = "SUCCESS"
	// StateFailure - when processing of the task fails
	StateFailure = "FAILURE"
)

// TaskState represents a state of a task
type TaskState struct {
	TaskUUID  string        `bson:"_id"`           // 任务的唯一标识符（UUID），用于唯一标识一个任务实例，等于 signature.UUID
	TaskName  string        `bson:"task_name"`     // 任务名称（即 Signature.Name），标识任务类型
	State     string        `bson:"state"`         // 当前任务的状态
	Results   []*TaskResult `bson:"results"`       // 任务执行结果（切片），也就是任务的返回值
	Error     string        `bson:"error"`         // 错误信息字符串，任务执行失败时记录具体的错误内容
	CreatedAt time.Time     `bson:"created_at"`    // 该实例的创建时间，通常用于记录任务进入当前状态的时间点
	TTL       int64         `bson:"ttl,omitempty"` // 该实例的存活时间（秒），可选字段，用于状态过期自动清理
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupUUID      string    `bson:"_id"`             // 任务组的唯一标识符
	TaskUUIDs      []string  `bson:"task_uuids"`      // 该组内所有任务的 UUID 列表。可以用来判断组内哪些任务已完成，哪些还在执行
	ChordTriggered bool      `bson:"chord_triggered"` // 是否已触发 chord 回调。chord 是指一组任务全部完成后自动执行的回调任务，这个字段用于防止重复触发
	Lock           bool      `bson:"lock"`            // 分布式锁标记，防止并发环境下多 worker 同时处理同一个 group 的状态变更或回调触发
	CreatedAt      time.Time `bson:"created_at"`      // 该实例的创建时间
	TTL            int64     `bson:"ttl,omitempty"`   // 该实例的存活时间（秒），用于自动过期清理
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID:  signature.UUID,
		TaskName:  signature.Name,
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateReceived,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateStarted,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(signature *Signature, results []*TaskResult) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateSuccess,
		Results:  results,
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(signature *Signature, err string) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateFailure,
		Error:    err,
	}
}

// NewRetryTaskState ...
func NewRetryTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StateRetry,
	}
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure()
}

// IsSuccess returns true if state is SUCCESS
func (taskState *TaskState) IsSuccess() bool {
	return taskState.State == StateSuccess
}

// IsFailure returns true if state is FAILURE
func (taskState *TaskState) IsFailure() bool {
	return taskState.State == StateFailure
}
