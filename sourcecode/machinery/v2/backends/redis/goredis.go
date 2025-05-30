package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"demo/sourcecode/machinery/v2/backends/iface"
	"demo/sourcecode/machinery/v2/common"
	"demo/sourcecode/machinery/v2/config"
	"demo/sourcecode/machinery/v2/log"
	"demo/sourcecode/machinery/v2/tasks"
)

// BackendGR represents a Redis result backend
type BackendGR struct {
	common.Backend                       // Backend 基类
	rclient        redis.UniversalClient // Redis 客户端实例，用于与 Redis 交互
	host           string                // Redis 主机地址（未实际使用，保留字段）
	password       string                // Redis 连接密码
	db             int                   // Redis 数据库编号
	socketPath     string                // Redis socket 文件路径（如设置则优先生效，覆盖 host）
	redsync        *redsync.Redsync      // Redsync 分布式锁实例，用于实现分布式互斥
	redisOnce      sync.Once             // 保证某些初始化操作只执行一次（如懒加载等场景）
}

// 创建并初始化一个 GoRedis backend 实例
// NewGR creates Backend instance
func NewGR(cnf *config.Config, addrs []string, db int) iface.Backend {
	// 1. 创建 BackendGR 实例
	b := &BackendGR{
		Backend: common.NewBackend(cnf),
	}

	// 2. 解析 Redis 地址和密码
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		// with passwrod
		b.password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1] // addr is the last one without @
	}

	// 3. 构造 Redis 连接参数
	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: b.password,
	}

	// 4. 补充高级配置
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}
	if cnf.TLSConfig != nil {
		ropt.TLSConfig = cnf.TLSConfig
	}
	// 哨兵密码
	if cnf.Redis != nil && cnf.Redis.SentinelPassword != "" {
		ropt.SentinelPassword = cnf.Redis.SentinelPassword
	}

	// 5. 根据集群模式选择客户端类型
	if cnf.Redis != nil && cnf.Redis.ClusterEnabled {
		b.rclient = redis.NewClusterClient(ropt.Cluster())
	} else {
		b.rclient = redis.NewUniversalClient(ropt)
	}

	// 6. 初始化 Redsync 分布式锁
	b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))

	return b
}

// 在 Redis 中创建并保存一个“任务组”的元数据对象
// InitGroup creates and saves a group meta data object
func (b *BackendGR) InitGroup(groupUUID string, taskUUIDs []string) error {
	// 1. 构造 GroupMeta 对象
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	// 2. 序列化为 JSON
	// 将 GroupMeta 对象编码为 JSON 字节流，便于存储到 Redis
	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	// 3. 获取过期时间
	expiration := b.getExpiration()

	// 4. 写入 Redis
	// 以 groupUUID 作为 key，将序列化后的数据写入 Redis，并设置过期时间
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// 判断某个任务组中的所有任务是否都已完成
// 【可优化】
// GroupCompleted returns true if all tasks in a group finished
func (b *BackendGR) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	// 1. 获取任务组元数据
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// 2. 获取所有子任务的状态
	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	// 3. 判断是否全部完成
	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// 获取某个任务组内所有子任务的状态
// GroupTaskStates returns states of all tasks in the group
func (b *BackendGR) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	// 1. 获取任务组元数据
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	// 2. 批量获取所有子任务的状态
	return b.getStates(groupMeta.TaskUUIDs...)
}

// 标记某个任务组的 chord（回调）是否已经被触发，确保 chord 只会被触发一次。
// 返回值为 true 表示本次可以触发，false 表示已经被触发过，不能再触发
// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *BackendGR) TriggerChord(groupUUID string) (bool, error) {
	// 1. 加分布式锁
	// 使用 Redsync 分布式锁，保证在分布式环境下同一时刻只有一个 worker 能执行 chord 触发逻辑，避免并发重复触发
	m := b.redsync.NewMutex("TriggerChordMutex")
	// 尝试获取锁，如果失败说明有其他 worker 正在执行，则本次不可执行，直接返回 false
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	// 2. 获取任务组元数据
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// 3. 判断是否已触发
	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// 4. 标记为已触发
	// Set flag to true
	groupMeta.ChordTriggered = true

	// 5. 更新元数据到 redis
	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

// 用于在更新任务状态前，将已存在的任务状态中的部分字段（如 CreatedAt 和 TaskName）合并到新的任务状态对象中，避免这些字段因状态变更而丢失
func (b *BackendGR) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *BackendGR) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *BackendGR) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *BackendGR) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *BackendGR) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *BackendGR) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *BackendGR) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// 根据任务的 UUID，从 Redis 中获取并返回该任务的最新状态（TaskState）
// GetState returns the latest task state
func (b *BackendGR) GetState(taskUUID string) (*tasks.TaskState, error) {

	// 1. 从 Redis获取数据（未解码的 JSON 字节流）
	item, err := b.rclient.Get(context.Background(), taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	// 2. 反序列化为 TaskState 对象
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// 删除任务状态
// PurgeState deletes stored task state
func (b *BackendGR) PurgeState(taskUUID string) error {
	err := b.rclient.Del(context.Background(), taskUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// 删除组的元数据
// PurgeGroupMeta deletes stored group meta data
func (b *BackendGR) PurgeGroupMeta(groupUUID string) error {
	err := b.rclient.Del(context.Background(), groupUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// 根据组的 UUID，从 Redis 中获取并返回该组的元数据（GroupMeta）
// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *BackendGR) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.rclient.Get(context.Background(), groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// 根据多个task 的 UUID，从 redis 中获取多个任务状态
// getStates returns multiple task states
func (b *BackendGR) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(context.Background(), func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(context.Background(), uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			log.ERROR.Print(err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// 更新 redis 中的某个 taskState
// updateState saves current task state
func (b *BackendGR) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *BackendGR) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}
