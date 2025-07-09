// 自己实现的 MySQL 版本的 backend

// 注意：
// 1、过期自动清理（本文没有实现，实现思路如下）：
// mysql 本身不能像 redis 一样直接给某条记录设置过期时间，只能结合 created_at 和 ttl 字段来判断某条记录是否过期
// 因此需要在插入时就指定 created_at 和 ttl 这两个字段，以及我们定时执行一个自动清理的 SQL：
// DELETE FROM group_meta WHERE ttl IS NOT NULL AND created_at < NOW() - INTERVAL ttl SECOND;

// 2、json 反序列化：依然用 json.Decoder，和源码保持一致；不要用 json.Unmarshal，因为解析出来的数值会被默认转换成 float64，导致反射类型转换失败

// 3、分布式锁：直接使用 GroupMeta 的 Lock 字段

// 4、状态更新方法（SetStateXXX)：PENDING 会先查找是否存在，不存在就插入行，存在就修改行。其他 5 个直接修改记录
package backend

import (
	"bytes"
	"context"
	"database/sql"
	"demo/sourcecode/machinery/v2/backends/iface"
	"demo/sourcecode/machinery/v2/common"
	"demo/sourcecode/machinery/v2/config"
	"demo/sourcecode/machinery/v2/tasks"
	"encoding/json"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// BackendMS 表示一个用 MySQL 做后端的 backend 实例
type BackendMS struct {
	common.Backend         // Backend 基类
	db             *sql.DB // sql.DB 实例，用于与 MySQL 交互
}

// addr = root:Root123456!@tcp(127.0.0.1:3306)
// dbname = testdb
func New(cnf *config.Config, addr string, dbname string) iface.Backend {
	// 1. 创建父类
	b := &BackendMS{
		Backend: common.NewBackend(cnf),
	}

	// 2. 创建连接
	dsn := addr + "/" + dbname + "?" +
		"charset=utf8mb4&" +
		"parseTime=True&" + // 将数据库时间解析为 time.Time
		"loc=Local&" + // 使用本地时区
		"timeout=5s&" + // 连接超时
		"readTimeout=10s&" + // 读超时
		"writeTimeout=10s" // 写超时

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil
	}

	// 3. 连接池配置
	db.SetMaxOpenConns(20)                 //设置数据库的最大打开连接数
	db.SetMaxIdleConns(5)                  // 设置连接池中的最大空闲连接数
	db.SetConnMaxLifetime(5 * time.Minute) // 设置连接的最大存活时间
	db.SetConnMaxIdleTime(1 * time.Minute) // 设置连接在池中的最大空闲时间

	// 4. 验证连接是否有效
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil
	}

	// 返回
	b.db = db
	return b
}

// 在 mysql 中创建并保存一个“任务组”的元数据对象
func (b *BackendMS) InitGroup(groupUUID string, taskUUIDs []string) error {
	// 1. 构造 GroupMeta 对象
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	// 2. taskUUIDs 序列化为 JSON
	taskUUIDsJSON, err := json.Marshal(groupMeta.TaskUUIDs)
	if err != nil {
		return err
	}

	// 3. 插入到 MySQL
	_, err = b.db.Exec(`
        INSERT INTO group_meta 
            (group_uuid, task_uuids, chord_triggered, `+"`lock`"+`, created_at, ttl)
        VALUES (?, ?, ?, ?, ?, ?)
    `, groupMeta.GroupUUID, string(taskUUIDsJSON), false, false, groupMeta.CreatedAt, nil)
	if err != nil {
		return err
	}

	return nil
}

// 判断某个任务组中的所有任务是否都已完成
func (b *BackendMS) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	// 1. 获取任务组元数据
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}
	if groupMeta == nil {
		return false, nil // 未找到该组
	}

	// 2. 获取所有子任务的状态
	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	// 3. 判断是否全部完成
	for _, taskState := range taskStates {
		if taskState != nil && !taskState.IsCompleted() {
			return false, nil
		}
	}

	return true, nil
}

// 获取某个任务组内所有子任务的状态
func (b *BackendMS) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
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
func (b *BackendMS) TriggerChord(groupUUID string) (bool, error) {
	// 1. 获取任务组元数据
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}
	if groupMeta == nil {
		return false, sql.ErrNoRows
	}

	// 2. 判断是否已触发，或者已上锁（正在被其他 worker 触发）
	if groupMeta.ChordTriggered || groupMeta.Lock {
		return false, nil
	}

	// 现在既没有触发，也没有上锁，可以尝试加锁

	// 3. 先尝试加锁（只允许 Lock=false 时才能加锁成功）
	res, err := b.db.Exec(`
        UPDATE group_meta SET `+"`lock`"+` = true
        WHERE group_uuid = ? AND `+"`lock`"+` = false
    `, groupUUID)
	if err != nil {
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if rowsAffected == 0 {
		// 加锁失败，说明已经被其他 worker 加锁
		return false, nil
	}

	// 现在加锁成功了，defer 解锁操作
	defer b.db.Exec("UPDATE group_meta SET `lock` = false WHERE group_uuid = ?", groupUUID)

	// 4. 标记为已触发
	_, err = b.db.Exec(`
        UPDATE group_meta SET chord_triggered = true
        WHERE group_uuid = ?
    `, groupUUID)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *BackendMS) SetStatePending(signature *tasks.Signature) error {
	// 1. 尝试插入新任务
	res, err := b.db.Exec(`
        INSERT IGNORE INTO task_state
            (task_uuid, task_name, state, created_at)
        VALUES (?, ?, ?, ?)
    `, signature.UUID, signature.Name, "PENDING", time.Now().UTC())
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		// 已存在，只更新 state 字段，不更新 created_at
		_, err = b.db.Exec(`
            UPDATE task_state SET state = ? WHERE task_uuid = ?
        `, "PENDING", signature.UUID)
		if err != nil {
			return err
		}
	}
	return nil
}
func (b *BackendMS) SetStateReceived(signature *tasks.Signature) error {
	_, err := b.db.Exec(`
        UPDATE task_state SET state = ? WHERE task_uuid = ?
    `, "RECEIVED", signature.UUID)
	return err
}
func (b *BackendMS) SetStateStarted(signature *tasks.Signature) error {
	_, err := b.db.Exec(`
        UPDATE task_state SET state = ? WHERE task_uuid = ?
    `, "STARTED", signature.UUID)
	return err
}
func (b *BackendMS) SetStateRetry(signature *tasks.Signature) error {
	_, err := b.db.Exec(`
        UPDATE task_state SET state = ? WHERE task_uuid = ?
    `, "RETRY", signature.UUID)
	return err
}
func (b *BackendMS) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	// 序列化 results
	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return err
	}
	_, err = b.db.Exec(`
        UPDATE task_state
        SET state = ?, results = ?, error = NULL
        WHERE task_uuid = ?
    `, "SUCCESS", string(resultsJSON), signature.UUID)
	return err
}
func (b *BackendMS) SetStateFailure(signature *tasks.Signature, errMsg string) error {
	_, err := b.db.Exec(`
        UPDATE task_state
        SET state = ?, error = ?, results = NULL
        WHERE task_uuid = ?
    `, "FAILURE", errMsg, signature.UUID)
	return err
}

// 根据任务的 UUID，从 Redis 中获取并返回该任务的最新状态（TaskState）
func (b *BackendMS) GetState(taskUUID string) (*tasks.TaskState, error) {
	row := b.db.QueryRow(`
        SELECT task_uuid, task_name, state, results, error, created_at, ttl
        FROM task_state WHERE task_uuid = ?
    `, taskUUID)

	var (
		taskState   tasks.TaskState
		resultsJSON sql.NullString
		errorStr    sql.NullString
		ttlValue    sql.NullInt64
	)
	err := row.Scan(
		&taskState.TaskUUID,
		&taskState.TaskName,
		&taskState.State,
		&resultsJSON, // NullString
		&errorStr,    // NullString
		&taskState.CreatedAt,
		&ttlValue, // NullInt64
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 未找到

		}
		return nil, err
	}

	// 处理 results 字段
	if resultsJSON.Valid && resultsJSON.String != "" {
		decoder := json.NewDecoder(bytes.NewReader([]byte(resultsJSON.String)))
		decoder.UseNumber()
		if err := decoder.Decode(&taskState.Results); err != nil {
			return nil, err
		}

	}

	// 处理 error 字段
	if errorStr.Valid {
		taskState.Error = errorStr.String
	}

	// 处理 ttl 字段
	if ttlValue.Valid {
		taskState.TTL = ttlValue.Int64
	}

	return &taskState, nil
}

// 删除任务状态
func (b *BackendMS) PurgeState(taskUUID string) error {
	_, err := b.db.Exec(`
        DELETE FROM task_state WHERE task_uuid = ?
    `, taskUUID)
	return err
}

// 删除组的元数据
func (b *BackendMS) PurgeGroupMeta(groupUUID string) error {
	_, err := b.db.Exec(`
        DELETE FROM group_meta WHERE group_uuid = ?
    `, groupUUID)
	return err
}

// 根据组的 UUID，从 MySQL 中获取并返回该组的元数据（GroupMeta）
func (b *BackendMS) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	row := b.db.QueryRow(`
        SELECT group_uuid, task_uuids, chord_triggered, `+"`lock`"+`, created_at, ttl
        FROM group_meta WHERE group_uuid = ?
    `, groupUUID)

	var (
		groupMeta    tasks.GroupMeta
		taskUUIDsStr string
		ttlValue     sql.NullInt64
	)
	err := row.Scan(
		&groupMeta.GroupUUID,
		&taskUUIDsStr,
		&groupMeta.ChordTriggered,
		&groupMeta.Lock,
		&groupMeta.CreatedAt,
		&ttlValue,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // 未找到返回 nil
		}
		return nil, err
	}

	// 反序列化 task_uuids 字段
	// 这里可以用 unmarshal 是因为不涉及数字
	if err := json.Unmarshal([]byte(taskUUIDsStr), &groupMeta.TaskUUIDs); err != nil {
		return nil, err
	}

	// 处理 ttl 字段
	if ttlValue.Valid {
		groupMeta.TTL = ttlValue.Int64
	}

	return &groupMeta, nil
}

// 根据多个 task 的 UUID，从 MySQL 中获取多个任务状态
func (b *BackendMS) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := []*tasks.TaskState{}
	for _, taskUUID := range taskUUIDs {
		taskState, err := b.GetState(taskUUID)
		if err != nil {
			return nil, err
		}

		taskStates = append(taskStates, taskState)
	}

	return taskStates, nil
}

// // 更新 mysql 中的某个 taskState（插入或更新）
// func (b *BackendMS) updateState(taskState *tasks.TaskState) error {
// 	// 1. 序列化 Results 字段为 JSON
// 	resultsJSON, err := json.Marshal(taskState.Results)
// 	if err != nil {
// 		return err
// 	}

// 	// 2. 插入或更新（主键冲突时更新所有字段）
// 	_, err = b.db.Exec(`
//         INSERT INTO task_state
//             (task_uuid, task_name, state, results, error, created_at, ttl)
//         VALUES (?, ?, ?, ?, ?, ?, ?)
//         ON DUPLICATE KEY UPDATE
//             task_name = VALUES(task_name),
//             state = VALUES(state),
//             results = VALUES(results),
//             error = VALUES(error),
//             created_at = VALUES(created_at),
//             ttl = VALUES(ttl)
//     `,
// 		taskState.TaskUUID,
// 		taskState.TaskName,
// 		taskState.State,
// 		string(resultsJSON),
// 		taskState.Error,
// 		taskState.CreatedAt,
// 		taskState.TTL,
// 	)
// 	return err
// }
