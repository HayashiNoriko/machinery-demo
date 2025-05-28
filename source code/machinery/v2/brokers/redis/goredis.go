package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// BrokerGR represents a Redis broker
type BrokerGR struct {
	common.Broker                       // 嵌入基础Broker，提供公共功能如配置、任务注册等
	rclient       redis.UniversalClient // Redis客户端，支持集群/单机/哨兵模式，所有Redis操作都通过这个客户端执行
	consumingWG   sync.WaitGroup        // 等待主消费循环
	processingWG  sync.WaitGroup        // 等待正在处理的任务完成
	delayedWG     sync.WaitGroup        // 等待延迟处理的任务完成

	// 以下字段与Redis连接相关
	socketPath           string           // 可选的Unix socket路径，用于替代hostname连接
	redsync              *redsync.Redsync // Redis分布式锁实现，基于Redis的Redlock算法实现
	redisOnce            sync.Once        // 确保某些初始化操作只执行一次
	redisDelayedTasksKey string           // Redis中存储延迟任务的ZSET键名
}

func NewGR(cnf *config.Config, addrs []string, db int) iface.Broker {
	// 1. 创建基础 Broker 实例
	b := &BrokerGR{Broker: common.NewBroker(cnf)}

	// 2. 解析 Redis 连接地址和密码
	var password string
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		// 处理带密码的连接地址格式 (password@host:port)
		password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1] // 分离出真正的地址部分
	}

	// 3. 配置 Redis 客户端选项
	ropt := &redis.UniversalOptions{
		Addrs:    addrs,    // Redis 地址列表
		DB:       db,       // Redis 数据库编号
		Password: password, // 认证密码
	}

	// 4. 处理 Redis 哨兵模式配置
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName // 哨兵模式下的主节点名称
	}

	// 5. 创建 Redis 通用客户端
	b.rclient = redis.NewUniversalClient(ropt)

	// 6. 配置延迟任务键名
	if cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}

	return b
}

// 开始消费。核心消费逻辑，负责启动任务消费流程
// 返回值：
// bool：表示当前 broker 是否还应该继续重试消费（通常和重试机制相关）
// error：表示本次启动消费过程中遇到的错误
// StartConsuming enters a loop and waits for incoming messages
func (b *BrokerGR) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	// 1. 初始化阶段
	b.consumingWG.Add(1)       // 标记消费开始
	defer b.consumingWG.Done() // 确保结束时标记完成

	// 用户填写 concurrency <= 0 时，表示希望 machinery 框架自动设置并发数（CPU核心数×2）
	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	// 执行基类的 StartConsuming 方法（设置 retryFunc）
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// 2. 健康检查
	// 通过 Redis PING 命令验证连接有效性
	// Ping the server to make sure connection is live
	_, err := b.rclient.Ping(context.Background()).Result()
	if err != nil {
		// 连接失败时自动触发重试逻辑
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// 3. 构建任务处理管道
	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency) // 任务传递通道
	pool := make(chan struct{}, concurrency)     // 控制并发 worker 数量

	// 初始化信号槽位
	// 空结构体作为轻量级信号量
	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// 4. 启动主消费协程
	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		// 死循环监听 stopChan 的信号或 worker 池信号
		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool: // worker 池中有空闲 worker，可取出一个
				// nextTask 方法会从 redis 队列中 BLPOP 拉取任务
				task, _ := b.nextTask(getQueueGR(b.GetConfig(), taskProcessor))
				//TODO: should this error be ignored?
				if len(task) > 0 {
					// 把拉取的 tasks 推送到 deliveries 中
					deliveries <- task
				}

				// 加回 worker 池中
				pool <- struct{}{}
			}
		}
	}()

	// 5. 启动延迟任务消费协程
	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		// 死循环监听 stopChan 的信号，如果暂时没有 stop 信号，就走 default 分支
		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				// 获取下一个延期任务
				task, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				// 反序列化后重新发布到普通队列
				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshalTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	// 6. 启动任务处理主循环
	// 调用 consume 方法并发处理 deliveries 中的任务。
	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// 等待所有任务处理完成后返回
	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *BrokerGR) StopConsuming() {
	// 1. 调用基类的 StopConsuming
	b.Broker.StopConsuming()
	// 2. 等待延迟队列的任务被执行完
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// 3. 等待正在主消费循环结束（就是 StartConsuming 方法执行结束）
	// Waiting for consumption to finish
	b.consumingWG.Wait()
	// 4. 关闭 redis 客户端
	b.rclient.Close()
}

// 将一个任务（task）发布到 Redis 队列，即把任务推送到消息队列中，供 worker 消费。它支持普通任务和延迟任务两种情况
// Publish places a new message on the default queue
func (b *BrokerGR) Publish(ctx context.Context, signature *tasks.Signature) error {
	// 1. 调整路由键（队列名）
	// 确保任务的 RoutingKey 字段被正确设置（决定消息发往哪个队列）
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	// 2. 序列化任务
	// 将任务签名（signature）序列化为 JSON 格式的字节流，便于存储到 Redis
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// 3. 任务入队
	// 判断是否为延迟任务
	// 如果 signature.ETA 字段不为空，且时间在未来，说明是延迟任务
	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			// 将任务存入 Redis 的 ZSET（有序集合），score 为 ETA 的纳秒时间戳，键为 b.redisDelayedTasksKey。这样任务会被延迟消费
			score := signature.ETA.UnixNano()
			err = b.rclient.ZAdd(context.Background(), b.redisDelayedTasksKey, redis.Z{Score: float64(score), Member: msg}).Err()
			return err
		}
	}

	// 不是延迟任务，是普通任务
	// 普通任务直接入队
	err = b.rclient.RPush(context.Background(), signature.RoutingKey, msg).Err()
	return err
}

// 获取指定 Redis 队列中所有等待执行的任务（即还未被 worker 消费的任务），并将它们反序列化为 tasks.Signature 结构体切片返回
// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *BrokerGR) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	// 1. 确定队列名
	// 如果传入的 queue 为空，则使用配置中的默认队列名
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	// 2. 获取队列中的所有任务
	results, err := b.rclient.LRange(context.Background(), queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// 3. 反序列化任务
	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// 获取所有已调度但尚未进入普通队列的延迟任务，即从 Redis 的 ZSET（有序集合）中读取所有延迟任务，并将它们反序列化为 tasks.Signature 结构体切片返回
// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *BrokerGR) GetDelayedTasks() ([]*tasks.Signature, error) {
	results, err := b.rclient.ZRange(context.Background(), b.redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// 并发消费任务，管理 worker 池，实现任务的并发处理和错误捕获
// 会不断从 deliveries channel 读取任务数据，并发地交给 worker 处理
// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *BrokerGR) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	// 1. 初始化错误通道和 worker 池
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// 2. 初始化 worker 池
	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	// 3. 主循环：消费任务
	for {
		// 使用 select 监听两个 channel
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			// 如果 deliveries channel 关闭，返回 nil 结束消费
			if !open {
				return nil
			}

			// 4. 并发处理任务
			// 如果设置了并发（concurrency > 0），先从 pool 获取一个槽位（如果没有槽位会阻塞，达到并发上限）
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			// 增加 processingWG 计数，表示有一个任务正在处理
			b.processingWG.Add(1)

			// 启动 goroutine 处理任务
			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				// 调用 consumeOne 实际处理任务
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				// 任务处理完毕
				b.processingWG.Done()

				// 把槽位归还给 pool
				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
// 处理单个任务消息，即对从 Redis 队列中取出的任务进行反序列化、校验、分发和执行
func (b *BrokerGR) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	// 1. 反序列化任务签名
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// 2. 校验任务是否已注册
	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		// 2.1 如果没有注册，且配置了“允许忽略”，直接忽略
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}

		// 2.2 如果没有注册，则将任务重新入队（RPUSH 到队列）
		// 这样可以让其他 worker 有机会处理该任务
		log.INFO.Printf("Task not registered with this worker. Requeuing message: %s", delivery)

		b.rclient.RPush(context.Background(), getQueueGR(b.GetConfig(), taskProcessor), delivery)
		return nil
	}

	// 3. 执行任务
	log.DEBUG.Printf("Received new message: %s", delivery)

	// 调用 Process 实际处理任务
	return taskProcessor.Process(signature)
}

// 从 Redis 队列中弹出（取出）下一个可用的任务
// nextTask pops next available task from the default queue
func (b *BrokerGR) nextTask(queue string) (result []byte, err error) {

	// 1. 确定轮询间隔
	// （每秒轮询一次队列）
	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	// （如果配置文件中设置了 NormalTasksPollPeriod，则使用配置值）
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	// 2. 阻塞式弹出任务
	// BLPop 会等待指定的超时时间（pollPeriod），如果队列有数据就返回，没有数据就阻塞等待
	items, err := b.rclient.BLPop(context.Background(), pollPeriod, queue).Result()
	if err != nil {
		return []byte{}, err
	}

	// 3. 处理返回结果
	// 第 2 步中返回的 items 是一个字符串切片，items[0] 是队列名，items[1] 是弹出的任务内容
	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	// 如果返回结果长度不是 2，说明没有弹出任务，返回空 byte 切片和 redis.Nil
	if len(items) != 2 {
		return []byte{}, redis.Nil
	}

	// 4. 返回任务数据
	result = []byte(items[1])

	return result, nil
}

// 尝试从 Redis 的 ZSET（有序集合）中弹出（取出）下一个到期的延迟任务
// 不管有没有取到，都应该尽快返回（要么返回任务，要么返回 redis.Nil 或其他错误）
// （外部处于 for{select-case-default}中，外部在轮询，因此不应该在nextDelayedTask内部阻塞太久时间）
// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *BrokerGR) nextDelayedTask(key string) (result []byte, err error) {

	//pipe := b.rclient.Pipeline()
	//
	//defer func() {
	//	// Return connection to normal state on error.
	//	// https://redis.io/commands/discard
	//	if err != nil {
	//		pipe.Discard()
	//	}
	//}()

	var (
		items []string
	)

	// 1. 确定轮询间隔
	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	// 这块有个小问题啊，for 循环其实是没有必要的，for 循环执行一次之后，就要么 break 要么 return 了
	// 觉得应该直接删掉
	// 2. 循环轮询 ZSET
	for {
		// 每次循环先 Sleep 一段时间（pollPeriod），防止高频请求 Redis
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)

		// 3. 使用 Redis 事务（WATCH/MULTI/EXEC）保证原子性
		// 确保“查找并移除”操作的原子性，避免并发场景下同一个任务被多个 worker 同时取走
		watchFunc := func(tx *redis.Tx) error {

			// 4. 查找到期任务
			// 4.1 获取当前时间戳
			now := time.Now().UTC().UnixNano()

			// https://redis.io/commands/zrangebyscore
			ctx := context.Background()

			// 4.2 查询 ZSET 中，分数（score）<= 当前时间（now）的任务，也就是已到期的任务。只取一个（Count: 1）
			items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return err
			}
			// 4.3 如果 items 中的元素数量不是 1 个，说明是 0 个，表示没有到期的任务，返回 redis.Nil
			if len(items) != 1 {
				return redis.Nil
			}

			// 5. 取出任务
			// 现在查到了到期任务，则用事务管道 TxPipelined 执行 ZRem，把该任务从 ZSET 中移除，并将任务内容保存到 result（命名返回值）中
			// only return the first zrange value if there are no other changes in this key
			// to make sure a delayed task would only be consumed once
			// TxPipelined的第二个参数是一个回调函数，它的返回值代表你在构造 pipeline 时的自定义错误，比如你主动 return 某个 error
			// 回调函数里 return nil，只表示 pipeline 里没有主动报错
			// 但如果 Redis 事务本身失败了（比如 WATCH 发现 key 被修改，事务未提交），go-redis 会让 TxPipelined 返回那个 error
			// 因此 TxPipelined 是否返回 error，取决于两个条件，1.用户的回调中是否主动抛出错误；2. go-redis 检测执行事务是否失败
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.ZRem(ctx, key, items[0])
				result = []byte(items[0])
				return nil
			})

			return err
		}

		// 6. 返回任务
		if err = b.rclient.Watch(context.Background(), watchFunc, key); err != nil {
			return
		} else {
			break
		}
	}

	return
}

func getQueueGR(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
