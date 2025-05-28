package tasks

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v2/utils"

	"github.com/google/uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// 任务头部信息
// 可用于传递额外的元数据，如链路追踪信息
// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// 遍历所有 header 中的键值对，并对每个字符串类型的值执行回调函数 handler
// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// Signature represents a single task invocation
type Signature struct {
	UUID           string       // 任务唯一 ID
	Name           string       // 任务名称，也是唯一的，通常对应你注册的任务函数名
	RoutingKey     string       // 路由键，用于消息队列路由任务到指定的队列或worker，其实就是队列名
	ETA            *time.Time   // 任务计划执行时间，如果设置，任务将在指定时间后执行（延迟任务）
	GroupUUID      string       // 任务组的唯一标识符，用于将多个任务归为一组（如批量任务、chord/group）
	GroupTaskCount int          // 任务组内任务数量，配合 GroupUUID 使用，标记组内任务总数
	Args           []Arg        // 任务参数，每个 Arg 表示一个参数，包含参数名、类型和值
	Headers        Headers      // 任务头部信息
	Priority       uint8        // 任务优先级，部分 broker 支持优先级队列
	Immutable      bool         // 是否不可变，如果为 true，任务参数不可被 worker 修改
	RetryCount     int          // 任务重试次数，任务失败后最多重试的次数
	RetryTimeout   int          // 任务重试间隔（单位秒），两次重试之间的等待时间
	OnSuccess      []*Signature // 成功回调任务，当前任务成功后要执行的后续任务列表
	OnError        []*Signature // 失败回调任务，当前任务失败后要执行的后续任务列表
	ChordCallback  *Signature   // Chord 回调任务，用于 chord 任务组全部完成后要执行的回调任务
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string // 消息组 ID，用于某些 broker（如 AWS SQS）实现消息分组
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string // SQS 消息的回执句柄，用于 AWS SQS 消息确认和删除
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool // 任务失败时是否阻止删除消息，主要用于 SQS，失败时不从源队列删除消息，便于发送到死信队列（DLQ）
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool // 当没有找到任务处理器时是否自动忽略该任务，为 true 时，未注册的任务不会重新入队，而是直接丢弃
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil
}

func CopySignatures(signatures ...*Signature) []*Signature {
	var sigs = make([]*Signature, len(signatures))
	for index, signature := range signatures {
		sigs[index] = CopySignature(signature)
	}
	return sigs
}

func CopySignature(signature *Signature) *Signature {
	var sig = new(Signature)
	_ = utils.DeepCopy(sig, signature)
	return sig
}
