package tasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"demo/sourcecode/machinery/v2/log"
)

// ErrTaskPanicked ...
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc   reflect.Value   // 任务对应的函数对象，通过反射机制存储。这样可以动态调用不同的任务函数
	UseContext bool            // 标记任务函数的第一个参数是否为 context.Context。如果为 true，调用任务时会自动把 context 作为第一个参数传入
	Context    context.Context // 任务执行时用到的上下文对象，可以用于传递超时、取消信号、链路追踪等信息
	Args       []reflect.Value // 任务参数列表，已经转换为反射值，方便后续通过反射调用任务函数
}

// 一个自定义类型的空结构体变量，用作 context 的 key
type signatureCtxType struct{}

var signatureCtx signatureCtxType

// 从 context.Context 中获取当前任务的 Signature 对象
// SignatureFromContext gets the signature from the context
func SignatureFromContext(ctx context.Context) *Signature {
	if ctx == nil {
		return nil
	}

	// 获取 context 中以 signatureCtx 作为 key 存储的值
	v := ctx.Value(signatureCtx)
	if v == nil {
		return nil
	}

	// 如果找到了，尝试类型断言为 *Signature 并返回
	signature, _ := v.(*Signature)
	return signature
}

// 根据 taskFunc 和 signature 创建一个 Task 实例，并将签名信息注入到 context 中
// NewWithSignature is the same as New but injects the signature
func NewWithSignature(taskFunc interface{}, signature *Signature) (*Task, error) {
	// 1. 提取参数
	args := signature.Args

	// 2. 新建 context，并将 signature 写入到 ctx
	ctx := context.Background()
	ctx = context.WithValue(ctx, signatureCtx, signature)

	// 3. 初始化 Task 实例
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  ctx,
	}

	// 4. 判断任务函数是否需要 context
	taskFuncType := reflect.TypeOf(taskFunc)
	// 检查任务函数的第一个参数类型是否为 context.Context
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		// 如果是，则设置 UseContext 字段为 true，后续调用时会自动把 context 作为第一个参数传入
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	// 5. 参数反射转换
	// 把 signature.Args 转换为反射值（[]reflect.Value），以便后续通过反射调用任务函数
	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// 根据传入的任务函数和参数，创建一个可执行的 Task 实例
// 并为后续通过反射调用任务函数做好准备。
// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(taskFunc interface{}, args []Arg) (*Task, error) {
	// 1. 初始化 Task 实例
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
	}

	// 2. 判断任务函数是否需要 context
	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	// 3. 参数反射转换
	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// 通过反射调用封装在 Task 结构体中的任务函数，并处理调用过程中的各种异常、错误和返回值，最终返回任务的执行结果（[]*TaskResult）和错误信息
// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
//  1. The reflected function invocation panics (e.g. due to a mismatched
//     argument list).
//  2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error) {
	// 1. 链路追踪处理
	// retrieve the span from the task's context and finish it as soon as this function returns
	// 如果 context 中有 tracing span，则在方法结束时自动调用 span.Finish()，用于分布式链路追踪
	if span := opentracing.SpanFromContext(t.Context); span != nil {
		defer span.Finish()
	}

	// 2. 异常捕获
	defer func() {
		// 捕获调用过程中可能发生的 panic，将 panic 转换为 error，并记录日志和链路追踪信息，保证任务执行不会因 panic 导致进程崩溃
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = ErrTaskPanicked
			case error:
				err = e
			case string:
				err = errors.New(e)
			}

			// mark the span as failed and dump the error and stack trace to the span
			if span := opentracing.SpanFromContext(t.Context); span != nil {
				opentracing_ext.Error.Set(span, true)
				span.LogFields(
					opentracing_log.Error(err),
					opentracing_log.Object("stack", string(debug.Stack())),
				)
			}

			// Print stack trace
			log.ERROR.Printf("%s", debug.Stack())
		}
	}()

	// 3. 参数准备
	// 默认使用 t.Args 作为参数
	args := t.Args
	// 如果 UseContext 为 true，说明任务函数第一个参数需要 context，则将 t.Context 作为第一个参数插入到参数列表前面
	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// 4. 反射调用任务函数
	// Invoke the task
	results := t.TaskFunc.Call(args)

	// 5. 返回值处理
	// 要求至少有一个返回值
	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	// 要求最后一个返回值必须是 error 类型，或实现了 Retriable 接口
	// Last returned value
	lastResult := results[len(results)-1]

	// 如果最后一个返回值不是 nil，说明任务执行出错，直接返回该错误
	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastResult.IsNil() {
		// 判断最后一个返回值是否实现了 Retriable 接口（框架自定义的一个接口，用于标记“这个错误可以重试”）
		// If the result implements Retriable interface, return instance of Retriable
		retriableErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retriableErrorInterface) {
			// 把它作为 ErrRetryTaskLater 返回，框架会据此决定是否重试任务
			return nil, lastResult.Interface().(ErrRetryTaskLater)
		}

		// 判断最后一个返回值是否实现了标准 error 接口
		// Otherwise, check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		// 如果不是 error 类型，说明任务函数的签名不符合要求（最后一个返回值必须是 error），返回 ErrLastReturnValueMustBeError 错误
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		// 作为正常 error 返回
		// Return the standard error
		return nil, lastResult.Interface().(error)
	}

	// 如果最后一个返回值为 nil，说明任务执行成功，将前面的返回值（业务返回值）转换为 TaskResult 列表返回
	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		// 去掉包名（自定义类型时，类型会带上包名，例如 main.Person）
		if len(strings.Split(typeStr, ".")) == 2 {
			typeStr = strings.Split(typeStr, ".")[1]
		}
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
		}
	}

	return taskResults, err
}

// 把任务参数（[]Arg）转换为反射值（[]reflect.Value），并赋值给 Task.Args 字段
// ReflectArgs converts []TaskArg to []reflect.Value
func (t *Task) ReflectArgs(args []Arg) error {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return err
		}
		argValues[i] = argValue
	}

	t.Args = argValues
	return nil
}
