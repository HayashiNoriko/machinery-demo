package tasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"

	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"github.com/RichardKnop/machinery/v2/log"
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

// NewWithSignature is the same as New but injects the signature
func NewWithSignature(taskFunc interface{}, signature *Signature) (*Task, error) {
	args := signature.Args
	ctx := context.Background()
	ctx = context.WithValue(ctx, signatureCtx, signature)
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  ctx,
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(taskFunc interface{}, args []Arg) (*Task, error) {
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
//  1. The reflected function invocation panics (e.g. due to a mismatched
//     argument list).
//  2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error) {
	// retrieve the span from the task's context and finish it as soon as this function returns
	if span := opentracing.SpanFromContext(t.Context); span != nil {
		defer span.Finish()
	}

	defer func() {
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

	args := t.Args

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// Invoke the task
	results := t.TaskFunc.Call(args)

	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	// Last returned value
	lastResult := results[len(results)-1]

	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastResult.IsNil() {
		// If the result implements Retriable interface, return instance of Retriable
		retriableErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retriableErrorInterface) {
			return nil, lastResult.Interface().(ErrRetryTaskLater)
		}

		// Otherwise, check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		// Return the standard error
		return nil, lastResult.Interface().(error)
	}

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
		}
	}

	return taskResults, err
}

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
