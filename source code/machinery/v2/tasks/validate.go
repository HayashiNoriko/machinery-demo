package tasks

import (
	"errors"
	"reflect"
)

var (
	// ErrTaskMustBeFunc ...
	ErrTaskMustBeFunc = errors.New("Task must be a func type")
	// ErrTaskReturnsNoValue ...
	ErrTaskReturnsNoValue = errors.New("Task must return at least a single value")
	// ErrLastReturnValueMustBeError ..
	ErrLastReturnValueMustBeError = errors.New("Last return value of a task must be error")
)

// 校验一个任务函数是否符合 machinery 框架的要求
// ValidateTask validates task function using reflection and makes sure
// it has a proper signature. Functions used as tasks must return at least a
// single value and the last return type must be error
func ValidateTask(task interface{}) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	// 1. 必须是函数类型
	// Task must be a function
	if t.Kind() != reflect.Func {
		return ErrTaskMustBeFunc
	}

	// 2. 必须至少有一个返回值
	// Task must return at least a single value
	if t.NumOut() < 1 {
		return ErrTaskReturnsNoValue
	}

	// 3. 最后一个返回值必须是 error 类型
	// Last return value must be error
	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrLastReturnValueMustBeError
	}

	return nil
}
