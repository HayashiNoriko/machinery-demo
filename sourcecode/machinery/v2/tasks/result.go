package tasks

import (
	"fmt"
	"reflect"
	"strings"
)

// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// 将一组 TaskResult（任务执行结果的通用结构体）转换为 Go 反射值（[]reflect.Value）数组
// 类似于 ReflectArgs
// ReflectTaskResults ...
func ReflectTaskResults(taskResults []*TaskResult) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(taskResults))
	for i, taskResult := range taskResults {
		resultValue, err := ReflectValue(taskResult.Type, taskResult.Value)
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	return resultValues, nil
}

// 将 Go 反射值数组 []reflect.Value 转换成供人类阅读的字符串
// HumanReadableResults ...
func HumanReadableResults(results []reflect.Value) string {
	if len(results) == 1 {
		return fmt.Sprintf("%v", results[0].Interface())
	}

	readableResults := make([]string, len(results))
	for i := 0; i < len(results); i++ {
		readableResults[i] = fmt.Sprintf("%v", results[i].Interface())
	}

	return fmt.Sprintf("[%s]", strings.Join(readableResults, ", "))
}
