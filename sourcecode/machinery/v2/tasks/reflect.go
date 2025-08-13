package tasks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	typesMap = map[string]reflect.Type{
		// base types
		"bool":    reflect.TypeOf(true),
		"int":     reflect.TypeOf(int(1)),
		"int8":    reflect.TypeOf(int8(1)),
		"int16":   reflect.TypeOf(int16(1)),
		"int32":   reflect.TypeOf(int32(1)),
		"int64":   reflect.TypeOf(int64(1)),
		"uint":    reflect.TypeOf(uint(1)),
		"uint8":   reflect.TypeOf(uint8(1)),
		"uint16":  reflect.TypeOf(uint16(1)),
		"uint32":  reflect.TypeOf(uint32(1)),
		"uint64":  reflect.TypeOf(uint64(1)),
		"float32": reflect.TypeOf(float32(0.5)),
		"float64": reflect.TypeOf(float64(0.5)),
		"string":  reflect.TypeOf(string("")),
		// slices
		"[]bool":    reflect.TypeOf(make([]bool, 0)),
		"[]int":     reflect.TypeOf(make([]int, 0)),
		"[]int8":    reflect.TypeOf(make([]int8, 0)),
		"[]int16":   reflect.TypeOf(make([]int16, 0)),
		"[]int32":   reflect.TypeOf(make([]int32, 0)),
		"[]int64":   reflect.TypeOf(make([]int64, 0)),
		"[]uint":    reflect.TypeOf(make([]uint, 0)),
		"[]uint8":   reflect.TypeOf(make([]uint8, 0)),
		"[]uint16":  reflect.TypeOf(make([]uint16, 0)),
		"[]uint32":  reflect.TypeOf(make([]uint32, 0)),
		"[]uint64":  reflect.TypeOf(make([]uint64, 0)),
		"[]float32": reflect.TypeOf(make([]float32, 0)),
		"[]float64": reflect.TypeOf(make([]float64, 0)),
		"[]byte":    reflect.TypeOf(make([]byte, 0)),
		"[]string":  reflect.TypeOf([]string{""}),
	}

	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem() // .Elem() 用于从接口指针类型获取接口本身的类型

	typeConversionError = func(argValue interface{}, argTypeStr string) error {
		return fmt.Errorf("%v is not %v", argValue, argTypeStr)
	}
)

// ErrUnsupportedType ...
type ErrUnsupportedType struct {
	valueType string
}

// NewErrUnsupportedType returns new ErrUnsupportedType
func NewErrUnsupportedType(valueType string) ErrUnsupportedType {
	return ErrUnsupportedType{valueType}
}

// 实现了 Go 的 error 接口，可以作为普通错误使用
// Error method so we implement the error interface
func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("%v is not one of supported types", e.valueType)
}

// 增加新的类型
func RegisterType(valueType string, value interface{}) {
	typesMap[valueType] = reflect.TypeOf(value)
}

// 将任意类型的参数（通常是从 JSON 或任务参数中反序列化出来的）转换为 Go 的 reflect.Value，以便后续通过反射调用任务函数
// ReflectValue converts interface{} to reflect.Value based on string type
func ReflectValue(valueType string, value interface{}) (reflect.Value, error) {
	// 根据 valueType 判断要转换的是基础类型还是切片类型
	if strings.HasPrefix(valueType, "[]") {
		return reflectValues(valueType, value)
	}

	return reflectValue(valueType, value)
}

// 将基础类型（非切片）字符串描述的类型和值转换为 reflect.Value
// reflectValue converts interface{} to reflect.Value based on string type
// representing a base type (not a slice)
func reflectValue(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}
	theValue := reflect.New(theType)

	// 根据不同的基础类型分别处理，例如 bool、int、uint、float、string 等，调用对应的 getXXXValue 方法做类型断言和转换

	// Booleans
	if theType.String() == "bool" {
		boolValue, err := getBoolValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetBool(boolValue)
		return theValue.Elem(), nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "int") {
		intValue, err := getIntValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetInt(intValue)
		return theValue.Elem(), err
	}

	// Unsigned integers
	if strings.HasPrefix(theType.String(), "uint") {
		uintValue, err := getUintValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetUint(uintValue)
		return theValue.Elem(), err
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "float") {
		floatValue, err := getFloatValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetFloat(floatValue)
		return theValue.Elem(), err
	}

	// Strings
	if theType.String() == "string" {
		stringValue, err := getStringValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetString(stringValue)
		return theValue.Elem(), nil
	}

	// 处理自定义类型的切片
	// Handle slices of custom types
	res, err := handleCustomType(theType, value)
	if err != nil {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}

	return res, err
}

// handleCustomType 处理自定义类型的转换
// handleCustomType handles conversion for custom types using JSON marshaling/unmarshaling
func handleCustomType(theType reflect.Type, value interface{}) (reflect.Value, error) {
	// 创建目标类型的新实例
	theValue := reflect.New(theType)

	// 如果 value 已经是目标类型，直接返回
	if reflect.TypeOf(value) == theType {
		return reflect.ValueOf(value), nil
	}

	// 使用 JSON 序列化/反序列化进行类型转换
	// 这种方法适用于大多数自定义结构体类型
	jsonData, err := json.Marshal(value)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to marshal value for custom type %s: %v", theType.String(), err)
	}

	// 反序列化到目标类型
	if err := json.Unmarshal(jsonData, theValue.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf("failed to unmarshal value for custom type %s: %v", theType.String(), err)
	}

	return theValue.Elem(), nil
}

// 将切片类型（如 "[]int"、"[]string"）字符串描述的类型和值转换为 reflect.Value
// reflectValues converts interface{} to reflect.Value based on string type
// representing a slice of values
func reflectValues(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}

	// For NULL we return an empty slice
	if value == nil {
		return reflect.MakeSlice(theType, 0, 0), nil
	}

	var theValue reflect.Value

	// 针对不同切片类型，遍历每个元素，调用对应的 getXXXValue 方法做类型断言和转换，最后组装成目标类型的切片 reflect.Value
	// Booleans
	if theType.String() == "[]bool" {
		bools := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, bools.Len(), bools.Len())
		for i := 0; i < bools.Len(); i++ {
			boolValue, err := getBoolValue(strings.Split(theType.String(), "[]")[1], bools.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetBool(boolValue)
		}

		return theValue, nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "[]int") {
		ints := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, ints.Len(), ints.Len())
		for i := 0; i < ints.Len(); i++ {
			intValue, err := getIntValue(strings.Split(theType.String(), "[]")[1], ints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetInt(intValue)
		}

		return theValue, nil
	}

	// Unsigned integers
	if strings.HasPrefix(theType.String(), "[]uint") || theType.String() == "[]byte" {

		// Decode the base64 string if the value type is []uint8 or it's alias []byte
		// See: https://golang.org/pkg/encoding/json/#Marshal
		// > Array and slice values encode as JSON arrays, except that []byte encodes as a base64-encoded string
		if reflect.TypeOf(value).String() == "string" {
			output, err := base64.StdEncoding.DecodeString(value.(string))
			if err != nil {
				return reflect.Value{}, err
			}
			value = output
		}

		uints := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, uints.Len(), uints.Len())
		for i := 0; i < uints.Len(); i++ {
			uintValue, err := getUintValue(strings.Split(theType.String(), "[]")[1], uints.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetUint(uintValue)
		}

		return theValue, nil
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "[]float") {
		floats := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, floats.Len(), floats.Len())
		for i := 0; i < floats.Len(); i++ {
			floatValue, err := getFloatValue(strings.Split(theType.String(), "[]")[1], floats.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetFloat(floatValue)
		}

		return theValue, nil
	}

	// Strings
	if theType.String() == "[]string" {
		strs := reflect.ValueOf(value)

		theValue = reflect.MakeSlice(theType, strs.Len(), strs.Len())
		for i := 0; i < strs.Len(); i++ {
			strValue, err := getStringValue(strings.Split(theType.String(), "[]")[1], strs.Index(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			theValue.Index(i).SetString(strValue)
		}

		return theValue, nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

func getBoolValue(theType string, value interface{}) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, typeConversionError(value, typesMap[theType].String())
	}

	return b, nil
}

func getIntValue(theType string, value interface{}) (int64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	// when converting from float64 to signed integer
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Int64()
	}

	n, ok := value.(int64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	// Losing precision only happens in receiving a JSON number from a language like js,
	// and receiving a large uint number from golang or python could cause json.Number.Int64 be turned into a panic.
	// So we use strconv.ParseUint to correctly parse a uint value.
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		uintVal, err := strconv.ParseUint(string(n), 10, 64)
		if err != nil {
			return 0, err
		}

		return uintVal, nil
	}

	var n uint64
	switch value := value.(type) {
	case uint64:
		n = value
	case uint8:
		n = uint64(value)
	default:
		return 0, typeConversionError(value, typesMap[theType].String())
	}
	return n, nil
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Float64()
	}

	f, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return f, nil
}

func getStringValue(theType string, value interface{}) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", typeConversionError(value, typesMap[theType].String())
	}

	return s, nil
}

// IsContextType checks to see if the type is a context.Context
func IsContextType(t reflect.Type) bool {
	return t == ctxType
}
