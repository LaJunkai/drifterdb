package common

import (
	"fmt"
	"reflect"
)

const (
	OpPut        = 1 << 0
	OpDelete     = 1 << 1
	OpCheckpoint = 1 << 2
	OpGet        = 1 << 3
	OpRange      = 1 << 4
	OpExists     = 1 << 5
)

type Operation struct {
	opType int
	key    interface{}
	value  interface{}
}

func OperationRecord(opType int, key *MVCCKey, value []byte) *Operation {
	return &Operation{opType: opType, key: key, value: value}
}

func (o Operation) KeyType() int {
	return o.opType
}

func (o Operation) Key() interface{} {
	return o.key
}

func (o Operation) KeyBytes() []byte {
	switch o.key.(type) {
	case []byte:
		return TypeBytes.DumpBytes(o.Key())
	case string:
		return TypeString.DumpBytes(o.Key())
	case MVCCKey:
		return TypeMVCCBytes.DumpBytes(o.Key())
	case *MVCCKey:
		return TypeMVCCBytes.DumpBytes(o.Key())
	default:
		panic(fmt.Sprintf("unsupported value type (%v)", reflect.TypeOf(o.key)))
	}
}

func (o Operation) Value() interface{} {
	return o.value
}

func (o Operation) ValueBytes() []byte {
	switch o.value.(type) {
	case []byte:
		return o.value.([]byte)
	case string:
		return o.value.([]byte)
	default:
		panic(fmt.Sprintf("unsupported value type (%v)", reflect.TypeOf(o.value)))
	}
}
