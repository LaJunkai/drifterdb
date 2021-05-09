package common

import (
	"encoding/binary"
)

type Comparable interface {
	QueryCompare(left, right interface{}) int
	ModifyCompare(left, right interface{}) int
	ByteSizes(v interface{}) int
	DumpBytes(v interface{}) []byte
	OpType(v interface{}) uint8
}

type ComparableString struct{}

func (t ComparableString) OpType(v interface{}) uint8 {
	return OpPut
}

func (t ComparableString) DumpBytes(v interface{}) []byte {
	return []byte(v.(string))
}

func (t ComparableString) ByteSizes(v interface{}) int {
	return len(v.(string))
}

func (t ComparableString) QueryCompare(left, right interface{}) int {
	leftValue, rightValue := left.(string), right.(string)
	if leftValue > rightValue {
		return 1
	} else {
		if left == right {
			return 0
		}
		return -1
	}
}

func (t ComparableString) ModifyCompare(left, right interface{}) int {
	leftValue, rightValue := left.(string), right.(string)
	if leftValue > rightValue {
		return 1
	} else {
		if left == right {
			return 0
		}
		return -1
	}
}

type ComparableBytes struct{}

func (c ComparableBytes) OpType(v interface{}) uint8 {
	return OpPut
}

func (c ComparableBytes) DumpBytes(v interface{}) []byte {
	return v.([]byte)
}

func (c ComparableBytes) ByteSizes(v interface{}) int {
	return len(v.([]byte))
}

func (c ComparableBytes) QueryCompare(left, right interface{}) int {
	leftValue, rightValue := left.([]byte), right.([]byte)
	for i := 0; i < len(leftValue) && i < len(rightValue); i++ {
		if leftValue[i] > rightValue[i] {
			return 1
		} else if leftValue[i] < rightValue[i] {
			return -1
		}
	}
	if len(leftValue) > len(rightValue) {
		return 1
	} else if len(leftValue) < len(rightValue) {
		return -1
	} else {
		return 0
	}
}

func (c ComparableBytes) ModifyCompare(left, right interface{}) int {
	leftValue, rightValue := left.([]byte), right.([]byte)
	for i := 0; i < len(leftValue) && i < len(rightValue); i++ {
		if leftValue[i] > rightValue[i] {
			return 1
		} else if leftValue[i] < rightValue[i] {
			return -1
		}
	}
	if len(leftValue) > len(rightValue) {
		return 1
	} else if len(leftValue) < len(rightValue) {
		return -1
	} else {
		return 0
	}
}

type MVCCBytes struct{}

func (m MVCCBytes) OpType(v interface{}) uint8 {
	var key MVCCKey
	switch v.(type) {
	case *MVCCKey:
		key = *(v.(*MVCCKey))
	case MVCCKey:
		key = v.(MVCCKey)
	}
	return key.KT
}

// if two Keys has the same bytes content, newer version should expose a smaller compare value
// so that new version can be set ahead the elder version.

func (m MVCCBytes) QueryCompare(left, right interface{}) int {
	var leftValue, rightValue MVCCKey
	switch left.(type) {
	case *MVCCKey:
		leftValue = *(left.(*MVCCKey))
		rightValue = *(right.(*MVCCKey))
	case MVCCKey:
		leftValue = left.(MVCCKey)
		rightValue = right.(MVCCKey)
	}
	if byteResult := TypeBytes.QueryCompare(leftValue.Content, rightValue.Content); byteResult == 0 {
		// reverse the seq compare result
		if leftValue.Seq < rightValue.Seq {
			return 1
		} else if leftValue.Seq > rightValue.Seq {
			return 0
		} else {
			return 0
		}
	} else {
		return byteResult
	}
}

func (m MVCCBytes) ModifyCompare(left, right interface{}) int {
	var leftValue, rightValue MVCCKey
	switch left.(type) {
	case *MVCCKey:
		leftValue = *(left.(*MVCCKey))
		rightValue = *(right.(*MVCCKey))
	case MVCCKey:
		leftValue = left.(MVCCKey)
		rightValue = right.(MVCCKey)
	}
	if byteResult := TypeBytes.QueryCompare(leftValue.Content, rightValue.Content); byteResult == 0 {
		// reverse the seq compare result
		if leftValue.Seq < rightValue.Seq {
			return 1
		} else if leftValue.Seq > rightValue.Seq {
			return -1
		} else {
			return 0
		}
	} else {
		return byteResult
	}
}

func (m MVCCBytes) ByteSizes(v interface{}) int {
	return len(v.(MVCCKey).Content) + 8
}

func (m MVCCBytes) DumpBytes(v interface{}) []byte {
	var key MVCCKey
	switch v.(type) {
	case *MVCCKey:
		key = *(v.(*MVCCKey))
	case MVCCKey:
		key = v.(MVCCKey)
	}
	seqBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBytes, (key.Seq<<8)|uint64(key.KT))
	return ConcatBytes(seqBytes, key.Content)
}

var TypeString = ComparableString{}
var TypeBytes = ComparableBytes{}
var TypeMVCCBytes = MVCCBytes{}
