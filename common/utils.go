package common

import (
	"math/rand"
	"os"
	"reflect"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	buf := make([]byte, len)
	randomBase := r.Int()
	for i := 0; i < len; i++ {
		b := (randomBase % 26) + 65
		buf[i] = byte(b)
		randomBase = randomBase * 5 / 6
	}
	return string(buf)
}

func isSlice(arg interface{}) bool {
	val := reflect.ValueOf(arg)
	return val.Kind() == reflect.Slice

}

func Equal(a interface{}, b interface{}) bool {
	if isSlice(a) && isSlice(b) {
		return false
	} else {
		return a == b
	}
}

func ConcatBytes(bytesArray ...[]byte) []byte {
	totalLength := 0
	for _, array := range bytesArray {
		totalLength += len(array)
	}
	result := make([]byte, totalLength)
	i := 0
	for _, array := range bytesArray {
		i += copy(result[i:], array)
	}
	return result
}

func UnsafeWrite(f *os.File, content []byte) int {
	i, err := f.Write(content)
	Throw(err)
	return i
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}