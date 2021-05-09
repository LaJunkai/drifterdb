package drifterdb

import (
	"fmt"
	"github.com/LaJunkai/drifterdb/common"
	"os"
	"testing"
)

func TestWALWriter_Append(t *testing.T) {
	file, _ := os.OpenFile("temp/wal00000000.log", os.O_APPEND | os.O_CREATE, 0777)
	key := common.MakeMVCCKey([]byte("key-1"), 0, common.OpPut, 0)
	fmt.Println(key)
	a := common.OperationRecord(common.OpPut, key, []byte("key-2"))
	l := NewWALWriter(file)
	for i := 0; i < 2; i++ {
		l.Append(a)
	}
	l.Flush()
}

func TestWALWriter_Op2Log(t *testing.T) {
	file, _ := os.OpenFile("temp/wal00000000.log", os.O_APPEND | os.O_CREATE , 0777)

	a := common.OperationRecord(common.OpPut, common.MakeMVCCKey([]byte("key-1"), 0, common.OpPut, 0), []byte("key-2"))
	l := NewWALWriter(file)
	fmt.Println(l.Op2Log(a))
}

func TestWALReader_Next(t *testing.T) {
	file, _ := os.OpenFile("temp/wal00000000.log", os.O_RDONLY , 0777)
	l := NewWALReader(file)

	for o := l.Next() ; o != nil ;o = l.Next() {
		fmt.Println(o.Key())
	}
}
func TestNewWALReader(t *testing.T) {
	a := make([]int, 10)
	b := a[10:]
	fmt.Println(b)
}