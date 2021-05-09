package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"testing"
)

func TestSkiplistMemtable_Put(t *testing.T) {
	memtable := NewSkiplistMemtable(common.TypeBytes)
	for i := 0; i < 10000; i++ {
		k, v := []byte(common.RandString(10)), []byte(common.RandString(10))
		memtable.Put(k, v)
		if e := memtable.Get(k); string(e.Value()) != string(v) {
			t.Errorf("error")
		}
	}
}