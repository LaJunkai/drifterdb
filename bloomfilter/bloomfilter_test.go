package bloomfilter

import (
	"github.com/LaJunkai/drifterdb/common"
	"fmt"
	"testing"
)

func TestFilter_Hashes(t *testing.T) {
	filter := NewFrozenFilter(15, 1)
	fmt.Println(filter.hashes("你好"))
	fmt.Println(filter.hashes([]byte("你好")))
}

func TestFilter_Add(t *testing.T) {
	filter := NewFrozenFilter(15, 5)
	for i := 0; i < 1000; i++ {
		filter.Add(common.RandString(21))
	}
}

func TestFilter_Exists(t *testing.T) {
	factor := 20
	filter := NewFrozenFilter(factor, 10)
	total := 100000
	//wrong := 0
	anoMap := make(map[string]bool)
	for i := 0; i < total; i++ {
		key := common.RandString(5)
		filter.Add(key)
		anoMap[key] = true
	}
	for k := range anoMap {
		if !filter.Exists(k) {
			t.Errorf("fail")
		}
	}
	wrong := 0
	for i := 0; i < total * 10; i++ {
		key := common.RandString(5)
		_, mexist := anoMap[key]
		bexist := filter.Exists(key)
		if mexist && ! bexist {
			t.Errorf("not pass")
		}
		if !mexist && bexist {
			wrong += 1
		}

	}
	fmt.Printf("%v mistakes / %v try with capacity[%v/%v]", wrong, total * 10, total, 1 << factor)
}


func TestFilter_Bits(t *testing.T) {
	filter := NewFrozenFilter(16, 1)
	filter.Add("test_key")
	e := filter.Exists("test_key")
	if !e {
		t.Errorf("the key is supposed to be existed")
	}
}

func TestFilter_DumpBytes(t *testing.T) {
	filter := NewFrozenFilter(10, 10)
	filter.Add("test_key")
	e := filter.Exists("test_key")
	if !e {
		t.Errorf("the key is supposed to be existed")
	}
	checkFilter := LoadFilterFromBytes(filter.DumpBytes())
	fmt.Println(filter.bits)
	fmt.Println(checkFilter.bits)
}


func TestLoadFilterFromBytes(t *testing.T) {
	var a string
	fmt.Println(a)
}