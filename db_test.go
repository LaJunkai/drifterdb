package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	db := New("temp", nil)
	fmt.Println(db.meta)
	fmt.Println(db.memtable)
	fmt.Println(db.storage)
}

func TestDrifterDB_Put(t *testing.T) {
	db := New("temp", nil)
	db.Put([]byte("123"), []byte("456"))
	db.Put([]byte("name"), []byte("lajunkai"))
	db.Put([]byte("age"), []byte("21"))
	db.Put([]byte("game"), []byte("lol"))
	db.Put([]byte("word"), []byte("elementary"))
}

func TestOpenDB(t *testing.T) {
	db := OpenDB("temp")
	fmt.Println("result: ", string(db.Get([]byte("name"))))
}

func TestDrifterDB_CompactLoop(t *testing.T) {
	for true {
		switch 1 {
		case 1:
			fmt.Println("xixi")
			break
		}
	}
}

func TestAtomic(t *testing.T) {
	var a uint64 = 0
	var r sync.Mutex
	start := time.Now()
	for i := 0; i < 1<<30; i++ {
		// _ = atomic.AddUint64(&a, 1)
		r.Lock()
		a += 1
		_ = a
		r.Unlock()
	}
	fmt.Println(time.Since(start).Nanoseconds() / (1 << 30))
}

func TestDrifterDB_FrozeMemtable2(t *testing.T) {
	db := OpenDB("temp")
	db.Put([]byte("USERNAME-112"), []byte("La Junkai"))
	start := time.Now()
	total := 100000
	keyArray, valueArray := make([][]byte, 0, total), make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		keyArray = append(keyArray, []byte(common.RandString(10)))
		valueArray = append(valueArray, []byte(common.RandString(10)))
	}
	for i := 0; i < total; i++ {
		if i % 10000 == 0 {
			fmt.Println("[test processing]", i, "/", total)
		}
		db.Put(keyArray[i], valueArray[i])
	}
	db.Put([]byte("USERNAME-112"), []byte("XU JIA"))
	fmt.Println(string(db.Get([]byte("USERNAME-112"))))
	fmt.Println("total time cost:", time.Since(start).Seconds())
	fmt.Println(string(db.Get([]byte("USERNAME-112"))))
	time.Sleep(10 * time.Second)
	db.Close()
}

func TestDrifterDB_Get(t *testing.T) {
	db := OpenDB("temp")
	db.Put([]byte("Meeting-123"), []byte("xixi"))
	db.Put([]byte("Meeting-156"), []byte("xixi"))
	db.Put([]byte("Meeting-239"), []byte("xixi"))
	db.Put([]byte("Meeting-478"), []byte("xixi"))
	for i, e := range db.Range([]byte("Meeting-"), []byte("Meeting-z"), 10, 0) {
		fmt.Println(i, string(e.Key().(*common.MVCCKey).Content))
	}
}
