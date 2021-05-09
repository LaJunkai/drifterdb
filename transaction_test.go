package drifterdb

import (
	"fmt"
	"github.com/LaJunkai/drifterdb/common"
	"sync"
	"testing"
	"time"
)

func TestDrifterDB_WithTransaction(t *testing.T) {
	db := OpenDB("temp")
	var wg sync.WaitGroup
	wg.Add(20002)
	db.Put([]byte("user-1"), []byte("lajunkai"))
	db.Put([]byte("user-2"), []byte("JJLin"))
	db.Put([]byte("user-3"), []byte("Leehom"))
	db.WithTransaction(func(trx *Transaction) {
		trx.Put([]byte("user-2"), []byte("user-2"))
	})
	fmt.Println(string(db.Get([]byte("user-2"))))
}

func TestTransactionSet_StartTimer(t *testing.T) {
	db := OpenDB("temp")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		db.WithTransaction(func(trx *Transaction) {
			trx.Put([]byte("123"), []byte("nothing"))
			time.Sleep(time.Second * 20)
		})
	}()
	go func() {
		time.Sleep(time.Second)
		db.WithTransaction(func(trx *Transaction) {
			trx.Put([]byte("123"), []byte("timeout"))
		})
		defer wg.Done()
	}()
	wg.Wait()
	fmt.Println(string(db.Get([]byte("123"))))
}

func TestDrifterDB_FrozeMemtable(t *testing.T) {
	db := OpenDB("temp")
	for i := 0; i < 10; i++ {
		db.Put([]byte(common.RandString(10)), []byte(common.RandString(10)))
	}
	db.FrozeMemtable()
	db.PreviewAllMemtables()
}

func TestTransaction_checkLockOnFrozenMemtable(t *testing.T) {
	db := OpenDB("temp")
	var wg sync.WaitGroup
	wg.Add(2)
	db.Put([]byte("name"), []byte("WangLihong"))
	go func() {
		defer wg.Done()
		db.WithTransaction(func(trx *Transaction) {
			trx.Put([]byte("name"), []byte("LaJunkai"))
			db.PreviewAllMemtables()
			db.FrozeMemtable()
			time.Sleep(2 * time.Second)
			db.PreviewAllMemtables()
		})
	}()
	go func() {
		defer wg.Done()
		db.WithTransaction(func(trx *Transaction) {
			time.Sleep(time.Second)
			trx.Put([]byte("name"), []byte("JJLin"))
		})
	}()
	wg.Wait()
	fmt.Println("final value:", string(db.Get([]byte("name"))))
}