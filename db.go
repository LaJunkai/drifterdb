package drifterdb

import (
	"fmt"
	"github.com/LaJunkai/drifterdb/common"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

/*
MVCCKey is used as the underlying key type of the db by default.

memtable has a lifecycle contains active/frozen/immutable
active memtable is the very table that record the transaction operations
frozen memtable is the table that referenced by the active transaction but capacity is out, so frozen nolonger accept
any more operation except commit/rollback.
*/

type BaseDB interface {
	Put(key, value []byte) bool
	Get(key []byte) []byte
	Delete(key []byte) bool
	Range(s, e []byte, count, offset int) []*Element
	WithTransaction(target func(trx *Transaction))
	StartTransaction() *Transaction
	MapTransaction(trxId uint32) *Transaction
	CommitTransaction(trx *Transaction)
	CommitTransactionByID(trxId uint32)
	RollbackTransaction(trx *Transaction)
	RollbackTransactionByID(trxId uint32)
}

type DrifterDB struct {
	seq     uint64
	meta    *Meta
	option  *Option
	storage *Storage

	writeSlowDown bool
	writePaused   bool
	initializing  bool

	//session to be implemented
	wal       *WALWriter
	walReader *WALReader
	//
	compactionCommitLock sync.Mutex
	needCompactionChan   chan int
	// memtable
	memtable             Memtable
	tableSeq             int
	frozenMemtables      []Memtable
	immutableMemtables   []Memtable
	memtableLock         sync.RWMutex
	switchMemtableLock   sync.RWMutex
	dumpMemtableChan     chan int
	frozeMemtableChan    chan int
	waitingForFreezing   bool
	memtableWalOffsetMap map[Memtable]uint64
	// transaction
	IsolationLevel uint8
	transactionSet *TransactionSet

	// shut down
	closer     io.Closer
	closerChan chan struct{}
	closeWait  sync.WaitGroup
}

func New(path string, option *Option) *DrifterDB {

	current, _ := ioutil.ReadFile(filepath.Join(path, "current"))
	meta := LoadMeta(path, string(current))
	walFile, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("wal%08d.log", meta.WalSeq)), os.O_APPEND|os.O_CREATE, 0777)
	common.Throw(err)
	walReaderFile, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("wal%08d.log", meta.WalSeq)), os.O_APPEND, 0777)
	common.Throw(err)
	if option == nil {
		option = DefaultOption()
	}
	newDB := &DrifterDB{
		storage:              NewStorage(path, option.Levels),
		meta:                 meta,
		memtable:             NewSkiplistMemtable(common.TypeMVCCBytes),
		needCompactionChan:   make(chan int, 16),
		dumpMemtableChan:     make(chan int, 16),
		frozeMemtableChan:    make(chan int, 16),
		closerChan:           make(chan struct{}, 16),
		waitingForFreezing:   false,
		wal:                  NewWALWriter(walFile),
		walReader:            NewWALReader(walReaderFile),
		option:               option,
		IsolationLevel:       common.RepeatableRead,
		memtableWalOffsetMap: make(map[Memtable]uint64),
	}
	// complete storage
	newDB.storage.InitCurrentVersion(newDB.memtable)
	//
	newDB.transactionSet = NewTransactionSet(common.RepeatableRead, newDB)
	// goroutines
	// no wait timer
	go newDB.transactionSet.StartTimer()
	// need to wait
	go newDB.FrozeMemtableLoop()   // 1
	go newDB.collectMemtable()     // 2
	go newDB.dumpImmutableTables() // 3
	go newDB.CompactLoop()         // 4
	newDB.closeWait.Add(4)
	//
	// set WAL file init offset
	_, err = walReaderFile.Seek(int64(newDB.storage.currentVersion.walOffset), 0)
	// recover the tableSeq of the db
	newDB.tableSeq = MaxSeqInVersion(newDB.storage.currentVersion) + 1
	common.Throw(err)
	return newDB
}
func OpenDB(path string) *DrifterDB {
	db := New(path, DefaultOption())
	db.initializing = true
	defer func() {
		db.initializing = false
	}()
	db.wal.SetOffset(db.storage.currentVersion.walOffset)
	common.Always("[recover from WAL] recovering memtable from the WAL (offset:", db.storage.currentVersion.walOffset, ").")
	for o := db.walReader.Next(); o != nil; o = db.walReader.Next() {
		if o.KeyType() == common.OpPut {
			mvccKey := o.Key().(*common.MVCCKey)
			if db.seq < mvccKey.Seq {
				db.seq = mvccKey.Seq
			}
			db.put(mvccKey, o.ValueBytes())
		}
	}
	return db
}

func (db *DrifterDB) getSeq() uint64 {
	//return atomic.AddUint64(&db.seq, 1)
	db.seq += 1
	return db.seq
}

func (db *DrifterDB) put(key *common.MVCCKey, value []byte) (*Element, bool) {
	// no log writing during initializing
	if db.initializing {
		_, length := db.wal.Op2Log(common.OperationRecord(common.OpPut, key, value))
		e, done := db.memtable.Put(key, value)
		if done {
			db.memtable.IncreaseBytesSize(int(length))
		}
		if db.memtable.BytesSize() > db.option.MemtableSize {
			db.FrozeMemtable()
		}
		return e, done
	} else {
		length := int(db.wal.Append(common.OperationRecord(common.OpPut, key, value)))
		db.wal.Flush()
		e, done := db.memtable.Put(key, value)
		if done {
			db.memtable.IncreaseBytesSize(length)
		}
		if db.memtable.BytesSize() > db.option.MemtableSize {
			db.FrozeMemtable()
		}
		return e, done
	}
}

func (db *DrifterDB) Put(key, value []byte) bool {
	db.WithTransaction(func(trx *Transaction) {
		trx.Put(key, value)
	})
	return true
}

func (db *DrifterDB) Get(key []byte) (v []byte) {
	db.WithTransaction(func(trx *Transaction) {
		if result := trx.Get(key); result != nil {
			v = result
		}
	})
	return
}

func (db *DrifterDB) Range(start, end []byte, count, offset int) (v []*Element) {

	db.WithTransaction(func(trx *Transaction) {
		if result := trx.Range(start, end, count, offset); result != nil {
			v = result
		}
	})
	return
}

func (db *DrifterDB) Delete(key []byte) bool {
	mvccKey := common.MakeMVCCKey(key, db.getSeq(), common.OpDelete, 0)
	if result := db.memtable.Delete(mvccKey); result != nil {
		return true
	} else {
		return false
	}
}

// CAS
func (db *DrifterDB) CheckAndSet(key, oldValue, newValue []byte) {

}

// CAA
func (db *DrifterDB) CheckAndAdd(key, oldValue []byte, delta int64) {

}

// Atomic add, supported when the value has a length of 1/2/4/8 bytes.
func (db *DrifterDB) AtomicAdd(key []byte, delta int64) {

}

// FrozeMemtable will froze current alive memtable to immutable memtable, and then flush im-table to the disk
func (db *DrifterDB) FrozeMemtable() {
	if !db.waitingForFreezing {
		db.waitingForFreezing = true
		db.frozeMemtableChan <- 1
	}
}

// FrozeMemtable will froze current alive memtable to frozen memtable asynchronously.
func (db *DrifterDB) FrozeMemtableLoop() {
	defer db.closeWait.Done()
freezeLoop:
	for {
		select {
		case _ = <-db.frozeMemtableChan:
			newMemtable := NewSkiplistMemtable(common.TypeMVCCBytes)
			db.switchMemtableLock.Lock()
			common.Debug("[Froze memtable]", "frozen memtables:", len(db.frozenMemtables), ",immutable memtables:", len(db.immutableMemtables))
			db.frozenMemtables = append(db.frozenMemtables, db.memtable)
			db.memtableWalOffsetMap[db.memtable] = db.wal.cursor
			db.memtable = newMemtable
			db.waitingForFreezing = false
			db.switchMemtableLock.Unlock()
		case _ = <-db.closerChan:
			break freezeLoop
		}
	}
}

// Compact is supposed to run in a goroutine, and do compaction asynchronously.
func (db *DrifterDB) CompactLoop() {
	defer db.closeWait.Done()
compactLoop:
	for true {
		select {
		case level := <-db.needCompactionChan:
			if level < 0 {
				// dump memtable
				if db.option.NoCompaction {

				}
			}
		case _ = <-db.closerChan:
			break compactLoop
		}
	}
}

func (db *DrifterDB) Close() {
	for i := 0; i < 16; i++ {
		db.closerChan <- struct{}{}
	}
	db.closeWait.Wait()
}

func (db *DrifterDB) WithTransaction(target func(trx *Transaction)) {
	theTrx := db.transactionSet.GetTransaction()
	target(theTrx)
	if theTrx.needRollback {
		db.transactionSet.RollbackTransaction(theTrx)
	} else {
		db.transactionSet.CommitTransaction(theTrx)
	}
}

func (db *DrifterDB) StartTransaction() *Transaction {
	return db.transactionSet.GetTransaction()
}

func (db *DrifterDB) MapTransaction(trxId uint32) *Transaction {
	return db.transactionSet.MapTransaction(trxId)
}

func (db *DrifterDB) CommitTransaction(trx *Transaction) {
	db.transactionSet.CommitTransaction(trx)
}

func (db *DrifterDB) CommitTransactionByID(trxId uint32) {
	db.transactionSet.RollbackTransactionByID(trxId)
}

func (db *DrifterDB) RollbackTransaction(trx *Transaction) {
	db.transactionSet.RollbackTransaction(trx)
}

func (db *DrifterDB) RollbackTransactionByID(trxId uint32) {
	db.transactionSet.CommitTransactionByID(trxId)
}

func (db *DrifterDB) SetIsolationLevel(level uint8) {
	db.IsolationLevel = level
	db.transactionSet.IsolationLevel = level
}

func (db *DrifterDB) PreviewAllMemtables() {
	fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
	fmt.Printf("memtable (%v refs):\n", db.memtable.CountRefs())
	db.memtable.InlinePreview()
	fmt.Printf("frozen tables (%v tables):\n", len(db.frozenMemtables))
	for i, ft := range db.frozenMemtables {
		fmt.Printf("frozen table - %v (%v refs) \n", i, ft.CountRefs())
		ft.InlinePreview()
	}
	fmt.Println("--------------------------------------------------------------------------")
}
