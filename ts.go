package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"sync"
	"sync/atomic"
	"time"
)

type TransactionSet struct {
	// TrxId is the next trx id to be assigned, start from 1
	TrxId          uint32
	IsolationLevel uint8
	Transactions   sync.Map
	// logger
	undoLog UndoLog
	// db
	db *DrifterDB
	// timer lock
	timerLock sync.RWMutex
}

func NewTransactionSet(isolationLevel uint8, db *DrifterDB) *TransactionSet {
	return &TransactionSet{
		IsolationLevel: isolationLevel,
		db:             db,
		TrxId:          0,
		undoLog:        UndoLog{},
	}
}

// GetTransaction get a new transaction from transaction set and set up version ref (memtable ref is setup when first accessing the specified memetable)
func (ts *TransactionSet) GetTransaction() *Transaction {
	newTrxId := atomic.AddUint32(&ts.TrxId, 1)
	newTransaction := &Transaction{
		ReadView:           *NewReadView(ts.db, ts.IsolationLevel, ts.db.storage.GetVersion()),
		trxId:              newTrxId,
		modificationRecord: make([]*TrxOpRecord, 0, 8),
		needRollback:       false,
		refTables:          make([]Memtable, 0, 1),
	}
	ts.timerLock.RLock()
	defer ts.timerLock.RUnlock()
	ts.Transactions.Store(newTrxId, newTransaction)
	return newTransaction
}

func (ts *TransactionSet) MapTransaction(trxId uint32) *Transaction {
	v, ok := ts.Transactions.Load(trxId)
	if !ok {
		common.Warning("Transaction is not opened, so it can't be rollback.")
		return nil
	}
	return v.(*Transaction)
}

func (ts *TransactionSet) RollbackTransaction(trx *Transaction) {
	trx.rollback()
	ts.timerLock.RLock()
	defer ts.timerLock.RUnlock()
	ts.Transactions.Delete(trx.trxId)
	ts.db.storage.ReleaseVersion(trx.version)
}

func (ts *TransactionSet) RollbackTransactionByID(trxId uint32) {
	v, ok := ts.Transactions.Load(trxId)
	if !ok {
		common.Warning("Transaction is not opened, so it can't be rollback.")
		return
	}
	trx := v.(*Transaction)
	trx.rollback()
	ts.timerLock.RLock()
	defer ts.timerLock.RUnlock()
	ts.Transactions.Delete(trxId)
	ts.db.storage.ReleaseVersion(trx.version)
}

func (ts *TransactionSet) CommitTransaction(trx *Transaction) {
	trx.commit()
	ts.timerLock.RLock()
	defer ts.timerLock.RUnlock()
	ts.Transactions.Delete(trx.trxId)
	ts.db.storage.ReleaseVersion(trx.version)
}

func (ts *TransactionSet) CommitTransactionByID(trxId uint32)  {
	v, ok := ts.Transactions.Load(trxId)
	if !ok {
		common.Warning("Transaction is not opened, so it can't be rollback.")
		return
	}
	trx := v.(*Transaction)
	trx.commit()
	ts.timerLock.RLock()
	defer ts.timerLock.RUnlock()
	ts.Transactions.Delete(trxId)
	ts.db.storage.ReleaseVersion(trx.version)
}

func (ts *TransactionSet) StartTimer() {
	for {
		time.Sleep(time.Second)
		ts.timerLock.Lock()
		ts.Transactions.Range(func(key, value interface{}) bool {
			value.(*Transaction).updateTimeSince()
			return true
		})
		ts.timerLock.Unlock()
	}
}
