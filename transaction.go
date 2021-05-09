package drifterdb

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/common"
	"sort"
)

/*
Optimistic Transaction
* Check And Swap: [check -> update -> check -> done/rollback -> return]
* Check And Add: [check -> update -> check -> done/rollback -> return]

every modification operation has a unique seq, even they are executed in the same transaction.

only read-uncommitted is supported because there is no differ between the committed data and the uncommitted data.

Atomicity:
Consistency:
Isolation:
Durability:


*/

const (
	TransactionTimeout = 15
)

type TrxOpRecord struct {
	table Memtable
	key   *common.MVCCKey
}

// ReadView ignore versions of the data newer than the readSeq.
type ReadView struct {
	// readSeq: records with seq larger than the readSeq is not visible for the ReadView.
	readSeq        uint64
	db             *DrifterDB
	IsolationLevel uint8
	version        *Version
	// version
}

func NewReadView(db *DrifterDB, isolationLevel uint8, version *Version) *ReadView {
	return &ReadView{
		db:             db,
		readSeq:        db.getSeq(),
		IsolationLevel: isolationLevel,
		version:        version,
	}
}

func (rv *ReadView) Get(key []byte) []byte {
	rv.db.memtableLock.RLock()
	defer rv.db.memtableLock.RUnlock()
	if rv.IsolationLevel == common.ReadCommitted || rv.IsolationLevel == common.ReadUncommitted {
		rv.readSeq = rv.db.getSeq()
	}
	mvccKey := common.MakeIsoMVCCKey(key, rv.readSeq, common.OpGet, 0, rv.IsolationLevel)
	if result := rv.db.memtable.Get(mvccKey); result != nil {
		return result.Value()
	}
	// find kv in other memtables
	for _, table := range rv.db.frozenMemtables {
		if result := table.Get(mvccKey); result != nil {
			return result.Value()
		}
	}
	for _, table := range rv.db.immutableMemtables {
		if result := table.Get(mvccKey); result != nil {
			return result.Value()
		}
	}
	// find kv in sstables of the version
	for _, level := range rv.version.levels {
		for _, table := range level {
			if result := table.Get(mvccKey); result != nil {
				return result.Value()
			}

		}
	}
	return nil
}

type Transaction struct {
	ReadView
	// WriteSeq is the max
	trxId uint32
	// modificationSeq is a array contains each modification operation sequence number carried by the trx.
	modificationRecord []*TrxOpRecord
	needRollback       bool
	timeSince          int
	refTables          []Memtable
}

// merge two sorted array
func MergeRangeResult(left, right []*Element) []*Element {
	temp := make([]*Element, 0, len(left)+len(right))
	temp = append(temp, left...)
	temp = append(temp, right...)
	sort.Slice(temp, func(i, j int) bool {
		return common.TypeMVCCBytes.ModifyCompare(temp[i].Key(), temp[j].Key()) < 0
	})
	cursor := 0
	var prev []byte = nil
	for _, e := range temp {
		content := e.Key().(*common.MVCCKey).Content
		if prev != nil && bytes.Equal(content, prev) {
			continue
		} else {
			prev = content
		}
		temp[cursor] = e
		cursor += 1
	}
	return temp[:cursor]
}

func (rv *ReadView) Range(start, end []byte, count, offset int) []*Element {
	result := make([]*Element, 0, 2*(count+offset))
	rv.db.memtableLock.RLock()
	defer rv.db.memtableLock.RUnlock()
	if rv.IsolationLevel == common.ReadCommitted || rv.IsolationLevel == common.ReadUncommitted {
		rv.readSeq = rv.db.getSeq()
	}
	startMvccKey := common.MakeIsoMVCCKey(start, rv.readSeq, common.OpGet, 0, rv.IsolationLevel)
	endMvccKey := common.MakeIsoMVCCKey(end, rv.readSeq, common.OpGet, 0, rv.IsolationLevel)

	result = MergeRangeResult(result, rv.version.memtable.Range(startMvccKey, endMvccKey, count, offset))
	// find kv in other memtables
	for _, table := range rv.version.frozenMemtable {
		result = MergeRangeResult(result, table.Range(startMvccKey, endMvccKey, count, offset))
	}
	for _, table := range rv.version.ImmutableMemtable {
		result = MergeRangeResult(result, table.Range(startMvccKey, endMvccKey, count, offset))
	}
	//find kv in sstables of the version
	for _, level := range rv.version.levels {
		for _, table := range level {
			result = MergeRangeResult(result, table.Range(startMvccKey, endMvccKey, count+offset, offset))
		}
	}
	if len(result) < offset {
		return nil
	} else {
		if len(result) >= offset+count {
			return result[offset : offset+count]
		} else {
			return result[offset:]
		}
	}
}

// checkLockOnFrozenMemtables check the lock by an optimistic way
// [ important! ] and will acquire the lock of  trx.db.switchMemtableLock without release.
func (trx *Transaction) checkLockOnFrozenMemtables(key []byte) {
	trx.db.switchMemtableLock.RLock()
	// search frozen tables first after lock the switch memtable
	foundConflict := false
	var conflictKey *common.MVCCKey = nil
	var conflictMemtable Memtable = nil
	tempKey := common.MakeIsoMVCCKey(key, 0xFFFFFFFFFFFFFFFF, common.OpGet, trx.trxId, common.ReadUncommitted)
	for i := 0; i < len(trx.db.frozenMemtables); i++ {
		table := trx.db.frozenMemtables[i]
		if table.CountRefs() == 0 {
			continue
		}
		if e := table.Get(tempKey); e != nil {
			conflictKeyFound := e.ListEntry.Key().(*common.MVCCKey)
			if conflictKeyFound.TrxId != 0 && conflictKeyFound.TrxId != trx.trxId {
				foundConflict = true
				conflictKey = conflictKeyFound
				conflictMemtable = table
				break
			}
		}
	}
	if foundConflict && conflictKey != nil && conflictMemtable != nil {
		// unref when stop waiting
		conflictMemtable.Ref(trx)
		trx.refTables = append(trx.refTables, conflictMemtable)
		trx.db.switchMemtableLock.RUnlock()
		for conflictKey.TrxId != 0 {
			if trx.timeout() {
				common.Error("Transaction timeout during waiting a lock on frozen memtable.")
			}
		}
		trx.db.switchMemtableLock.RLock()
		conflictMemtable.CancelRef(trx)
	}
}

func (trx *Transaction) Put(key, value []byte) bool {

	mvccKey := common.MakeMVCCKey(key, 0, common.OpPut, trx.trxId) // add trx record
	trx.checkLockOnFrozenMemtables(key)
	for {
		//
		trx.db.memtable.Ref(trx)
		trx.refTables = append(trx.refTables, trx.db.memtable)
		// fragile design, unlock the mutex locked in the func checkLockOnFrozenMemtables
		trx.db.memtableLock.Lock()
		mvccKey.Seq = trx.db.getSeq()
		trx.modificationRecord = append(trx.modificationRecord, &TrxOpRecord{
			table: trx.db.memtable,
			key:   mvccKey,
		})
		e, done := trx.db.put(mvccKey, value)
		trx.db.memtableLock.Unlock()
		if done {
			trx.db.switchMemtableLock.RUnlock()
			return true
		} else {
			trx.modificationRecord = trx.modificationRecord[:len(trx.modificationRecord)-1]
			for e.ListEntry.Key().(*common.MVCCKey).TrxId != 0 {
				if trx.timeout() {
					trx.db.switchMemtableLock.RUnlock()
					common.Error("Transaction timeout during Waiting a lock on active memtable.")
				}
			}
		}
	}
}

func (trx *Transaction) Delete(key []byte) bool {
	mvccKey := common.MakeMVCCKey(key, 0, common.OpDelete, trx.trxId) // add trx record
	trx.checkLockOnFrozenMemtables(key)
	for {
		//
		trx.db.memtable.Ref(trx)
		trx.refTables = append(trx.refTables, trx.db.memtable)
		// fragile design, unlock the mutex locked in the func checkLockOnFrozenMemtables
		trx.db.memtableLock.Lock()
		mvccKey.Seq = trx.db.getSeq()
		trx.modificationRecord = append(trx.modificationRecord, &TrxOpRecord{
			table: trx.db.memtable,
			key:   mvccKey,
		})
		e, done := trx.db.put(mvccKey, []byte(""))
		trx.db.memtableLock.Unlock()
		if done {
			trx.db.switchMemtableLock.RUnlock()
			return true
		} else {
			trx.modificationRecord = trx.modificationRecord[:len(trx.modificationRecord)-1]
			for e.ListEntry.Key().(*common.MVCCKey).TrxId != 0 {
				if trx.timeout() {
					trx.db.switchMemtableLock.RUnlock()
					common.Error("Transaction timeout during Waiting a lock on active memtable.")
				}
			}
		}
	}
}

func (trx *Transaction) SetCommit() {
	trx.needRollback = false
}

func (trx *Transaction) SetRollback() {
	trx.needRollback = true
}

func (trx *Transaction) commit() {
	trx.db.switchMemtableLock.RLock()
	defer trx.db.switchMemtableLock.RUnlock()
	for _, r := range trx.modificationRecord {
		r.key.TrxId = 0
	}
	// release the version and release the memtable
	for _, t := range trx.refTables {
		t.CancelRef(trx)
	}
	trx.db.storage.ReleaseVersion(trx.version)
}

func (trx *Transaction) rollback() {
	trx.db.switchMemtableLock.RLock()
	defer trx.db.switchMemtableLock.RUnlock()
	trx.db.memtableLock.Lock()
	defer trx.db.memtableLock.Unlock()
	// delete log from the skiplist
	for _, r := range trx.modificationRecord {
		r.table.Delete(r.key)
		r.key.TrxId = 0
	}
	// release the version and release the memtable
	for _, t := range trx.refTables {
		t.CancelRef(trx)
	}
	trx.db.storage.ReleaseVersion(trx.version)

}

func (trx *Transaction) timeout() bool {
	return trx.timeSince > TransactionTimeout
}

func (trx *Transaction) updateTimeSince() {
	trx.timeSince += 1
}

func (trx *Transaction) TrxID() uint32 {
	return trx.trxId
}
