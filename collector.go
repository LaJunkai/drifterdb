package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"time"
)

const (
	MemtableCollectorInterval = 1 * time.Second
)

func (db *DrifterDB) collectMemtable() {
	defer db.closeWait.Done()
collectMemtableLoop:
	for {
		select {
		case _ = <-db.closerChan:
			break collectMemtableLoop
		case _ = <-time.After(MemtableCollectorInterval):
			db.switchMemtableLock.Lock()
			nFrozenMemtabls := len(db.frozenMemtables)
			for i := 0; i < nFrozenMemtabls; {
				if db.frozenMemtables[i].CountRefs() == 0 {
					db.immutableMemtables = append(db.immutableMemtables, db.frozenMemtables[i])
					db.frozenMemtables = append(db.frozenMemtables[:i], db.frozenMemtables[i+1:]...)
					nFrozenMemtabls -= 1
					db.dumpMemtableChan <- 1
				} else {
					i += 1
				}
			}
			db.switchMemtableLock.Unlock()

		}
	}
}

func (db *DrifterDB) dumpImmutableTables() {
	defer db.closeWait.Done()
dumpImmutableLoop:
	for {
		common.Debug("[dump memtable] start dump immutable table loop")
		select {
		case _ = <-db.dumpMemtableChan:
			i := 0
			tableToDump := db.immutableMemtables[i]
			common.Debug("[dump memtable] before dump")

			// dump table and update the version and add new table to level 0
			newTable := db.storage.DumpMemtable(tableToDump, db.tableSeq)

			common.Debug("[dump memtable] dump finish, ready to acquire the lock")
			start := time.Now()

			db.switchMemtableLock.Lock()
			newVersion := CopyVersion(db.storage.currentVersion)
			newVersion.levels[0] = append(newVersion.levels[0], newTable)
			newVersion.walOffset = db.memtableWalOffsetMap[tableToDump]
			delete(db.memtableWalOffsetMap, tableToDump)
			db.storage.SetVersion(newVersion)
			common.Debug("[dump memtable] offset:", db.memtableWalOffsetMap[tableToDump], newVersion.walOffset)
			common.Debug("[dump memtable] dump got the lock, cost: ", time.Since(start).Seconds(), "s")
			db.immutableMemtables = append(db.immutableMemtables[:i], db.immutableMemtables[i + 1:]...)

			// setup every version
			db.tableSeq += 1
			db.switchMemtableLock.Unlock()
		case _ = <-db.closerChan:
			break dumpImmutableLoop
		}
	}
}

// collectSSTable check compacted sstables and delete them whose ref count is 0.
func (db *DrifterDB) collectSSTable() {

}
