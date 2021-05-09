package drifterdb

import (
	"encoding/json"
	"github.com/LaJunkai/drifterdb/common"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type Storage struct {
	workDir          string
	nFiles           int
	levels           [][]*Table
	deprecatedTables map[*Table]interface{}
	versionLock      sync.Mutex
	// versions and ref count
	versions       map[*Version]int
	currentVersion *Version
}

func NewStorage(workDir string, nlevels int) *Storage {
	newStorage := &Storage{
		workDir:          workDir,
		deprecatedTables: make(map[*Table]interface{}, 0),
		versions:         make(map[*Version]int),
		levels:           make([][]*Table, nlevels),
	}
	newStorage.currentVersion = LoadVersion(workDir, nlevels)
	newStorage.DumpVersion(newStorage.currentVersion)
	return newStorage
}

func (s *Storage) GetVersion() *Version {
	s.versionLock.Lock()
	defer s.versionLock.Unlock()
	s.versions[s.currentVersion] += 1
	return s.currentVersion
}

func (s *Storage) ReleaseVersion(v *Version) {
	s.versionLock.Lock()
	defer s.versionLock.Unlock()
	s.versions[v] -= 1
	if s.versions[v] == 0 {
		for _, level := range v.levels {
			for _, table := range level {
				table.countVersionRefs -= 1
				if table.countVersionRefs == 0 {
					if _, existed := s.deprecatedTables[table]; existed {
						table.RemoveFile()
						delete(s.deprecatedTables, table)
					}
				}
			}
		}
	}
}

func (s *Storage) DumpMemtable(tableToDump Memtable, tableSeq int) *Table {
	return DumpTable(tableToDump, s.workDir, tableSeq)
}

func (s *Storage) CompactionLoop() {

}

func (s *Storage) Compact(level int) {

}

func (s *Storage) FindTable(key interface{}) *Table {
	return nil
}

func (s *Storage) SwitchVersion() {

}
func (s *Storage) versionFilename() string {
	return filepath.Join(s.workDir, "version.json")
}

func (s *Storage) versionBackupName() string {
	return filepath.Join(s.workDir, "version.json.bak")
}

// InitCurrentVersion should be called during the initialization of the db (before recover the memtable from the wal)
func (s *Storage) InitCurrentVersion(mt Memtable) {
	s.currentVersion.memtable = mt
}

func (s *Storage) DumpVersion(v *Version) []byte {
	levels := make([][]string, len(v.levels))
	for i, level := range v.levels {
		levels[i] = make([]string, 0, len(v.levels[i]))
		for _, table := range level {
			levels[i] = append(levels[i], table.FullPath())
		}
	}
	tablesToDelete := make([]string, 0, len(v.tablesToDelete))
	for _, table := range v.tablesToDelete {
		tablesToDelete = append(tablesToDelete, table.FullPath())
	}
	result, err := json.Marshal(VersionJson{
		Levels:         levels,
		TablesToDelete: tablesToDelete,
		WalOffset:      v.walOffset,
	})
	common.Throw(err)
	// write to the disk
	if common.PathExists(s.versionFilename()) {
		err = os.Rename(s.versionFilename(), s.versionBackupName())
		common.Throw(err)
	}
	err = ioutil.WriteFile(s.versionFilename(), result, 0777)
	common.Throw(err)
	return result
}

func (s *Storage) SetVersion(nv *Version) {
	s.versionLock.Lock()
	defer s.versionLock.Unlock()
	s.currentVersion = nv
	s.versions[nv] = 0
	s.DumpVersion(nv)
}
