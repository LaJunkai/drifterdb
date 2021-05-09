package drifterdb

import (
	"encoding/json"
	"github.com/LaJunkai/drifterdb/common"
	"io/ioutil"
	"path/filepath"
)

type VersionJson struct {
	Levels         [][]string `json:"levels"`
	TablesToDelete []string   `json:"tables_to_delete"`
	WalOffset      uint64     `json:"wal_offset"`
}

type Version struct {
	levels            [][]*Table
	memtable          Memtable
	frozenMemtable    []Memtable
	ImmutableMemtable []Memtable
	tablesToDelete    []*Table
	walOffset         uint64
}

func EmptyVersion(defaultLevels int) *Version {

	ev := &Version{
		levels:            make([][]*Table, defaultLevels),
		memtable:          nil,
		frozenMemtable:    make([]Memtable, 0),
		ImmutableMemtable: make([]Memtable, 0),
		tablesToDelete:    make([]*Table, 0),
		walOffset:         0,
	}
	for i := range ev.levels {
		ev.levels[i] = make([]*Table, 0, 0)
	}
	return ev
}

func NewVersion(levels [][]*Table, memtable Memtable, frozenMemtable []Memtable, immutableMemtable []Memtable, tablesToDelete []*Table, walOffset uint64) *Version {
	copyLevels := make([][]*Table, len(levels))
	for i := 0; i < len(levels); i++ {
		copyLevels[i] = append(make([]*Table, 0, len(levels)), levels[i]...)
	}
	copyFrozenMemtable := make([]Memtable, 0, len(frozenMemtable))
	copyFrozenMemtable = append(copyFrozenMemtable, frozenMemtable...)
	copyImmutableMemtable := make([]Memtable, 0, len(immutableMemtable))
	copyImmutableMemtable = append(copyImmutableMemtable, immutableMemtable...)
	copyTablesToDelete := make([]*Table, 0, len(tablesToDelete))
	copyTablesToDelete = append(copyTablesToDelete, tablesToDelete...)

	return &Version{
		levels:            levels,
		memtable:          memtable,
		frozenMemtable:    copyFrozenMemtable,
		ImmutableMemtable: copyImmutableMemtable,
		tablesToDelete:    copyTablesToDelete,
		walOffset:         walOffset,
	}
}

// LoadVersion method load the version metadata and load header of the tables
func LoadVersion(path string, defaultLevels int) *Version {
	versionBytes, err := ioutil.ReadFile(filepath.Join(path, "version.json"))
	if err != nil {
		common.Regular("[storage] Default version not found, use back up instead.")
		versionBytes, err = ioutil.ReadFile(filepath.Join(path, "version.json.bak"))
		common.Regular("[storage] No version file found. Initialize with new version.")
		return EmptyVersion(defaultLevels)
	}
	versionJson := VersionJson{}
	err = json.Unmarshal(versionBytes, &versionJson)
	common.Throw(err)
	levels := make([][]*Table, len(versionJson.Levels))
	tablesToDelete := make([]*Table, 0)
	for i, level := range versionJson.Levels {
		for _, tablePath := range level {
			levels[i] = append(levels[i], LoadTable(tablePath))
		}
	}
	for _, tableName := range versionJson.TablesToDelete {
		tablesToDelete = append(tablesToDelete, LoadTable(tableName))
	}
	return NewVersion(
		levels,
		nil,
		make([]Memtable, 0),
		make([]Memtable, 0),
		tablesToDelete,
		versionJson.WalOffset,
	)
}

func CopyVersion(src *Version) *Version {
	return NewVersion(
		src.levels,
		src.memtable,
		src.frozenMemtable,
		src.ImmutableMemtable,
		src.tablesToDelete,
		src.walOffset,
	)
}

func MaxSeqInVersion(src *Version) int {
	max := 0
	for _, level := range src.levels {
		for _, table := range level {
			_, _, seq, _ := parseSSTablePath(table.FullPath())
			if seq > max {
				max = seq
			}
		}
	}
	return max
}
