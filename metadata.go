package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

const MetaBackups = 3

type Meta struct {
	// work directory
	MetaVersion   int    `json:"meta_version"`
	WorkDir       string `json:"work_dir"`
	WalSeq        int    `json:"wal_seq"`
	WalCheckPoint uint64 `json:"wal_check_point"`
	SstableSeq    int    `json:"sstable_seq"`
}

func LoadMeta(workDir string, current string) *Meta {
	metas, err := filepath.Glob("temp/meta*")
	common.Throw(err)
	if len(metas) == 0 {
		common.Always("No meta file found. Init a new meta file.")
		newMeta := &Meta{
			WorkDir:       workDir,
			MetaVersion:   0,
			WalSeq:        0,
			WalCheckPoint: 0,
			SstableSeq:    0,
		}
		newMeta.Flush()
		return newMeta
	} else {
		sort.Strings(metas)
		targetPath := ""
		for _, meta := range metas {

			if meta == filepath.Join(workDir, "meta"+current) {
				targetPath = meta
				break
			}
		}
		if targetPath == "" {
			targetPath = metas[len(metas)-1]
			common.Always("Specified meta file not found, use newest version(", targetPath, " )instead")
		} else {
			common.Always("Use specified meta file: ", targetPath)
		}
		metaBytes, err := ioutil.ReadFile(targetPath)
		common.Throw(err)
		theMeta := &Meta{}
		err = json.Unmarshal(metaBytes, theMeta)
		common.Throw(err)
		theMeta.MetaVersion += 1
		return theMeta
	}
}

func (m *Meta) Clear() {
	metas, err := filepath.Glob("temp/meta*")
	common.Throw(err)
	currentMetaFilename := filepath.Join(m.WorkDir, fmt.Sprintf("meta%010d", m.MetaVersion))
	sort.Strings(metas)
	for i, meta := range metas {
		if meta < currentMetaFilename {
			if i < len(metas) -MetaBackups {
				err := os.Remove(meta)
				common.Throw(err)
			}
		}
	}
}

func (m *Meta) Flush() {
	metaBytes, err := json.Marshal(m)
	common.Throw(err)
	err = ioutil.WriteFile(filepath.Join(m.WorkDir, fmt.Sprintf("meta%010d", m.MetaVersion)), metaBytes, 0777)
	common.Throw(err)
	m.Clear()
}
