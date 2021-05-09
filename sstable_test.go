package drifterdb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestDumpTable(t *testing.T) {
	db := OpenDB("temp")
	db.Put([]byte("9"), []byte("lajunkai"))
	db.Put([]byte("8"), []byte("21"))
	db.Put([]byte("1"), []byte("student"))
	db.Put([]byte("4"), []byte("programmer"))
	table := DumpTable(db.memtable, "temp", 1)
	fmt.Println(table.filter.DumpBytes())
}

func TestLoadTable(t *testing.T) {
	_ = os.Remove("temp/0000000001.sst")
	db := OpenDB("temp")
	db.Put([]byte("9"), []byte("lajunkai"))
	db.Put([]byte("8"), []byte("21"))
	db.Put([]byte("1"), []byte("student"))
	db.Put([]byte("4"), []byte("programmer"))
	table := DumpTable(db.memtable, "temp", 1)
	anoTable := LoadTable("temp/0000000001.sst")
	fmt.Println(table.filter.DumpBytes())
	fmt.Println(anoTable.filter.DumpBytes())
	if !bytes.Equal(table.filter.DumpBytes(), anoTable.filter.DumpBytes()) {
		t.Error("filter load error")
	}
	if !table.filter.Exists([]byte("4")) {
		t.Error("4 is supposed to be existed in the filter")
	}
	fmt.Println(table.filter.Exists([]byte("4")))

}

func TestLoadTable2(t *testing.T) {

}