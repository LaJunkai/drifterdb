package drifterdb

//func TestLinearIndex_Find(t *testing.T) {
//	db := OpenDB("temp")
//	newTable := DumpTable(db.memtable, "temp", 5)
//	fmt.Println(newTable.dataBlockIndex)
//	result := newTable.dataBlockIndex.Find([]byte("4"))
//	if result != nil {
//		fmt.Printf("Found: (%v offset) (%v size)\n", result.offset, result.size)
//	} else {
//		fmt.Println("Not found")
//	}
//}