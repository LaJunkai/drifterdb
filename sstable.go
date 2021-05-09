package drifterdb

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/bloomfilter"
	"github.com/LaJunkai/drifterdb/common"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

/*
magic string
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|  h   |  o   |  m   |  e   |  .   |  d   |  r   |  i   |  f   |  t   |  e   |  r   |  .   |  v   |  i   |  p   |


header block (include header length / index length / filter length)
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|       header length       |        index length       |       filter length       |    data length (8bytes)

| 16   | 17   | 18   | 19   | 20   | 21   | 22   | 23   | 24   | 25   | 26   | 27   | 28   | 29   | 30   | 31   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
        data length         |       minKey length       |                     minKey and maxKey......


index block (similar concept to the page directory in other db engines)
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|        key length         |                        offset                         |            key......


data record
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|          checksum         | kvll | drty |    key length      |      |      |      |      |      |      |      |

*/

const MagicString = "home.drifter.vip"
const MagicLength = 16
const BloomFilterK = 4
const BlockSize = 1 << 12
const LL = 24

type Block struct {
	offset uint64
	size   uint64
	table  *Table
}

func (b *Block) LoadBytes() []byte {
	buffer := make([]byte, b.size)
	_, err := b.table.file.ReadAt(buffer, int64(b.offset))
	common.Throw(err)
	return buffer
}

type Header struct {
	Block
	headerLength int
	indexBlock   *Block
	filterBlock  *Block
	dataBlock    *Block
	minKeyBlock  *Block
	maxKeyBlock  *Block
}

type Table struct {
	path             string
	tableSeq         int
	dataBlockIndex   BlockIndex
	filter           *bloomfilter.Filter
	file             *os.File
	header           *Header
	min, max         common.MVCCKey
	countVersionRefs int
	level            int
}

func (t *Table) LoadHeaderInfo() {

	headerBytes := make([]byte, LL)
	_, err := t.file.ReadAt(headerBytes, MagicLength)
	common.Throw(err)
	headerLength := binary.LittleEndian.Uint32(headerBytes[0:4])
	indexBlockLength := binary.LittleEndian.Uint32(headerBytes[4:8])
	filterBlockLength := binary.LittleEndian.Uint32(headerBytes[8:12])
	dataBlockLength := binary.LittleEndian.Uint64(headerBytes[12:20])
	minKeyLength := binary.LittleEndian.Uint32(headerBytes[20:24])
	t.header = &Header{
		Block:        Block{offset: MagicLength, size: uint64(headerLength), table: t},
		headerLength: int(headerLength),
		indexBlock: &Block{
			table:  t,
			size:   uint64(indexBlockLength),
			offset: uint64(MagicLength + LL),
		},
		filterBlock: &Block{
			table:  t,
			size:   uint64(filterBlockLength),
			offset: uint64(MagicLength + LL + indexBlockLength),
		},
		minKeyBlock: &Block{
			offset: uint64(MagicLength + LL + indexBlockLength + filterBlockLength),
			size:   uint64(minKeyLength),
			table:  t,
		},
		maxKeyBlock: &Block{
			offset: uint64(MagicLength + LL + indexBlockLength + filterBlockLength + minKeyLength),
			size:   uint64(headerLength - LL - indexBlockLength - filterBlockLength - minKeyLength),
			table:  t,
		},
		dataBlock: &Block{
			table:  t,
			size:   dataBlockLength,
			offset: uint64(MagicLength + headerLength),
		},
	}
}

// load index block && filter block && minKey && maxKey && data block
func (t *Table) LoadFullHeader() {
	br := &BlockReader{}
	indexBytes := make([]byte, t.header.indexBlock.size)
	filterBytes := make([]byte, t.header.filterBlock.size)
	minKeyBytes := make([]byte, t.header.minKeyBlock.size)
	maxKeyBytes := make([]byte, t.header.maxKeyBlock.size)
	br.Prepare(t.header.indexBlock, indexBytes)
	br.Prepare(t.header.filterBlock, filterBytes)
	br.Prepare(t.header.minKeyBlock, minKeyBytes)
	br.Prepare(t.header.maxKeyBlock, maxKeyBytes)
	br.Read()

	t.dataBlockIndex = NewLinearIndex(indexBytes, t, t.header.dataBlock.size, minKeyBytes, maxKeyBytes)
	t.dataBlockIndex.SetBaseOffset(t.header.dataBlock.offset)
	t.filter = bloomfilter.LoadFilterFromBytes(filterBytes)
	t.min = *common.ParseMVCCKey(minKeyBytes)
	t.max = *common.ParseMVCCKey(maxKeyBytes)
}

func (t *Table) Get(key *common.MVCCKey) *Element {
	if cmp := bytes.Compare(key.Content, t.max.Content); cmp > 0 {
		return nil
	}
	if cmp := bytes.Compare(key.Content, t.min.Content); cmp < 0 {
		return nil
	}
	if existed := t.filter.Exists(key.Content); !existed {
		return nil
	}
	// read from the disk
	_, targetBlock := t.dataBlockIndex.Find(key.Content)
	if targetBlock == nil {
		return nil
	} else {
		recordsBytes := targetBlock.LoadBytes()
		elements := RowRecordBytesToElement(recordsBytes)
		return ElementsBinarySearch(elements, func(element *Element) int {
			return bytes.Compare(element.key.(*common.MVCCKey).Content, key.Content)
		})
	}

}

func (t *Table) Range(start, end *common.MVCCKey, count, offset int) []*Element {
	if cmp := bytes.Compare(start.Content, t.max.Content); cmp > 0 {
		return nil
	}
	if cmp := bytes.Compare(end.Content, t.min.Content); cmp < 0 {
		return nil
	}
	result := make([]*Element, 0, count)
	currentCount, currentOffset := 0, 0
	var prev *common.MVCCKey = nil
	for index, targetBlock := t.dataBlockIndex.Find(start.Content); targetBlock != nil; targetBlock = t.dataBlockIndex.GetByIndex(index) {
		recordsBytes := targetBlock.LoadBytes()
		elements := RowRecordBytesToElement(recordsBytes)
		for _, element := range elements {
			elementKey := element.Key().(*common.MVCCKey)
			if common.TypeMVCCBytes.ModifyCompare(start, elementKey) <= 0 {
				if common.TypeMVCCBytes.ModifyCompare(end, elementKey) > 0 {
					if currentOffset >= offset {
						if prev != nil && bytes.Equal(prev.Content, elementKey.Content) {
							continue
						} else {
							prev = elementKey
						}
						if elementKey.KT != common.OpDelete {
							result = append(result, element)
							currentCount += 1
							if currentCount >= count {
								return result
							}
						}
					} else {
						currentOffset += 1
					}
				}
			}
		}
		index += 1
	}
	return result
}

func IndexBlockRecord(key []byte, offset uint64) []byte {
	indexBytes := make([]byte, 4+8+len(key))
	binary.LittleEndian.PutUint32(indexBytes[:4], uint32(len(key)))
	binary.LittleEndian.PutUint64(indexBytes[4:12], offset)
	copy(indexBytes[12:], key)
	return indexBytes
}

func parseSSTablePath(path string) (basePath, tableName string, seq int, level int) {
	tableFullPath := strings.Split(strings.Replace(path, "\\", "/", math.MaxInt32), "/")
	basePath = filepath.Join(tableFullPath[:len(tableFullPath)-1]...)
	tableName = tableFullPath[len(tableFullPath)-1]
	_, err := fmt.Sscanf(tableName, "%02dL%010d.sst", &level, &seq)
	common.Debug("[parse sstable path]",tableName, level, seq)
	common.Throw(err)
	return
}

func LoadTable(path string) *Table {
	file, err := os.OpenFile(path, os.O_RDONLY, 0777)
	common.Throw(err)
	basePath, _, seq, level := parseSSTablePath(path)
	newTable := &Table{
		path:     basePath,
		file:     file,
		tableSeq: seq,
		level:    level,
	}
	newTable.LoadHeaderInfo()
	newTable.LoadFullHeader()
	common.Debug("[load table]", "index block", newTable.header.indexBlock)
	common.Debug("[load table]", "filter block", newTable.header.filterBlock)
	common.Debug("[load table]", "minKey block", newTable.header.minKeyBlock)
	common.Debug("[load table]", "maxKey block", newTable.header.maxKeyBlock)
	common.Debug("[load table]", "data block", newTable.header.dataBlock)
	return newTable
}

func TableFullPath(path string, level, seq int) string {
	return filepath.Join(path, fmt.Sprintf("%02dL%010d.sst", level, seq))
}

func (t *Table) FullPath() string {
	return TableFullPath(t.path, t.level, t.tableSeq)
}

func DumpTable(memtable Memtable, path string, memtableSeq int) *Table {
	start := time.Now()
	defer func() {
		common.Debug("[dump table]", "time cost: ", time.Since(start).Seconds(), "s")
	}()
	const initDataBytesSize = 500 * KB
	// filename/header should be assigned by the storage object.
	iterator := memtable.Iterator()
	newTable := &Table{
		path:     path,
		tableSeq: memtableSeq,
		filter:   bloomfilter.NewFrozenFilter(common.MaxInt(int(math.Log2(float64(memtable.Size()))), 7), BloomFilterK),
		file:     nil,
		header:   nil,
		level:    0,
	}
	offset := 0
	prev := 0
	tableFile, err := os.OpenFile(
		newTable.FullPath(),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL,
		0777)
	defer func() {
		tableFile.Close()
	}()
	common.Throw(err)
	// generate data block and index block
	dataBytes := make([]byte, initDataBytesSize)
	dataBytesCursor := 0
	indexBytes := make([]byte, 0)
	for ; iterator.HasNext(); {
		e := iterator.Next()
		// generate index block
		keyBytes := common.TypeMVCCBytes.DumpBytes(e.Key())
		if offset >= BlockSize {
			prev += offset / BlockSize
			offset %= BlockSize
			indexOffset := uint64(prev)*uint64(BlockSize) + uint64(offset)
			indexBytes = common.ConcatBytes(
				indexBytes, IndexBlockRecord(
					keyBytes, indexOffset,
				),
			)
		}
		// add key to the bloom filter
		newTable.filter.Add(keyBytes)
		//
		keyLength := len(keyBytes)
		valueLength := len(e.Value())
		// make redundant space for the record
		recordBytes, rLength := ElementToRowRecordBytes(e, keyLength, valueLength)
		if dataBytesCursor + rLength >= len(dataBytes) {
			newDataBytes := make([]byte, len(dataBytes) + initDataBytesSize)
			copy(newDataBytes, dataBytes)
			dataBytes = newDataBytes
		}
		dataBytesCursor += copy(dataBytes[dataBytesCursor:], recordBytes)
		offset += rLength
	}
	dataBytes = dataBytes[:dataBytesCursor]

	// setup block index
	minKey := common.TypeMVCCBytes.DumpBytes(memtable.First().Key())
	maxKey := common.TypeMVCCBytes.DumpBytes(memtable.Last().Key())
	newTable.dataBlockIndex = NewLinearIndex(
		indexBytes,
		newTable,
		uint64(len(dataBytes)),
		minKey,
		maxKey,
	)
	//
	filterBytes := newTable.filter.DumpBytes()
	filterBlockLength := len(filterBytes)
	headerBytes := make([]byte, LL)
	indexBlockLength := len(indexBytes)
	headerLength := LL + indexBlockLength + filterBlockLength + len(minKey) + len(maxKey)
	binary.LittleEndian.PutUint32(headerBytes[:4], uint32(headerLength))
	binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(indexBlockLength))
	binary.LittleEndian.PutUint32(headerBytes[8:12], uint32(filterBlockLength))
	binary.LittleEndian.PutUint32(headerBytes[12:20], uint32(len(dataBytes)))
	binary.LittleEndian.PutUint32(headerBytes[20:24], uint32(len(minKey)))
	// patch, forget the data length in the previous implementation Orz.
	// write table
	common.UnsafeWrite(tableFile, []byte(MagicString))
	common.UnsafeWrite(tableFile, headerBytes)
	common.UnsafeWrite(tableFile, indexBytes)
	common.UnsafeWrite(tableFile, filterBytes)
	common.UnsafeWrite(tableFile, minKey)
	common.UnsafeWrite(tableFile, maxKey)
	common.UnsafeWrite(tableFile, dataBytes)
	return newTable
}

// RemoveFile remove file that is deprecated by the compaction procedure.
func (t *Table) RemoveFile() {
	os.Remove(t.path)
}
