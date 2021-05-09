package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"encoding/binary"
	"hash/crc32"
)

func ElementToRowRecordBytes(e *Element, keyLength, valueLength int) ([]byte, int) {
	recordBytes := make([]byte, 4+1+1+4+8+keyLength+valueLength)
	i := 6
	binary.LittleEndian.PutUint32(recordBytes[i:i+4], uint32(keyLength))
	i += 4
	binary.LittleEndian.PutUint64(recordBytes[i:i+8], uint64(valueLength))
	i += 8
	// dirty mark for gc
	recordBytes[5] = 0
	// placeholder
	recordBytes[4] = byte(i - 6)
	i += copy(recordBytes[i:], common.TypeMVCCBytes.DumpBytes(e.Key()))
	i += copy(recordBytes[i:], e.Value())
	binary.LittleEndian.PutUint32(recordBytes, crc32.ChecksumIEEE(recordBytes[4:i]))
	return recordBytes[:i], i
}

func RowRecordBytesToElement(recordBytes []byte) []*Element {
	records := make([]*Element, 0)
	i := 0
	for ; i < len(recordBytes); {
		start := i
		crc := binary.LittleEndian.Uint32(recordBytes[i: i+4])
		i += 4
		// placeholder - 1
		_ = int(recordBytes[i])
		i += 1
		// dirty
		_ = recordBytes[i] != 0
		i += 1
		keyLength := int(binary.LittleEndian.Uint32(recordBytes[i: i+4]))
		i += 4
		valueLength := int(binary.LittleEndian.Uint64(recordBytes[i:i + 8]))
		i += 8
		key := common.ParseMVCCKey(recordBytes[i:i + keyLength])
		i += keyLength
		value := recordBytes[i: i+valueLength]
		i += valueLength
		//common.Debug("[row]", string(key.Content), ":", string(value))
		if !(crc == crc32.ChecksumIEEE(recordBytes[start + 4: i])) {
			common.Error("error occurred during parse sstable data: crc32 checksum does not match.")
		}
		records = append(records, &Element{
			key:       key,
			value:     value,
			ListEntry: nil,
		})
	}
	return records
}

func ElementsBinarySearch(elements []*Element, compare func(*Element) int) *Element {
	lo, hi := 0, len(elements)
	for lo < hi {
		mid := (lo + hi) / 2
		if cmp := compare(elements[mid]); cmp < 0 {
			lo = mid + 1
		} else if cmp > 0 {
			hi = mid
		} else {
			return elements[mid]
		}
	}
	return nil
}