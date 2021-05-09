package drifterdb

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/common"
	"encoding/binary"
)

/*
BlockIndex is the index structure for the data blocks of the sstable.
Constructor of the BlockIndex is supposed to setup the max boundary.
*/
type IBlock struct {
	Block
	minRecord []byte
}

type BlockIndex interface {
	Min() []byte
	Max() []byte
	Find([]byte) (int, *Block)
	SetBaseOffset(uint64)
	GetByIndex(index int) *Block
}

type LinearIndex struct {
	maxKey           []byte
	minKey           []byte
	indexBlocks      []*IBlock
	baseOffsetSetted bool
}

func (l *LinearIndex) Min() []byte {
	return common.ParseMVCCKey(l.minKey).Content
}

func (l *LinearIndex) Max() []byte {
	return common.ParseMVCCKey(l.maxKey).Content
}

func (l *LinearIndex) Find(key []byte) (int, *Block) {
	if len(l.indexBlocks) == 0 || common.TypeBytes.QueryCompare(key, l.Min()) < 0 || common.TypeBytes.QueryCompare(key, l.Max()) > 0 {
		return -1, nil
	}
	lo, hi := 0, len(l.indexBlocks)
	for lo < hi {
		mid := (lo + hi) / 2
		current := l.indexBlocks[mid]
		//common.Debug("[index find]", current.offset, current.size, string(common.ExtractMVCCKeyContent(current.minRecord)))
		if cmp := bytes.Compare(common.ExtractMVCCKeyContent(current.minRecord), key); cmp < 0 {
			lo = mid + 1
		} else if cmp > 0 {
			hi = mid
		} else {
			return mid, &current.Block
		}
	}
	if bytes.Compare(common.ExtractMVCCKeyContent(l.indexBlocks[lo].minRecord), key) > 0 {
		lo -= 1
	}
	return lo, &l.indexBlocks[lo].Block
}

func (l *LinearIndex) GetByIndex(index int) *Block {
	if index >= 0 && index < len(l.indexBlocks) {
		return &l.indexBlocks[index].Block
	} else {
		return nil
	}
}
func (l *LinearIndex) SetBaseOffset(baseOffset uint64) {
	if !l.baseOffsetSetted {
		for _, theBlock := range l.indexBlocks {
			theBlock.offset += baseOffset
		}
		l.baseOffsetSetted = true
	}
}

func NewLinearIndex(indexBytes []byte, table *Table, dataBlockSize uint64, minKey, maxKey []byte) *LinearIndex {
	idx := &LinearIndex{
		maxKey:      maxKey,
		minKey:      minKey,
		indexBlocks: make([]*IBlock, 0),
	}
	var cursor uint64 = 0
	indexBytesLen := uint64(len(indexBytes))
	var firstIndexSize uint64 = 0
	// setup the size of the first i-block if there is no other i-blocks.
	if len(indexBytes) == 0 {
		firstIndexSize = dataBlockSize
	}
	// setup the first i-block, but size may be not filled.
	idx.indexBlocks = append(
		idx.indexBlocks,
		&IBlock{
			Block: Block{
				offset: 0,
				size:   firstIndexSize,
				table:  table,
			},
			minRecord: minKey,
		},
	)
	for cursor < indexBytesLen {
		keyLength := binary.LittleEndian.Uint32(indexBytes[cursor : cursor+4])
		cursor += 4
		iOffset := binary.LittleEndian.Uint64(indexBytes[cursor : cursor+8])
		cursor += 8
		iKey := make([]byte, keyLength)
		cursor += uint64(copy(iKey, indexBytes[cursor:cursor+uint64(keyLength)]))
		if len(idx.indexBlocks) > 0 {
			prev := idx.indexBlocks[len(idx.indexBlocks)-1]
			prev.Block.size = iOffset - prev.Block.offset
		}
		// update size of the first i-block if there is other i-blocks.
		if len(idx.indexBlocks) == 1 {
			idx.indexBlocks[0].Block.size = iOffset
		}
		//
		idx.indexBlocks = append(
			idx.indexBlocks,
			&IBlock{
				Block: Block{
					offset: iOffset,
					size:   0,
					table:  table,
				},
				minRecord: iKey,
			},
		)
	}
	if len(indexBytes) > 0 {
		prev := idx.indexBlocks[len(idx.indexBlocks)-1]
		prev.Block.size = dataBlockSize - prev.Block.offset
	}
	return idx
}
