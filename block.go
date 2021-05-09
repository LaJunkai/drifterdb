package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
)

// An util struct to read several sequential blocks in the way of a single sequential io operation call.
type BlockReader struct {
	blocks  []*Block
	targets [][]byte
}

func (br *BlockReader) Prepare(b *Block, target []byte) {
	br.blocks = append(br.blocks, b)
	br.targets = append(br.targets, target)
}

// Read method return nothing but write the result into the bytes array passed by Prepare method
func (br *BlockReader) Read() {
	table := br.blocks[0].table
	startOffset := br.blocks[0].offset
	endOffset := br.blocks[len(br.blocks) - 1].offset
	for i := 1; i < len(br.blocks); i++ {
		if table != br.blocks[i].table {
			panic("supplied block belong to multiple tables")
		} else if br.blocks[i].offset <= endOffset {
			endOffset = br.blocks[i].size + br.blocks[i].offset
		} else {
			panic("supplied blocks is not adjacent")
		}
	}
	buf := make([]byte, endOffset-startOffset)
	_, err := table.file.ReadAt(buf, int64(startOffset))
	common.Throw(err)
	for idx, b := range br.blocks {
		i := b.offset - startOffset
		copy(br.targets[idx], buf[i:i+b.size])
	}
}
