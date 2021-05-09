package drifterdb

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/common"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"
)

const WALBufferSize = 1 << 7

type WALWriter struct {

	// w is the underlying file io writer
	w io.Writer
	// seq is the sequence number of the current log
	seq uint64
	// cursor is the current offset of the WAL log file.
	cursor uint64
	// i is the byte size that already filled in i,
	// during flush action, first i bytes in buffer will be flushed to the log file.
	i uint64
	// pending is whether the log is buffered but not written
	pending bool
	//bufferSize is the byte size of the buffer
	bufferSize uint64
	// buffer is the buffer.
	buffer []byte

	lock sync.Mutex

}

func NewWALWriter(w io.Writer) *WALWriter {
	return &WALWriter{
		w:          w,
		buffer:     make([]byte, WALBufferSize),
		bufferSize: WALBufferSize,
	}
}

func (wal *WALWriter) SetOffset(offset uint64) {
	wal.cursor = offset
}

func (wal *WALWriter) Offset() uint64 {
	return wal.cursor
}
func (wal *WALWriter) Op2Log(o *common.Operation) ([]byte, uint64) {
	var keyBytesLength = uint64(len(o.KeyBytes()))
	var valueBytesLength = uint64(len(o.ValueBytes()))
	// calculate the length of the new record, key/value length + key length + value length + operation type
	maxLength := 16 + keyBytesLength + valueBytesLength + 1
	log := make([]byte, maxLength)
	var i uint64 = 7
	// 0-4 crc32 checksum, 4-7 length of the kv lengths / for alignment, 8 opType
	i += uint64(binary.PutUvarint(log[i:], uint64(o.KeyType())))
	i += uint64(binary.PutUvarint(log[i:], keyBytesLength))
	i += uint64(binary.PutUvarint(log[i:], valueBytesLength))
	binary.PutUvarint(log[4:7], uint64(i-7))
	i += uint64(copy(log[i:], o.KeyBytes()))
	i += uint64(copy(log[i:], o.ValueBytes()))
	// calculate the crc32 checksum and write to the buffer
	binary.LittleEndian.PutUint32(log[:4], crc32.ChecksumIEEE(log[4:i]))
	return log[:i], i
}

// Flush function flush the log record from buffer to the disk and reset the buffer array.
func (wal *WALWriter) Flush()  {
	if wal.i != 0 {
		i, err := wal.w.Write(wal.buffer[:wal.i])
		if err != nil {
			panic(err)
		}
		wal.i = 0
		wal.cursor += uint64(i)
	}
}

// MakeRoom makes room for large kv record, it will call the Flush method at first
func (wal *WALWriter) MakeRoom(size uint64) {
	wal.Flush()
	wal.buffer = make([]byte, size)
	wal.bufferSize = size
}

// Append is the method to append operation records to the WAL log and return whether the newly appended record is flushed to the disk.
func (wal *WALWriter) Append(o *common.Operation) uint64 {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	// write WAL log if modified
	if o.KeyType() == common.OpPut || o.KeyType() == common.OpDelete {
		log, length := wal.Op2Log(o)
		if length+wal.i > wal.bufferSize {
			wal.Flush()
			if length > wal.bufferSize {
				wal.MakeRoom(length)
			}
		}
		wal.i += uint64(copy(wal.buffer[wal.i:], log))
		return length
	}
	return 0
}

type WALReader struct {
	// r is the underlying file io writer
	r io.Reader
	// seq is the sequence number of the current log
	seq uint64
	//bufferSize is the byte size of the buffer
	bufferSize uint64
	// buffer is the buffer.
	buffer []byte
}

func NewWALReader(reader io.Reader) *WALReader {
	return &WALReader{
		r: reader,
	}
}

func (wal *WALReader) Next() *common.Operation {
	header := make([]byte, 7)
	n, err := wal.r.Read(header)
	if n == 0{
		return nil
	}
	if err != nil {
		panic(err)
	}
	rawCRC := binary.LittleEndian.Uint32(header[:4])
	kvll, _ := binary.ReadUvarint(bytes.NewReader(header[4:7]))
	kvBytes := make([]byte, kvll)
	_, err = wal.r.Read(kvBytes)
	if err != nil {
		panic(err)
	}
	kvbr := bytes.NewReader(kvBytes)
	opType, _ := binary.ReadUvarint(kvbr)
	keyLength, _ := binary.ReadUvarint(kvbr)
	valueLength, _ := binary.ReadUvarint(kvbr)
	contentBytes := make([]byte, 7+kvll+keyLength+valueLength)
	_, err = wal.r.Read(contentBytes[7+kvll:])
	common.Throw(err)
	copy(contentBytes[:7], header)
	copy(contentBytes[7:7+kvll], kvBytes)
	newCalCrc := crc32.ChecksumIEEE(contentBytes[4:])
	if newCalCrc != rawCRC {
		panic("crc32 checksums do not match")
	}
	return common.OperationRecord(
		int(opType), common.ParseMVCCKey(contentBytes[7+kvll:7+kvll+keyLength]), contentBytes[7+kvll+keyLength:7+kvll+keyLength+valueLength],
	)

}
