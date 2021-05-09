package bloomfilter

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/common"
	"encoding/binary"
	"fmt"
	"github.com/steakknife/hamming"
	"log"
	"math"
	"reflect"
)
/*
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|  k   |                        counter                        |                      data                      |

*/
type Filter struct {
	// k is the number of the hash functions
	// 2 ** m is the max number of the elements
	// counter is the current number of the elements
	bits     []uint64
	counter  uint64
	m        int
	mask     uint64
	k        int
	hashPool *HashFunctionsPool
}

func (f *Filter) hashes(key interface{}) []uint64 {
	var keyBytes []byte

	switch key.(type) {
	case string:
		keyBytes = []byte(key.(string))
	case []byte:
		keyBytes = key.([]byte)
	default:
		panic(fmt.Sprintf("Invalid type [%v] of the key\n", reflect.TypeOf(key)))
	}

	hashValues := make([]uint64, f.k)
	for i := 0; i < f.k; i += 1 {
		hashValues[i] = f.hashPool.GetHashFunctionByIndex(i)(keyBytes)
	}
	return hashValues
}

func (f *Filter) Add(key interface{}) {
	for _, i := range f.hashes(key) {
		i &= f.mask
		f.bits[i>>6] |= 1 << (i & uint64(0x3f))
	}
	f.counter++
}

func (f *Filter) Exists(key interface{}) bool {
	found := true
	for _, i := range f.hashes(key) {
		i &= f.mask
		found = found && (f.bits[i>>6]&(1<<(i&0x3f))) != 0
	}
	return found
}

func (f *Filter) PreciseFilledRatio() float64 {
	return float64(hamming.CountBitsUint64s(f.bits)) / float64(f.m)

}

func (f *Filter) Count() uint64 {
	return f.counter
}

func (f *Filter) DumpBytes() []byte {
	fullBytes := make([]byte, len(f.bits) * 8 + 9)
	binary.PutUvarint(fullBytes[:1], uint64(f.k))
	binary.LittleEndian.PutUint64(fullBytes[1:9], f.counter)
	buffer := fullBytes[9:]
	for i, value := range f.bits {
		binary.BigEndian.PutUint64(buffer[i*8: i*8+8], value)
	}
	return fullBytes
}

func LoadFilterFromBytes(src []byte) *Filter {
	uintArrayLength := (len(src) - 9) / 8
	m := int(math.Log2(float64(uintArrayLength)))
	pool := NewHashFunctionsPool()
	bitsArray := make([]uint64, uintArrayLength)
	dataPart := src[9:]
	for i := 0; i < uintArrayLength; i++ {
		bitsArray[i] = binary.BigEndian.Uint64(dataPart[i*8: i*8+8])
	}
	k, err := binary.ReadUvarint(bytes.NewReader(src[:1]))
	counter := binary.LittleEndian.Uint64(src[1:9])
	common.Throw(err)
	return &Filter{
		m:        m,
		mask:     0xFFFFFFFFFFFFFFFF >> (64 - m),
		counter:  counter,
		bits:     bitsArray,
		k:        int(k),
		hashPool: pool,

	}
}

func NewFrozenFilter(m int, k int) *Filter {
	pool := NewHashFunctionsPool()
	if k > pool.Size() {
		log.Panicf("unsupported k value of %v, k should no larger than (%v)", k, pool.Size())
	}
	//fmt.Println("filter size:", 1<<(m-6), "uint64")
	return &Filter{
		m:        m,
		mask:     0xFFFFFFFFFFFFFFFF >> (64 - m),
		counter:  0,
		bits:     make([]uint64, 1<<(m-6)),
		k:        k,
		hashPool: pool,
	}
}
