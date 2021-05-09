package drifterdb

import (
	"github.com/LaJunkai/drifterdb/common"
	"github.com/LaJunkai/drifterdb/skiplist"
	"fmt"
	"sync"
)

type MemtableIterator interface {
	First() *Element
	Back() *Element
	Next() *Element
	HasNext() bool
}

type SkiplistMemtableIterator struct {
	memtable *SkiplistMemtable
	iterator *skiplist.ListIterator
}

func (s *SkiplistMemtableIterator) First() *Element {
	if e := s.memtable.list.First(); e != nil {
		return &Element{
			key:   e.Key(),
			value: e.Value.([]byte),
		}
	} else {
		return nil
	}
}

func (s *SkiplistMemtableIterator) Back() *Element {
	if e := s.memtable.list.Back(); e != nil {
		return &Element{
			key:   e.Key(),
			value: e.Value.([]byte),
		}
	} else {
		return nil
	}
}

func (s *SkiplistMemtableIterator) Next() *Element {
	if e := s.iterator.Get(); e != nil {
		return &Element{
			key:   e.Key(),
			value: e.Value.([]byte),
		}
	} else {
		return nil
	}
}

func (s *SkiplistMemtableIterator) HasNext() bool {
	return s.iterator.HasNext()
}

type Memtable interface {
	Put(key interface{}, value []byte) (*Element, bool)
	Get(key interface{}) *Element
	Delete(key interface{}) *Element
	Range(start, end interface{}, count, offset int) []*Element
	Exists(key interface{}) bool
	Size() int
	BytesSize() int
	IncreaseBytesSize(delta int)
	Iterator() MemtableIterator
	First() *Element
	Last() *Element
	Ref(transaction *Transaction)
	CancelRef(transaction *Transaction)
	CountRefs() int
	InlinePreview()

}

type SkiplistMemtable struct {
	comparable common.Comparable
	list       *skiplist.SkipList
	byteSize   int
	mutable    bool
	RefTrx     sync.Map
	countRefs  int
	walOffset uint64
}

func (s *SkiplistMemtable) InlinePreview() {
	i := s.Iterator()
	for ; i.HasNext(); {
		n := i.Next()
		k := n.Key().(*common.MVCCKey)
		fmt.Printf("[%v](%v, %v): [%v]     ", string(k.Content), k.Seq, k.KT, string(n.Value()))
	}
	fmt.Println()
}

func (s *SkiplistMemtable) CountRefs() int {
	return s.countRefs
}

func (s *SkiplistMemtable) Ref(trx *Transaction) {
	if _, existed := s.RefTrx.Load(trx); !existed {
		s.RefTrx.Store(trx, nil)
		s.countRefs += 1
	}
}

func (s *SkiplistMemtable) CancelRef(trx *Transaction) {
	if _, existed := s.RefTrx.Load(trx); existed {
		s.RefTrx.Delete(trx)
		s.countRefs -= 1
	}
}

func (s *SkiplistMemtable) First() *Element {
	if result := s.list.First(); result != nil {
		return &Element{
			key:   result.Key(),
			value: result.Value.([]byte),
		}
	} else {
		return nil
	}
}

func (s *SkiplistMemtable) Last() *Element {
	if result := s.list.Back(); result != nil {
		return &Element{
			key:   result.Key(),
			value: result.Value.([]byte),
		}
	} else {
		return nil
	}
}

func (s *SkiplistMemtable) Size() int {
	return s.list.Length()
}

func (s *SkiplistMemtable) BytesSize() int {
	return s.byteSize
}

func (s *SkiplistMemtable) IncreaseBytesSize(delta int) {
	s.byteSize += delta
}

func (s *SkiplistMemtable) SetupBytesSize(key interface{}, value []byte, increase bool) {
	if increase {
		s.byteSize += s.comparable.ByteSizes(key) + len(value) + 16
	} else {
		s.byteSize -= s.comparable.ByteSizes(key) + len(value) + 16
	}
}

func NewSkiplistMemtable(comparable common.Comparable) *SkiplistMemtable {
	return &SkiplistMemtable{
		list:       skiplist.NewSkipList(comparable),
		comparable: comparable,
	}
}

func (s *SkiplistMemtable) Put(key interface{}, value []byte) (*Element, bool) {
	entry, success := s.list.Set(key, value)
	if entry != nil {
		return ParseElement(entry.Key(), entry.Value.([]byte), entry), success
	} else {
		return nil, success
	}
}

func (s *SkiplistMemtable) Get(key interface{}) *Element {
	entry := s.list.GetEntry(key)

	if entry != nil {
		return ParseElement(key, entry.Value.([]byte), entry)
	} else {
		return nil
	}
}

func (s *SkiplistMemtable) Delete(key interface{}) *Element {
	entry := s.list.Delete(key)
	if entry != nil {
		return ParseElement(entry.Key(), entry.Value.([]byte), entry)
	} else {
		return nil
	}
}

func (s *SkiplistMemtable) Range(start, end interface{}, count, offset int) []*Element {
	skiplistResult := s.list.Range(start, end, count + offset, 0)
	result := make([]*Element, 0, len(skiplistResult))
	for _, entry := range skiplistResult {
		result = append(result, &Element{key: entry.Key(), value: entry.Value.([]byte), ListEntry: entry})
	}
	return result
}

func (s *SkiplistMemtable) Exists(key interface{}) bool {
	return s.list.Exists(key)
}

func (s *SkiplistMemtable) Iterator() MemtableIterator {
	return &SkiplistMemtableIterator{
		memtable: s,
		iterator: s.list.Iterator(),
	}
}
