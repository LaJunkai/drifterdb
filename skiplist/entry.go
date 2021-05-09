package skiplist

import "unsafe"

// The entry of the skip list.
// The Entry doesn't contain the filed `score` for this skip list is only designed to be used as a map.
type Entry struct {
	EntryBase

	key   interface{}
	Value interface{}

	prev            *Entry
	higherLevelPrev *Entry
	list            *SkipList
}

// Base of the Entry or the skiplist
// Field levels maintains the hasNext Entry in all levels.
type EntryBase struct {
	levels []*Entry
}

func (e *EntryBase) Entry() *Entry {
	return (*Entry)(unsafe.Pointer(e))
}

// constructor
func NewEntry(list *SkipList, level int, key, value interface{}) *Entry {
	return &Entry{
		EntryBase: EntryBase{
			levels: make([]*Entry, level),
		},
		key:   key,
		Value: value,
		list:  list,
	}
}

// return hasNext entry
func (e Entry) NextEntry() *Entry {
	if len(e.levels) == 0 {
		return nil
	}
	return e.levels[0]
}

// return previous entry
func (e Entry) PrevEntry() *Entry {
	return e.prev
}

// return hasNext entry at specified level
func (e Entry) LevelNext(level int) *Entry {
	if level < 0 || level > len(e.levels) {
		return nil
	}
	return e.levels[level]
}

// return previous entry at specified level
func (e Entry) LevelPrev(level int) *Entry {
	if level == 0 {
		return e.prev
	}

	if level < 0 || level > len(e.levels) {
		return nil
	}
	prev := e.prev
	for prev != nil {
		if len(prev.levels) > level {
			return prev
		}
		prev = prev.prev
	}
	// search back to the head, return nil
	return prev
}

func (e Entry) Key() interface{} {
	return e.key
}

func (e Entry) Level() int {
	return len(e.levels)
}
