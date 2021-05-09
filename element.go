package drifterdb

import "github.com/LaJunkai/drifterdb/skiplist"

type Element struct {
	key interface{}
	value []byte
	ListEntry *skiplist.Entry
}

func ParseElement(key interface{}, value []byte, entry *skiplist.Entry) *Element {
	return &Element{key: key, value: value, ListEntry: entry}
}

func (e Element) Key() interface{} {
	return e.key
}

func (e Element) Value() []byte {
	return e.value
}