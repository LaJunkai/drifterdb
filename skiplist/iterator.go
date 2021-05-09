package skiplist

type ListIterator struct {
	currentEntry    *Entry
	list            *SkipList
	currentPosition int
	hasNext         bool
}

func NewListIterator(list *SkipList) *ListIterator {
	return &ListIterator{
		currentEntry:    list.levels[0],
		list:            list,
		currentPosition: 0,
		hasNext:         list.levels[0] != nil,
	}
}

func (iterator *ListIterator) Size() int {
	return iterator.list.length
}

func (iterator *ListIterator) Tell() int {
	return iterator.currentPosition
}

// get one element from the iterator and then make the cursor move forward
func (iterator *ListIterator) Get() *Entry {
	defer func() {
		iterator.currentPosition += 1
		iterator.currentEntry = iterator.currentEntry.levels[0]
		iterator.hasNext = iterator.currentEntry != nil
	}()
	return iterator.currentEntry
}

func (iterator *ListIterator) HasNext() bool {
return iterator.hasNext
}
