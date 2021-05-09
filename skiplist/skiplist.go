package skiplist

import (
	"bytes"
	"github.com/LaJunkai/drifterdb/common"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sync"
	"time"
)

var DefaultMaxLevel = 48

/*
SkipList support mvcc by using TypeMVCCBytes as the keyType of the SkipList instance.
*/
type SkipList struct {
	EntryBase

	keyType  common.Comparable
	maxLevel int
	length   int
	rand     *rand.Rand
	back     *Entry

	levelCounter []int

	// RWMutex for concurrency control
	concurrent bool
	lock       sync.RWMutex
}

func NewSkipList(elementType common.Comparable) *SkipList {
	return &SkipList{
		EntryBase: EntryBase{
			levels: make([]*Entry, DefaultMaxLevel),
		},
		keyType:      elementType,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel:     DefaultMaxLevel,
		levelCounter: make([]int, DefaultMaxLevel, DefaultMaxLevel),
		concurrent:   false,
		lock:         sync.RWMutex{},
	}
}
func ConcurrentSkipList(elementType common.Comparable) *SkipList {
	return &SkipList{
		EntryBase: EntryBase{
			levels: make([]*Entry, DefaultMaxLevel),
		},
		keyType:      elementType,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel:     DefaultMaxLevel,
		levelCounter: make([]int, DefaultMaxLevel, DefaultMaxLevel),
		concurrent:   true,
		lock:         sync.RWMutex{},
	}
}

// get the level assigned for a new element, no mutex to acquired for rand package is implemented with lock for sync.
func (list *SkipList) randomLevel() int {
	level := 1
	var threshold = math.MaxInt32 / 2
	var randomValue = int(list.rand.Int31())
	var estimated = bits.Len(uint(list.length)) * 4
	if estimated > list.maxLevel {
		estimated = list.maxLevel
	}
	for ; randomValue < threshold && level <= estimated; {
		level += 1
		if level%16 == 0 {
			threshold = math.MaxInt32 / 2
			randomValue = int(list.rand.Int31())
		} else {
			threshold = threshold / 2
		}
	}
	list.levelCounter[level-1] += 1
	return level
}

func (list *SkipList) TraditionalRandomLevel() (level int) {
	level = 1
	if list.maxLevel <= 1 {
		return
	}
	var threshold = math.MaxInt32 / 2

	for randomValue := int(list.rand.Int31()); randomValue < threshold && level <= list.maxLevel; randomValue = int(list.rand.Int31()) {
		level += 1
	}
	list.levelCounter[level-1] += 1
	return
}

func (list *SkipList) Set(key, value interface{}) (*Entry, bool) {
	// simply setup the list if the list is empty
	// <to be implemented> -- ensure that the first entry of the list always reach the top level
	if list.length == 0 {
		// double check
		checkPass := true
		if list.concurrent {
			list.lock.Lock()
			if list.length != 0 {
				list.lock.Unlock()
				checkPass = false
			}
		}
		// check pass, commit the insert
		if checkPass {
			randomLevel := list.randomLevel()
			newEntry := NewEntry(list, randomLevel, key, value)
			for i := 0; i < randomLevel; i++ {
				list.EntryBase.levels[i] = newEntry
				list.back = newEntry
			}
			list.length += 1
			if list.concurrent {
				list.lock.Unlock()
			}
			return newEntry, true
		}

	}

	// previousLevels: the previous entry of every level
	previousLevels := make([]*EntryBase, list.maxLevel)
	if list.concurrent {
		list.lock.RLock()
	}
	currentEntry := &list.EntryBase
	// search the position at every level for insertion
	for i := list.maxLevel - 1; i >= 0; {
		for nextEntry := currentEntry.levels[i]; nextEntry != nil; nextEntry = currentEntry.levels[i] {
			if comp := list.keyType.ModifyCompare(key, nextEntry.key); comp <= 0 {
				if comp == 0 {
					nextEntry.Value = value
					if list.concurrent {
						list.lock.Unlock()
					}
					return nextEntry, true
				}
				break
			}
			currentEntry = &nextEntry.EntryBase
		}
		previousLevels[i] = currentEntry
		// skip the level if they point the same entry as the higher level
		topLevel := currentEntry.levels[i]
		for i--; i >= 0 && currentEntry.levels[i] == topLevel; i-- {
			previousLevels[i] = currentEntry
		}
	}
	if list.concurrent {
		list.lock.RUnlock()
		list.lock.Lock()
	}
	randomLevel := list.randomLevel()

	newEntry := NewEntry(list, randomLevel, key, value)
	// commit the insert operation, acquire the lock if concurrent is true
	// setup prev field at level 0
	if previousLevels[0].levels[0] != nil {
		previousLevels[0].levels[0].prev = newEntry
		// **if there is another active trx edit the key, there must be a key record with smaller seq**
		if list.keyType == common.TypeMVCCBytes {
			mvccKey := key.(*common.MVCCKey)
			nextKey := previousLevels[0].levels[0].key.(*common.MVCCKey)
			if bytes.Equal(nextKey.Content, mvccKey.Content) && nextKey.TrxId != 0 && nextKey.TrxId != mvccKey.TrxId {
				return previousLevels[0].levels[0], false
			}
		}
	}
	if prev := previousLevels[0]; prev != &list.EntryBase {
		newEntry.prev = prev.Entry()
	}
	// let the new entry point to the previous hasNext entries
	// let the previous entries point the new entry
	for i := 0; i < randomLevel; i++ {
		newEntry.levels[i] = previousLevels[i].levels[i]
		previousLevels[i].levels[i] = newEntry
	}
	list.length += 1
	if list.concurrent {
		list.lock.Unlock()
	}
	// maintain the back pointer
	if newEntry.LevelNext(0) == nil {
		list.back = newEntry
	}
	return newEntry, true

}

func (list *SkipList) GetEntry(key interface{}) (result *Entry) {
	if list.concurrent {
		list.lock.RLock()
		defer list.lock.RUnlock()
	}
	currentEntry := &list.EntryBase
	// search the position at every level for insertion
	result = nil
	for i := list.maxLevel - 1; i >= 0; i -= 1 {
		for nextEntry := currentEntry.levels[i]; nextEntry != nil; nextEntry = currentEntry.levels[i] {
			if comp := list.keyType.QueryCompare(key, nextEntry.key); comp <= 0 {
				if comp == 0 {
					if list.keyType != common.TypeMVCCBytes {
						// common key type
						if list.keyType.OpType(nextEntry.key) == common.OpPut {
							result = nextEntry
						} else {
							result = nil
						}
					} else {
						// MVCC key type
						// isolation level
						mvccKey := key.(*common.MVCCKey)
						if mvccKey.IsoLevel == common.ReadUncommitted {
							// just same as the common key type
							// make sure seq is 0xFFFFFFFFFFFFFFFF or current max when iso level is read uncommitted to read uncommitted value.
							if list.keyType.OpType(nextEntry.key) == common.OpPut {
								result = nextEntry
							} else {
								result = nil
							}
						} else if mvccKey.IsoLevel == common.ReadCommitted {
							// add condition that the nextEntry is committed or edited by current trx: TrxId == 0
							if nextMVCCKey := nextEntry.key.(*common.MVCCKey);
								nextMVCCKey.TrxId == 0x00000000 || nextMVCCKey.TrxId == mvccKey.TrxId {
								//
								if list.keyType.OpType(nextEntry.key) == common.OpPut {
									result = nextEntry
								} else {
									result = nil
								}
								//
							}
						} else if mvccKey.IsoLevel == common.RepeatableRead {
							// repeatable read
							if nextMVCCKey := nextEntry.key.(*common.MVCCKey);
								(nextMVCCKey.Seq <= mvccKey.Seq && nextMVCCKey.TrxId == 0x00000000) &&
									nextMVCCKey.TrxId == mvccKey.TrxId {
								//
								if list.keyType.OpType(nextEntry.key) == common.OpPut {
									result = nextEntry
								} else {
									result = nil
								}
								//
							}
						}
					}
				}
				break
			}
			currentEntry = &nextEntry.EntryBase
		}
	}
	return
}

func (list *SkipList) Get(key interface{}) interface{} {
	if theEntry := list.GetEntry(key); theEntry != nil {
		return theEntry.Value
	} else {
		return nil
	}
}

func (list *SkipList) Exists(key interface{}) bool {
	return list.GetEntry(key) != nil
}

// delete the specified entry of the skip list, return the entry if the key exists other wise nil.
func (list *SkipList) Delete(key interface{}) *Entry {
	var specifiedEntry *Entry = nil
	// previousLevels: the previous entry of every level
	previousLevels := make([]*EntryBase, list.maxLevel)
	currentEntry := &list.EntryBase
	// search the position at every level for insertion
	for i := list.maxLevel - 1; i >= 0; {
		for nextEntry := currentEntry.levels[i]; nextEntry != nil; nextEntry = currentEntry.levels[i] {
			if comp := list.keyType.ModifyCompare(key, nextEntry.key); comp <= 0 {
				if comp == 0 {
					specifiedEntry = nextEntry
				}
				break
			}
			currentEntry = &nextEntry.EntryBase
		}
		previousLevels[i] = currentEntry
		// skip the level if they point the same entry as the higher level
		topLevel := currentEntry.levels[i]
		for i--; i >= 0 && currentEntry.levels[i] == topLevel; i-- {
			previousLevels[i] = currentEntry
		}
	}
	// remove the entry at every level
	if specifiedEntry != nil {
		for i := 0; i < len(specifiedEntry.levels); i++ {
			previousLevels[i].levels[i] = specifiedEntry.levels[i]
			if specifiedEntry.levels[i] != nil {
				specifiedEntry.levels[i].prev = previousLevels[i].Entry()
			}
		}
		if specifiedEntry == list.back {
			list.back = specifiedEntry.prev
		}
		return specifiedEntry
	} else {
		return nil
	}

}

func (list *SkipList) LevelStatistics() {
	common.Always("size:", list.length)
	for i := range list.levelCounter {
		fmt.Printf("%10d", i)
	}
	common.Always()
	for _, value := range list.levelCounter {
		fmt.Printf("%10d", value)
	}
	common.Always()
}

func (list *SkipList) Iterator() *ListIterator {
	return NewListIterator(list)
}

func (list *SkipList) Length() int {
	return list.length
}

func (list *SkipList) First() *Entry {
	return list.levels[0]
}

func (list *SkipList) Back() *Entry {
	return list.back
}

func (list *SkipList) SyncCustomOperation(task func()) {
	list.lock.RLock()
	task()
	defer list.lock.RUnlock()
}

func (list *SkipList) Range(start, end interface{}, count, offset int) []*Entry {
	preAlloc := 4096
	if count < 4096 {
		preAlloc = count
	}
	currentOffset, currentCount := 0, 0
	result := make([]*Entry, 0, preAlloc)
	leftmostEntry := list.First()
	var prevContent []byte = nil
	for currentEntry := leftmostEntry; currentEntry != nil; currentEntry = currentEntry.NextEntry() {
		if cmp := list.keyType.ModifyCompare(currentEntry.key, start); cmp >= 0 {
			if list.keyType.ModifyCompare(currentEntry.key, end) < 0 {
				if currentOffset >= offset {
					//
					currentKey := currentEntry.Key().(*common.MVCCKey)
					if prevContent != nil && bytes.Equal(currentKey.Content, prevContent) {
						continue
					} else {
						prevContent = currentKey.Content
					}
					if currentKey.KT != common.OpDelete {
						result = append(result, currentEntry)
						currentCount += 1
						if currentCount >= count {
							break
						}
					}
				} else {
					currentOffset += 1
				}
			} else {
				break
			}
		}
	}
	return result
}