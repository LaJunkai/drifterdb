# DrifterDB

## component
### skip list
#### improvements
#### `RandomLevel` method of the skip list
`RandomLevel` is an important method of the skip list used to determine which level should the new entry be placed during the insertion operation.  

Traditiontial `RandomLevel` may call the `rand` function as many times as the levels the skip list maintains.

`rand` function has higher cost than common
calculation statement. By calling `rand` function less times 
I make the `RandomLevel` method `55%` faster than 
traditional `RandomLevel` implementations and make 
the `Set`/`Insertion` operation `1.07%` faster than before.
#### `Set` operation
Skip levels if they point to the same entry as the higher level  
replace reflect type conversion to a certain manually specified type conversion in `Compare` method.
#### `Get` operation
introduce `RWMutex` to support concurrent `Get` operation.
### bloom filter

## leveldb
### features
* Inspired by the paper WiscKey, separate the key from the value to reduce write-amplification.
* User DI to implement decoupling and make the test easier.
* Introduce Marker byte for the vLog to accelerate the GC procedure(cause a little reliable random writes).
* Introduce multiple thread search.
### classes of leveldb
#### `Memtable`(interface)
##### `SkiplistMemtable`(class)
##### `HashMapMemtable`(class)
#### `WALLogger`(interface)

#### `TableIndex`(interface)

#### `SSTable`(interface)

##### `DefaultSSTable`(class)

#### `Compactor`(interface)

##### `DefaultCompactor`(class)



Use singleton to make sure there is only one writer is appending log to the WAL.
Collect operation log until the counter reaches the threshold or timer expires.
Timer can be implemented by using goroutine and channel.

##### `LocalWALLogger`
### db(LSM TREE / WiscKey)