package drifterdb
/*
UndoLog is used to recover transaction set after the crash.
record struct (each record takes up 12 bytes: 4 bytes trx-id and 8 bytes operation seq)
| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   | 13   | 14   | 15   |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
|      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |


*/
type UndoLog struct {

}
