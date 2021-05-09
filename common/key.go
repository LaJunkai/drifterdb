package common

import (
	"encoding/binary"
)

/*

MVCCKey  bytes content + sequence number (7bytes) + OpType(1bytes)
	...		| 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    |
	...		| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
 content... |                       seq                      |OpType|


*/
type MVCCKey struct {
	Content  []byte
	Seq      uint64
	KT       uint8
	TrxId    uint32
	IsoLevel uint8
}

func MakeMVCCKey(content []byte, seq uint64, kt uint8, trxId uint32) *MVCCKey {
	return &MVCCKey{Content: content, Seq: seq, KT: kt, TrxId: trxId, IsoLevel: ReadCommitted}
}

func MakeIsoMVCCKey(content []byte, seq uint64, kt uint8, trxId uint32, isoLevel uint8) *MVCCKey {
	return &MVCCKey{Content: content, Seq: seq, KT: kt, TrxId: trxId, IsoLevel: isoLevel}
}

func ParseMVCCKey(src []byte) *MVCCKey {
	return &MVCCKey{
		Content: src[8:],
		Seq:     (binary.LittleEndian.Uint64(src[:8]) & uint64(0xFFFFFFFFFFFFFF00)) >> 8,
		KT:      uint8(binary.LittleEndian.Uint64(src[:8]) & uint64(0x00000000000000FF)),
		TrxId:   0,
		IsoLevel: RepeatableRead,
	}
}

func ExtractMVCCKeyContent(src []byte) []byte {
	return src[8:]
}
