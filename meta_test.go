package drifterdb

import (
	"testing"
)

func TestLoadMeta(t *testing.T) {
	for i := 0; i < 100; i++ {
		m := LoadMeta("temp", "")
		m.Flush()
	}
}

func TestMeta_Clear(t *testing.T) {
	a := make([]uint64, 10)
	a[0] = 1
	a[1] = 2


}