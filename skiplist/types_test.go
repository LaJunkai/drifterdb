package skiplist

import (
	"github.com/LaJunkai/drifterdb/common"
	"fmt"
	"testing"
	"time"
)

func BenchmarkComparableString_Compare(b *testing.B) {
	size := 1000
	keys := make([]string, size)
	values := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = common.RandString(10)
		values[i] = common.RandString(10)
	}
	start := time.Now()
	for i := 0; i < size; i++ {
		common.TypeString.QueryCompare(keys[i], values[i])
		// _ = keys[i] > values[i]
	}
	fmt.Println(time.Since(start).Nanoseconds() / int64(size))

}

