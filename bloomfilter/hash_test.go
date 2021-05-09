package bloomfilter

import (
	"fmt"
	"testing"
)

func TestHashFunctionsPool(t *testing.T) {

	var pool = NewHashFunctionsPool()
	for i, name := range pool.Names() {
		fmt.Println(i, name)
		fmt.Println(pool.GetHashFunctionByName(name)([]byte("user-2")))
	}

}