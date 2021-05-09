package skiplist

import (
	"github.com/LaJunkai/drifterdb/common"
	"fmt"
	"testing"
	"time"
)


func TestSkipList_Set(t *testing.T) {
	for i := 0; i < 10; i++ {
		list := NewSkipList(common.TypeString)
		list.Set("a", "a_value")
		if list.Get("a") == nil {
			fmt.Println(list)
		}

	}
}
func TestSkipList_Get(t *testing.T) {
	list := NewSkipList(common.TypeString)
	for i := 0; i < 1000000; i++ {
		key, value := common.RandString(10), common.RandString(20)
		//_, _ = RandString(10), RandString(20)
		list.Set(key, value)
		returnValue := list.Get(key)
		if returnValue != value {
			 t.Errorf("%v - Return value %v does not equals to original %v.\n", i, returnValue, value)
		}
	}
}

func TestSkipList_Delete(t *testing.T) {
	size := 1000
	list := NewSkipList(common.TypeString)
	refMap := make(map[string]string)
	for i := 0; i < size; i++ {
		key, value := common.RandString(10), common.RandString(20)
		list.Set(key, value)
		refMap[key] = value
		if i % 2 == 0 {
			list.Delete(key)
			delete(refMap, key)
		}
	}
	for k, v := range refMap {
		if lv := list.Get(k); v != lv {
			t.Errorf("wrong value: map: %v <--> SkipList: % v", v, lv)
		}
	}
}

func TestSkipList_Iterator(t *testing.T) {
	list := NewSkipList(common.TypeString)
	list.Set("d", "d")
	list.Set("c", "c")
	list.Set("b", "b")
	list.Set("a", "a")

	for iter := list.Iterator(); iter.HasNext(); {
		fmt.Println(iter.Get().Key())
	}

}
func TestRandomLevel(t *testing.T) {
	counter := make([]int, 33)
	list := NewSkipList(common.TypeString)
	for i := 0; i < 10; i++ {
		counter[list.randomLevel()] += 1
	}
	for i := range counter {
		fmt.Println(i, counter[i])
	}
}


func TestSkipList_LevelStatistics(t *testing.T) {
	list := NewSkipList(common.TypeString)
	for i := 0; i < 10000; i++ {
		key, value := common.RandString(10), common.RandString(20)
		//_, _ = RandString(10), RandString(20)
		list.Set(key, value)
	}
	list.LevelStatistics()
}

func TestSkipList_TraditionalRandomLevel(b *testing.T) {
	counter := make([]int, 33)
	counter2 := make([]int, 33)
	list := NewSkipList(common.TypeString)
	for i := 0; i < 100000000; i++ {
		counter[list.TraditionalRandomLevel() - 1] += 1
	}
	for i := 0; i < 100000000; i++ {
		counter2[list.randomLevel() - 1] += 1
	}
	fmt.Println("level     traditional      new      ")
	for i := range counter {
		fmt.Printf("%10d %10d %10d\n", i, counter[i], counter2[i])
	}
}

func BenchmarkSkipList_TraditionalRandomLevel(b *testing.B) {
	list := NewSkipList(common.TypeString)
	start := time.Now()
	b.StartTimer()
	for i := 0; i < 1000000; i++ {
		list.TraditionalRandomLevel()
	}
	b.StopTimer()
	fmt.Println(time.Since(start).Seconds())
}
func BenchmarkSkipList_RandomLevel(b *testing.B) {
	list := NewSkipList(common.TypeString)
	start := time.Now()
	b.StartTimer()
	for i := 0; i < 1000000; i++ {
		list.randomLevel()
	}
	b.StopTimer()
	fmt.Println(time.Since(start).Seconds())
}

func BenchmarkSkipList_Set(b *testing.B) {
	size := 2 << 20
	keys := make([]string, size)
	values := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = common.RandString(10)
		values[i] = common.RandString(10)
	}
	list := NewSkipList(common.TypeString)
	b.ResetTimer()
	b.StartTimer()
	start := time.Now()
	for i := 0; i < size; i++ {
		list.Set(keys[i], values[i])
	}
	fmt.Println(time.Since(start).Seconds())
	b.StopTimer()
	list.LevelStatistics()
}

func BenchmarkSynchronousSkipList_Set(b *testing.B) {
	size := 2 << 20
	keys := make([]string, size)
	values := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = common.RandString(10)
		values[i] = common.RandString(10)
	}
	list := ConcurrentSkipList(common.TypeString)
	b.ResetTimer()
	b.StartTimer()
	start := time.Now()
	for i := 0; i < size; i++ {
		list.Set(keys[i], values[i])
	}
	fmt.Println(time.Since(start).Nanoseconds() / int64(size))
	b.StopTimer()
	list.LevelStatistics()
}



func TestSkipList_Back(t *testing.T) {
	list := ConcurrentSkipList(common.TypeString)
	list.Set("10", "10")
	fmt.Println(list.Back().Value)
	list.Set("4", "4")
	fmt.Println(list.Back().Value)
	list.Set("7", "7")
	fmt.Println(list.Back().Value)
	list.Set("6", "6")
	fmt.Println(list.Back().Value)
	list.Set("8", "8")
	fmt.Println(list.Back().Value)
	list.Set("1", "1")
	fmt.Println(list.Back().Value)
	list.Set("3", "3")
	fmt.Println(list.First().Value, list.Back().Value)
	fmt.Println(list.First().Value, list.Back().Value)
	list.Set("2", "2")
	fmt.Println(list.First().Value, list.Back().Value)
	list.Set("0", "0")
	fmt.Println(list.First().Value, list.Back().Value)

	for iter := list.Iterator(); iter.HasNext(); {
		fmt.Print(iter.Get().Key(), " ")
	}
}

func TestSkipList_Range(t *testing.T) {
	list := ConcurrentSkipList(common.TypeString)
	list.Set("10", "10")
	list.Set("4", "4")
	list.Set("7", "7")
	list.Set("6", "6")
	list.Set("8", "8")
	list.Set("1", "1")
	list.Set("3", "3")
	list.Set("2", "2")
	list.Set("0", "0")

	for _, i := range list.Range("3", "8", 10, 1) {
		fmt.Print(i.Key().(string), ",")
	}
}
