package bloomfilter

type HashFunctionsPool struct {
	pool        []func(str []byte) (hash uint64)
	functionMap map[string]func(str []byte) (hash uint64)
	size        int
}

func NewHashFunctionsPool() *HashFunctionsPool {
	pool := HashFunctionsPool{
		pool:        make([]func(str []byte) (hash uint64), 0),
		size:        0,
		functionMap: make(map[string]func(str []byte) (hash uint64)),
	}
	pool.AddHashFunction("RSHash", RSHash)
	pool.AddHashFunction("JSHash", JSHash)
	pool.AddHashFunction("PJWHash", PJWHash)
	pool.AddHashFunction("ELFHash", ELFHash)
	pool.AddHashFunction("BKDRHash", BKDRHash)
	pool.AddHashFunction("SDBMHash", SDBMHash)
	pool.AddHashFunction("DJBHash", DJBHash)
	pool.AddHashFunction("DEKHash", DEKHash)
	pool.AddHashFunction("BPHash", BPHash)
	pool.AddHashFunction("FNVHash", FNVHash)
	pool.AddHashFunction("APHash", APHash)
	return &pool
}

func (pool *HashFunctionsPool) AddHashFunction(name string, f func(str []byte) (hash uint64)) {
	if v := pool.functionMap[name]; v == nil {
		pool.pool = append(pool.pool, f)
		pool.functionMap[name] = f
		pool.size += 1
	}
}

func (pool *HashFunctionsPool) GetHashFunctionByName(name string) func(str []byte) (hash uint64) {
	return pool.functionMap[name]
}

func (pool *HashFunctionsPool) GetHashFunctionByIndex(index int) func(str []byte) (hash uint64) {
	if index < pool.size {
		return pool.pool[index]
	} else {
		return nil
	}
}

func (pool *HashFunctionsPool) Size() int {
	return pool.size
}

func (pool *HashFunctionsPool) Names() []string {
	names := make([]string, pool.size)
	i := 0
	for k := range pool.functionMap {
		names[i] = k
		i += 1
	}
	return names
}

func RSHash(str []byte) (hash uint64) {
	var b uint64 = 378551
	var a uint64 = 63689
	hash = 0
	for i := 0; i < len(str); i++ {
		hash = hash*a + uint64(str[i])
		a = a * b
	}
	return
}

func JSHash(str []byte) (hash uint64) {
	hash = 1315423911
	for i := 0; i < len(str); i++ {
		hash ^= (hash << 5) + uint64(str[i]) + (hash >> 2)
	}
	return
}

func PJWHash(str []byte) (hash uint64) {
	var (
		BitsInUnsignedInt uint64 = 64
		ThreeQuarters     uint64 = 48
		OneEighth         uint64 = 8
		HighBits          uint64 = 0xFFFFFFFF << (BitsInUnsignedInt - OneEighth)
	)
	hash = 0
	var temp uint64
	for i := 0; i < len(str); i++ {
		hash = (hash << OneEighth) + uint64(str[i])
		if temp = hash & HighBits; temp != 0 {
			hash = (hash ^ (temp >> ThreeQuarters)) & (0xFFFFFFFF ^ HighBits)
		}
	}
	return
}

func ELFHash(str []byte) (hash uint64) {
	hash = 0
	var x uint64 = 0
	for i := 0; i < len(str); i++ {
		hash = (hash << 4) + uint64(str[i])
		if x = hash & 0xF0000000; x != 0 {
			hash ^= x >> 24
		}
		hash &= 0xFFFFFFFF ^ x
	}
	return
}

func BKDRHash(str []byte) (hash uint64) {
	hash = 0
	var seed uint64 = 131
	for i := 0; i < len(str); i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return
}

func SDBMHash(str []byte) (hash uint64) {
	hash = 0
	for i := 0; i < len(str); i++ {
		hash = uint64(str[i]) + (hash << 6) + (hash << 16) - hash
	}
	return

}

func DJBHash(str []byte) (hash uint64) {
	hash = 5381
	for i := 0; i < len(str); i++ {
		hash = ((hash << 5) + hash) + uint64(str[i])
	}
	return
}

func DEKHash(str []byte) (hash uint64) {
	hash = uint64(len(str))
	for i := 0; i < len(str); i++ {
		hash = ((hash << 5) ^ (hash >> 27)) ^ uint64(str[i])
	}
	return
}

func BPHash(str []byte) (hash uint64) {
	hash = 0
	for i := 0; i < len(str); i++ {
		hash = hash<<7 ^ uint64(str[i])
	}
	return
}

func FNVHash(str []byte) (hash uint64) {
	hash = 0
	const fnvPrime uint64 = 0x811c9Dc5
	for i := 0; i < len(str); i++ {
		hash *= fnvPrime
		hash ^= uint64(str[i])
	}
	return
}

func APHash(str []byte) (hash uint64) {
	hash = 0xAAAAAAAA
	for i := 0; i < len(str); i++ {
		hash ^= TE(
			(i&1) == 0,
			(hash<<7)^uint64(str[i])*(hash>>3),
			0xFFFFFFFF^((hash<<11)+uint64(str[i])^(hash>>5)))
	}
	return
}

func TE(condition bool, trueValue uint64, falseValue uint64) uint64 {
	if condition {
		return trueValue
	} else {
		return falseValue
	}
}
