package common

func MinInt(x ...int) (result int) {
	result = x[0]
	for i := 0; i < len(x); i++ {
		if x[i] < result {
			result = x[i]
		}
	}
	return
}


func MaxInt(x ...int) (result int) {
	result = x[0]
	for i := 0; i < len(x); i++ {
		if x[i] > result {
			result = x[i]
		}
	}
	return
}