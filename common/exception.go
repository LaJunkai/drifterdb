package common

func Throw(err error) {
	if err != nil {
		panic(err)
	}
}
