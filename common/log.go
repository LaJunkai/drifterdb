package common

import "fmt"

const Dev = true

func Always(content ...interface{}) {
	fmt.Println(content...)
}

func Debug(content ...interface{}) {
	if Dev {
		fmt.Println(content...)
	}
}

func Regular(content ...interface{}) {
	if !Dev {
		fmt.Println(content...)
	}
}

func Error(content string) {
	fmt.Println("[ERROR] @", content)
	panic(content)
}

func Warning(content string) {
	fmt.Println("[WARNING] @", content)
}