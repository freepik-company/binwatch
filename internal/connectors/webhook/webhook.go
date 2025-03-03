package webhook

import "fmt"

func Send(data []byte) {
	fmt.Println(string(data))
}
