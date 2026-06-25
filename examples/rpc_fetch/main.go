package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	h, p, err := vsoa.Fetch(
		"vsoa://127.0.0.1:3005/echo",
		"123456",
		0,
		&vsoa.Payload{Param: map[string]any{"hello": "fetch"}},
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("header:", h)
	fmt.Println("payload:", p)
}
