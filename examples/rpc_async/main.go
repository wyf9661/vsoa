package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	done := make(chan struct{})
	if err := cli.CallAsync("/echo", 0, &vsoa.Payload{Param: map[string]any{"hello": "async"}}, 3*time.Second,
		func(h *vsoa.Header, p *vsoa.Payload, err error) {
			fmt.Println("header:", h)
			fmt.Println("payload:", p)
			fmt.Println("err:", err)
			close(done)
		}); err != nil {
		panic(err)
	}

	<-done
}
