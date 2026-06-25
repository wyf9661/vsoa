package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.SetPositionServers("127.0.0.1:3000")
	cli.OnConnect(func(c *vsoa.Client, conn bool, info any) {
		fmt.Println("connect:", conn, "info:", info)
	})
	if err := cli.Connect("vsoa://pyserver", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()
	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"hello": "world"}}, 3*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("header:", h, "payload:", p)
}
