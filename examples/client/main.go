package main

import (
	"fmt"
	"net"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.SetPositionServers("127.0.0.1:3000")

	cli.OnConnect(func(c *vsoa.Client, conn bool, info any) {
		fmt.Println("connect:", conn, "info:", info)
	})
	cli.OnMessage(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("publish:", url, p.Param, "quick:", quick)
	})
	cli.OnData(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("datagram:", url, p.Param, "quick:", quick)
	})

	if err := cli.Connect("vsoa://pyserver", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"hello": "world"}}, 3*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("echo header:", h, "payload:", p)

	if err := cli.Subscribe("/topic1", 3*time.Second); err != nil {
		panic(err)
	}

	if err := cli.Datagram("/custom/data", &vsoa.Payload{Param: map[string]any{"from": "go-client"}}, false); err != nil {
		panic(err)
	}

	ctxDone := make(chan struct{})
	go func() {
		h, _, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"stream": true}}, 3*time.Second)
		if err == nil && h != nil && h.TunID > 0 {
			conn, cerr := cli.CreateStream(h.TunID, func(conn net.Conn, up bool) {
				fmt.Println("stream connect:", up)
			}, func(data []byte) {
				fmt.Println("stream data:", string(data))
			}, 3*time.Second)
			if cerr == nil {
				defer conn.Close()
			}
		}
		close(ctxDone)
	}()

	select {
	case <-ctxDone:
	case <-time.After(8 * time.Second):
	}
}
