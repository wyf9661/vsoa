package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
	"github.com/wyf9661/vsoa/transport"
)

func main() {
	fmt.Println("This example expects files: server.crt server.key")
	s := vsoa.NewServer("tls-server", "123456", false)
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	go func() {
		if err := s.Run("127.0.0.1:3443", &transport.TLSOptions{Cert: "server.crt", Key: "server.key"}); err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3443", "123456", 5*time.Second, &transport.TLSOptions{
		Hostname: "127.0.0.1",
		InsecureSkipVerify: true,
	}); err != nil {
		panic(err)
	}
	defer cli.Close()
	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"tls": true}}, 3*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("header:", h)
	fmt.Println("payload:", p)
}
