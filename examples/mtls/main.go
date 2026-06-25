package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
	"github.com/wyf9661/vsoa/transport"
)

func main() {
	fmt.Println("This example expects files: ca.crt server.crt server.key client.crt client.key")
	s := vsoa.NewServer("mtls-server", "123456", false)
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		fmt.Println("server peer cert:", cli.GetPeerCert() != nil)
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	go func() {
		if err := s.Run("127.0.0.1:3444", &transport.TLSOptions{
			Cert: "server.crt", Key: "server.key", CACert: "ca.crt", RequireClientCert: true,
		}); err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3444", "123456", 5*time.Second, &transport.TLSOptions{
		Hostname: "127.0.0.1",
		CACert:   "ca.crt",
		Cert:     "client.crt",
		Key:      "client.key",
	}); err != nil {
		panic(err)
	}
	defer cli.Close()
	fmt.Println("client peer cert:", cli.GetPeerCert() != nil)
	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"mtls": true}}, 3*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("header:", h)
	fmt.Println("payload:", p)
}
