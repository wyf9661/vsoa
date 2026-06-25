package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	s := vsoa.NewServer("Go VSOA Server", "123456", false)
	data := map[string]any{"foo": 0}

	s.OnClient(func(cli *vsoa.RemoteClient, conn bool) {
		fmt.Println("client", cli.ID(), "connect:", conn, "addr:", cli.Address())
	})

	s.OnData(func(cli *vsoa.RemoteClient, url string, payload vsoa.Payload, quick bool) {
		fmt.Println("server datagram from client:", url, payload.Param, "quick:", quick)
		cli.Datagram("/server/echo", &vsoa.Payload{Param: map[string]any{"echo": payload.Param}}, false)
	})

	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})

	s.Command("/foo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		if m, ok := payload.Param.(map[string]any); ok {
			data = m
		}
		cli.Reply(req.SeqNo, &vsoa.Payload{Param: data}, 0, 0)
	})

	go func() {
		for {
			time.Sleep(2 * time.Second)
			s.Publish("/topic1", &vsoa.Payload{Param: map[string]any{"ts": time.Now().Unix()}}, false)
		}
	}()

	fmt.Println("server listening on :3005")
	if err := s.Run("0.0.0.0:3005", nil); err != nil {
		panic(err)
	}
}
