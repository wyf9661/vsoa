package main

import (
	"fmt"

	"github.com/wyf9661/vsoa"
)

func main() {
	s := vsoa.NewServer("Go VSOA Server", "123456", false)
	s.OnClient(func(cli *vsoa.RemoteClient, conn bool) {
		fmt.Println("client", cli.ID(), "connect:", conn)
	})
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	fmt.Println("server listening on :3005")
	if err := s.Run("0.0.0.0:3005", nil); err != nil {
		panic(err)
	}
}
