package main

import (
	"fmt"
	"net"

	"github.com/wyf9661/vsoa/position"
)

func main() {
	s, err := position.Listen("0.0.0.0:3000", func(q position.Query) *position.ServerInfo {
		if q.Name == "pyserver" {
			return &position.ServerInfo{Addr: "127.0.0.1", Port: 3005, Domain: int(net.ParseIP("127.0.0.1").To4()[0])}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	defer s.Close()
	fmt.Println("position server listening on", s.Addr())
	select {}
}
