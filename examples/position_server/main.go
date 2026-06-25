package main

import (
	"fmt"
	"net"

	"github.com/wyf9661/vsoa"
)

func main() {
	ps, err := vsoa.ListenPosition("0.0.0.0:3000", func(q vsoa.PositionQuery) *vsoa.PositionServerInfo {
		if q.Name == "pyserver" {
			return &vsoa.PositionServerInfo{Addr: "127.0.0.1", Port: 3005, Domain: int(net.ParseIP("127.0.0.1").To4()[0])}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	defer ps.Close()
	fmt.Println("position server listening on", ps.Addr())
	select {}
}
