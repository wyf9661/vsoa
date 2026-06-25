package main

import (
	"context"
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.OnConnect(func(c *vsoa.Client, conn bool, info any) {
		fmt.Println("connect:", conn, "info:", info)
	})
	cli.OnMessage(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("publish:", url, p.Param, "quick:", quick)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli.SetRobotPingTurbo(500 * time.Millisecond)
	cli.Robot(ctx, "vsoa://127.0.0.1:3005", "123456", 3*time.Second, 5*time.Second, 1*time.Second, nil)

	for {
		time.Sleep(3 * time.Second)
		fmt.Println("connected:", cli.Connected(), "pendings:", cli.Pendings(), "turbo:", cli.RobotPingTurbo())
	}
}
