package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.OnMessage(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("publish:", url, p.Param, "quick:", quick)
	})
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	if err := cli.Subscribe([]string{"/topic1", "/topic2", "/a/"}, 3*time.Second); err != nil {
		panic(err)
	}

	fmt.Println("subscribed, waiting for publishes...")
	time.Sleep(30 * time.Second)
}
