package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.OnData(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("quick datagram recv:", url, p.Param, "quick:", quick)
	})
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	if err := cli.Datagram("/quick/example", &vsoa.Payload{Param: map[string]any{"mode": "quick"}}, true); err != nil {
		panic(err)
	}

	fmt.Println("quick datagram sent, waiting for reply...")
	time.Sleep(5 * time.Second)
}
