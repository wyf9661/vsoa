package main

import (
	"fmt"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	cli.OnData(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		fmt.Println("datagram recv:", url, p.Param, "quick:", quick)
	})
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	if err := cli.Datagram("/custom/data", &vsoa.Payload{Param: map[string]any{"from": "datagram-example"}}, false); err != nil {
		panic(err)
	}

	fmt.Println("datagram sent, waiting for reply...")
	time.Sleep(5 * time.Second)
}
