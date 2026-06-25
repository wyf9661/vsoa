package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()

	h, _, err := cli.Call("/get_data", 0, nil, 3*time.Second)
	if err != nil {
		panic(err)
	}
	if h == nil || h.TunID == 0 {
		panic("no stream tunid returned")
	}

	file, err := os.Create("stream_output.bin")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	done := make(chan struct{})
	conn, err := cli.CreateStream(h.TunID, func(conn net.Conn, up bool) {
		fmt.Println("stream connect:", up)
		if !up {
			close(done)
		}
	}, func(data []byte) {
		fmt.Println("stream recv:", len(data), "bytes")
		_, _ = file.Write(data)
	}, 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		fmt.Println("stream timeout")
	}
}
