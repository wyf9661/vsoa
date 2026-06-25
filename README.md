# VSOA for Go

VSOA (Vehicle SOA) is a reliable, real-time Service Oriented Architecture framework originally by ACOINFO. This is a Go reimplementation providing full protocol compatibility with the Python SDK (`vsoa-python`).

## Features

- **TCP + UDP dual-channel** protocol with quick channel for high-frequency data
- **RPC** (Remote Procedure Call) with GET/SET methods
- **Publish/Subscribe** with URL prefix matching
- **Datagram** (reliable TCP + unreliable UDP quick)
- **Stream tunnels** for high-throughput data transfer
- **TLS** one-way and mutual authentication
- **WorkQueue** for serial async command execution
- **Client robot** auto-reconnect with keepalive
- **Position service** discovery (UDP name lookup)

## Quick Start

### Server

```go
package main

import (
	"fmt"
	"github.com/wyf9661/vsoa"
)

func main() {
	s := vsoa.NewServer("Go VSOA Server", "123456", false)
	s.OnClient(func(cli *vsoa.RemoteClient, conn bool) {
		fmt.Println("Client", cli.ID(), "connected:", conn)
	})
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	s.Command("/foo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		fmt.Println("foo", payload.Param)
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	fmt.Println("Server listening on :3005")
	if err := s.Run("0.0.0.0:3005", nil); err != nil {
		panic(err)
	}
}
```

### Client

```go
package main

import (
	"fmt"
	"time"
	"github.com/wyf9661/vsoa"
)

func main() {
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil); err != nil {
		panic(err)
	}
	defer cli.Close()
	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"hello": "world"}}, 3*time.Second)
	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println("header:", h, "payload:", p)
	}
}
```

## Project Structure

```
vsoa/
  protocol/     # Packet encoding/decoding, constants, URL matching
  transport/    # TLS config, full-write helper
  workqueue/    # Serial async job queue
  timer/        # One-shot / interval timer
  events/       # EventEmitter (Node.js style)
  position/     # UDP position service (name → addr:port)
  vsoa.go       # Server, Client, RemoteClient, Stream, Fetch
```

## Testing

```bash
go test ./...
```

## Protocol Compatibility

This library is wire-compatible with `vsoa-python`. Go clients can connect to Python servers and vice versa. The packet format uses big-endian encoding with 20-byte headers, 4-byte alignment, and supports both TCP (reliable) and UDP (quick) channels.

## License

Copyright (c) 2025 wyf9661. All rights reserved.

Licensed under the Apache-2.0 license. See [LICENSE](LICENSE) for details.
