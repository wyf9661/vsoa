# VSOA for Go

VSOA is the abbreviation of Vehicle SOA. This repository is a Go implementation of the VSOA protocol and SDK, designed to be wire-compatible with the Python implementation at https://github.com/acoinfo/vsoa-python.

Current status:
- TCP + UDP quick dual-channel protocol implemented
- RPC / publish-subscribe / datagram / stream implemented
- TLS one-way and mutual-auth tested
- Go ↔ Python interoperability tested for RPC, datagram, stream
- position discovery implemented with top-level compatibility helpers

## Features

- Unified URL-based service model
- URL prefix matching subscribe/publish model
- Real-time RPC (GET/SET)
- Reliable TCP publish/datagram
- Unreliable quick UDP publish/datagram
- Stream tunnels for large or high-throughput data
- TLS server/client support, including mutual TLS
- WorkQueue, Timer, EventEmitter helper packages
- Position service discovery via UDP
- Robot reconnect mode with keepalive/turbo interval support

## Protocol notes

- Main channel: TCP
- Quick channel: UDP
- Packet header length: 20 bytes
- TCP max packet length: 262144 bytes
- Quick max packet length: 65507 bytes
- Packet alignment: 4 bytes
- Method values:
  - GET = 0
  - SET = 1

Quick channel is intended for high-frequency traffic where strict delivery guarantees are not required.

## Package layout

- `vsoa.go` — main public API (`Server`, `Client`, `RemoteClient`, `Stream`, `Fetch`)
- `protocol/` — packet encode/decode, unpacker, constants, subscription matching
- `transport/` — TLS helpers and full-write helper
- `position/` — UDP name lookup service
- `workqueue/` — serial async job queue
- `timer/` — timer helper
- `events/` — event emitter helper
- `examples/` — runnable examples

## Public API overview

### Server

```go
s := vsoa.NewServer("Go VSOA Server", "123456", false)
```

Main methods:
- `NewServer(info any, passwd string, raw bool) *Server`
- `Run(addr string, tlsOpt *transport.TLSOptions) error`
- `Close() error`
- `Address() net.Addr`
- `Clients() []*RemoteClient`
- `OnClient(func(*RemoteClient, bool))`
- `OnData(func(*RemoteClient, string, Payload, bool))`
- `Command(url string, q *workqueue.Queue, h Handler)`
- `Publish(url string, payload *Payload, quick bool) bool`
- `IsSubscribed(url string) bool`
- `CreateStream(onlink func(*Stream, bool), ondata func(*Stream, []byte), timeout time.Duration) (*Stream, error)`
- `SendTimeout(timeout time.Duration)`

### RemoteClient

Main methods:
- `ID() uint32`
- `Address() net.Addr`
- `SetAuthed(bool)`
- `Close()`
- `IsClosed() bool`
- `Reply(seqno uint32, payload *Payload, status uint8, tunid uint16) bool`
- `Datagram(url string, payload *Payload, quick bool) bool`
- `IsSubscribed(url string) bool`
- `OnSubscribe(func(*RemoteClient, []string))`
- `OnUnsubscribe(func(*RemoteClient, []string))`

### Client

```go
cli := vsoa.NewClient(false)
err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 5*time.Second, nil)
```

Main methods:
- `NewClient(raw bool) *Client`
- `Connect(rawURL, passwd string, timeout time.Duration, tlsOpt *transport.TLSOptions) error`
- `Close()`
- `Connected() bool`
- `OnConnect(func(*Client, bool, any))`
- `OnMessage(func(*Client, string, Payload, bool))`
- `OnData(func(*Client, string, Payload, bool))`
- `Call(url string, method int, payload *Payload, timeout time.Duration) (*Header, *Payload, error)`
- `CallAsync(url string, method int, payload *Payload, timeout time.Duration, callback func(*Header, *Payload, error)) error`
- `Ping(timeout time.Duration) error`
- `Subscribe(urls any, timeout time.Duration) error`
- `Unsubscribe(urls any, timeout time.Duration) error`
- `Datagram(url string, payload *Payload, quick bool) error`
- `CreateStream(tunid uint16, onlink func(net.Conn, bool), ondata func([]byte), timeout time.Duration) (net.Conn, error)`
- `Robot(ctx context.Context, rawURL, passwd string, keepalive, connTimeout, reconnDelay time.Duration, tlsOpt *transport.TLSOptions)`
- `SetRobotPingTurbo(time.Duration)`
- `RobotPingTurbo() time.Duration`
- `Pendings() int`
- `GetPeerCert() any`
- `SetPositionServers(...string)`

### One-shot fetch

```go
h, p, err := vsoa.Fetch("vsoa://127.0.0.1:3005/echo", "123456", 0, &vsoa.Payload{Param: map[string]any{"a": 1}}, 3*time.Second, false, nil)
```

### Position compatibility helpers

To align more closely with the Python implementation, the top-level package also exposes:
- `ListenPosition(addr string, handler func(PositionQuery) *PositionServerInfo) (*PositionServer, error)`
- `SetPositionServer(addr string, port int)`
- `LookupPosition(name string) (*net.UDPAddr, error)`

## Examples

### 1. Position server

File: `examples/position_server/main.go`

Run:
```bash
go run ./examples/position_server
```

### 2. VSOA server

File: `examples/server/main.go`

Run:
```bash
go run ./examples/server
```

### 3. VSOA client using position discovery

File: `examples/client/main.go`

Run:
```bash
go run ./examples/client
```

Startup order:
1. start `examples/position_server`
2. start `examples/server`
3. start `examples/client`

## TLS options

`transport.TLSOptions` supports:
- `Hostname`
- `CACert`
- `Cert`
- `Key`
- `Password`
- `InsecureSkipVerify`
- `RequireClientCert`
- `HandshakeErrorLog`
- `LoadDefaultCerts`

Typical patterns:
- server-only TLS: set `Cert` + `Key`
- mutual TLS: set server `CACert` + `RequireClientCert`, and client `CACert` + `Cert` + `Key`

## Interoperability status

Automated tests currently cover:
- Go server ↔ Python client RPC
- Python server ↔ Go client RPC
- Go server ↔ Python client datagram
- Python server ↔ Go client datagram
- Python server ↔ Go client stream
- TLS one-way auth
- TLS mutual auth

## Testing

Run all tests:

```bash
go test ./...
```

Focused interop tests:

```bash
go test ./... -run 'Interop|TLS|Stream|Robot' -count=1
```

## License

Copyright (c) 2025 wyf9661.

Licensed under Apache-2.0. See `LICENSE`.
