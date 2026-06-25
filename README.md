# VSOA for Go

VSOA for Go is a Go SDK and protocol implementation for building TCP/UDP based service-oriented communication with RPC, publish-subscribe, datagram, stream, TLS, position discovery, and reconnect support.

## Quick Start

1. Start the position server:

```bash
go run ./examples/position_server
```

2. Start the main server:

```bash
go run ./examples/server
```

3. Start the client:

```bash
go run ./examples/client
```

This gives you a working baseline with:
- position discovery
- RPC
- publish/subscribe
- datagram

## More examples

- `go run ./examples/rpc_fetch`
- `go run ./examples/rpc_async`
- `go run ./examples/pubsub`
- `go run ./examples/datagram`
- `go run ./examples/quick_datagram`
- `go run ./examples/stream`
- `go run ./examples/robot`
- `go run ./examples/tls_server_client`
- `go run ./examples/mtls`

## Testing

```bash
go test ./...
```

## Wiki

Detailed documentation is maintained in the GitHub Wiki:

https://github.com/wyf9661/vsoa/wiki

Suggested reading order:
- Home
- Quick Start
- Examples
- Server API
- Client API
- TLS and mTLS
- Position Discovery
- Robot and Reconnect
- Datagram and Quick Channel
- Stream Tunnels

## License

Copyright (c) 2025 wyf9661.

Licensed under Apache-2.0. See `LICENSE`.
