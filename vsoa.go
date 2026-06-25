package vsoa

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	vsoapos "github.com/wyf9661/vsoa/position"
	"github.com/wyf9661/vsoa/protocol"
	"github.com/wyf9661/vsoa/transport"
	"github.com/wyf9661/vsoa/workqueue"
)

type Payload = protocol.Payload
type Header = protocol.Header
type Request = protocol.Request

type Handler func(cli *RemoteClient, req Request, payload Payload)

type Client struct {
	mu        sync.Mutex
	conn      net.Conn
	quickConn net.Conn
	unpacker  *protocol.Unpacker
	server    string
	info      any
	cid       uint32
	raw       bool
	sendTO    time.Duration
	tlsOpt    *transport.TLSOptions
	onConnect func(*Client, bool, any)
	onMessage func(*Client, string, Payload, bool)
	onData    func(*Client, string, Payload, bool)
	pending   map[uint32]chan *protocol.Decoded
	seq       uint32
	closed    bool
	posServers []string
	robotPingTurbo time.Duration
}

type RemoteClient struct {
	server   *Server
	conn     net.Conn
	quickUDP *net.UDPConn
	quickAddr *net.UDPAddr
	id       uint32
	authed   bool
	priority int
	subs     map[string]struct{}
	sendTO   time.Duration
	closed   bool
	mu       sync.Mutex
	onSubscribe func(*RemoteClient, []string)
	onUnsubscribe func(*RemoteClient, []string)
}

type Server struct {
	mu        sync.RWMutex
	info      any
	passwd    string
	raw       bool
	listener  net.Listener
	quick     *net.UDPConn
	clients   map[uint32]*RemoteClient
	commands  map[string]commandEntry
	prefixCmd map[string]commandEntry
	nextCID   uint32
	onClient  func(*RemoteClient, bool)
	onData    func(*RemoteClient, string, Payload, bool)
	sendTO    time.Duration
	tlsOpt    *transport.TLSOptions
}

type commandEntry struct {
	h Handler
	q *workqueue.Queue
}

type Stream struct {
	listener net.Listener
	conn     net.Conn
	TunID    uint16
	sendTO   time.Duration
}

func NewServer(info any, passwd string, raw bool) *Server {
	return &Server{
		info:      info,
		passwd:    passwd,
		raw:       raw,
		clients:   map[uint32]*RemoteClient{},
		commands:  map[string]commandEntry{},
		prefixCmd: map[string]commandEntry{},
		sendTO:    100 * time.Millisecond,
		onClient:  func(*RemoteClient, bool) {},
		onData:    func(*RemoteClient, string, Payload, bool) {},
	}
}

func (s *Server) OnClient(fn func(*RemoteClient, bool)) { s.onClient = fn }
func (s *Server) OnData(fn func(*RemoteClient, string, Payload, bool)) { s.onData = fn }
func (s *Server) SendTimeout(timeout time.Duration) { s.sendTO = timeout }

func (s *Server) Command(url string, q *workqueue.Queue, h Handler) {
	if strings.HasSuffix(url, "/") {
		s.prefixCmd[url] = commandEntry{h: h, q: q}
	} else {
		s.commands[url] = commandEntry{h: h, q: q}
	}
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) Address() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) Clients() []*RemoteClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*RemoteClient, 0, len(s.clients))
	for _, c := range s.clients {
		out = append(out, c)
	}
	return out
}

func (s *Server) IsSubscribed(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.clients {
		if c.IsSubscribed(url) {
			return true
		}
	}
	return false
}

func (s *Server) Publish(url string, payload *Payload, quick bool) bool {
	if !strings.HasPrefix(url, "/") {
		panic("URL must start with /")
	}
	s.mu.RLock()
	clients := make([]*RemoteClient, 0, len(s.clients))
	for _, cli := range s.clients {
		if cli.IsSubscribed(url) {
			clients = append(clients, cli)
		}
	}
	s.mu.RUnlock()
	ok := true
	for _, cli := range clients {
		if !cli.send(protocol.TypePublish, 0, 0, 0, url, payload, quick) {
			ok = false
		}
	}
	return ok
}

func (s *Server) CreateStream(onlink func(*Stream, bool), ondata func(*Stream, []byte), timeout time.Duration) (*Stream, error) {
	if s.listener == nil {
		return nil, errors.New("server not run")
	}
	addr := s.listener.Addr().String()
	host, _, _ := net.SplitHostPort(addr)
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return nil, err
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := net.LookupPort("tcp", portStr)
	stream := &Stream{listener: ln, TunID: uint16(port), sendTO: s.sendTO}
	go func() {
		_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(timeout))
		conn, err := ln.Accept()
		if err != nil {
			onlink(stream, false)
			return
		}
		stream.conn = conn
		onlink(stream, true)
		if ondata != nil {
			buf := make([]byte, protocol.MaxPacketLength)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					break
				}
				ondata(stream, append([]byte(nil), buf[:n]...))
			}
		}
		onlink(stream, false)
	}()
	return stream, nil
}

func (s *Stream) Connected() bool { return s.conn != nil }
func (s *Stream) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
func (s *Stream) Send(data []byte) (int, error) {
	if s.conn == nil {
		return 0, errors.New("stream not connected")
	}
	if err := transport.WriteFull(s.conn, data, s.sendTO); err != nil {
		return 0, err
	}
	return len(data), nil
}
func (s *Stream) SendTimeout(timeout time.Duration) { s.sendTO = timeout }

func (s *Server) Run(addr string, tlsOpt *transport.TLSOptions) error {
	var ln net.Listener
	var err error
	if tlsOpt != nil {
		cfg, err := transport.NewServerTLSConfig(*tlsOpt)
		if err != nil {
			return err
		}
		ln, err = tls.Listen("tcp", addr, cfg)
		if err != nil {
			return err
		}
		s.tlsOpt = tlsOpt
	} else {
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		udpAddr, _ := net.ResolveUDPAddr("udp", addr)
		s.quick, _ = net.ListenUDP("udp", udpAddr)
		go s.quickLoop()
	}
	s.listener = ln
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) quickLoop() {
	buf := make([]byte, protocol.MaxQPacketLength)
	for {
		n, _, err := s.quick.ReadFromUDP(buf)
		if err != nil {
			return
		}
		pkt, err := protocol.DecodePacket(buf[:n], protocol.DecodeOptions{Raw: s.raw})
		if err != nil || pkt.Header.Type != protocol.TypeDatagram {
			continue
		}
		s.mu.RLock()
		cli := s.clients[pkt.Header.SeqNo]
		s.mu.RUnlock()
		if cli != nil {
			s.handlePacket(cli, pkt, true)
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	cli := &RemoteClient{server: s, conn: conn, subs: map[string]struct{}{}, sendTO: s.sendTO}
	up := protocol.NewUnpacker(s.raw)
	buf := make([]byte, protocol.MaxPacketLength)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		if err := up.Feed(buf[:n], func(pkt *protocol.Decoded) error {
			if cli.id == 0 && pkt.Header.Type == protocol.TypeServInfo {
				return s.handshake(cli, pkt)
			}
			s.handlePacket(cli, pkt, false)
			return nil
		}); err != nil {
			break
		}
	}
	cli.closeInternal()
}

func (s *Server) handshake(cli *RemoteClient, pkt *protocol.Decoded) error {
	if s.passwd != "" {
		m, _ := pkt.Payload.Param.(map[string]any)
		if m == nil || fmt.Sprint(m["passwd"]) != s.passwd {
			cli.send(protocol.TypeServInfo, protocol.FlagReply, protocol.StatusPassword, pkt.Header.SeqNo, "", nil, false)
			return errors.New("invalid password")
		}
	}
	s.mu.Lock()
	s.nextCID++
	if s.nextCID == 0 {
		s.nextCID++
	}
	cli.id = s.nextCID
	cli.authed = true
	s.clients[cli.id] = cli
	s.mu.Unlock()
	payload := &Payload{Param: s.info}
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, cli.id)
	if s.quick != nil && pkt.Header.Flags&protocol.FlagTunnel != 0 {
		addr := connRemoteIP(cli.conn)
		cli.quickAddr = &net.UDPAddr{IP: addr, Port: int(pkt.Header.TunID)}
		if udpAddr, ok := s.quick.LocalAddr().(*net.UDPAddr); ok {
			data = append(data, 0, 0)
			binary.BigEndian.PutUint16(data[4:6], uint16(udpAddr.Port))
		}
	}
	payload.Data = data
	cli.send(protocol.TypeServInfo, protocol.FlagReply, 0, pkt.Header.SeqNo, "", payload, false)
	s.onClient(cli, true)
	return nil
}

func (s *Server) handlePacket(cli *RemoteClient, pkt *protocol.Decoded, quick bool) {
	if pkt.Header.Flags&protocol.FlagReply != 0 || pkt.Header.Type == protocol.TypeNoop {
		return
	}
	payload := pkt.Payload
	switch pkt.Header.Type {
	case protocol.TypeDatagram:
		s.onData(cli, pkt.URL, payload, quick)
	case protocol.TypePingEcho:
		cli.send(protocol.TypePingEcho, protocol.FlagReply, 0, pkt.Header.SeqNo, "", nil, false)
	case protocol.TypeSubscribe:
		status := cli.subscribe(pkt.URL, payload.Param)
		cli.send(protocol.TypeSubscribe, protocol.FlagReply, status, pkt.Header.SeqNo, "", nil, false)
	case protocol.TypeUnsubscribe:
		status := cli.unsubscribe(pkt.URL, payload.Param)
		cli.send(protocol.TypeUnsubscribe, protocol.FlagReply, status, pkt.Header.SeqNo, "", nil, false)
	case protocol.TypeRPC:
		entry, ok := s.findCommand(pkt.URL)
		if !ok {
			cli.send(protocol.TypeRPC, protocol.FlagReply, protocol.StatusInvalidURL, pkt.Header.SeqNo, "", nil, false)
			return
		}
		req := Request{URL: pkt.URL, SeqNo: pkt.Header.SeqNo, Method: protocol.MethodGet, MWData: map[string]any{}}
		if pkt.Header.Flags&protocol.FlagSet != 0 {
			req.Method = protocol.MethodSet
		}
		run := func() { entry.h(cli, req, payload) }
		if entry.q != nil {
			entry.q.Add(run)
		} else {
			run()
		}
	}
}

func (s *Server) findCommand(url string) (commandEntry, bool) {
	if entry, ok := s.commands[url]; ok {
		return entry, true
	}
	for prefix, entry := range s.prefixCmd {
		if strings.HasPrefix(url, prefix) && len(url) > len(prefix) && url[len(prefix)] == '/' {
			return entry, true
		}
	}
	if entry, ok := s.prefixCmd["/"]; ok {
		return entry, true
	}
	return commandEntry{}, false
}

func (c *RemoteClient) send(typ, flags, status uint8, seqno uint32, url string, payload *Payload, quick bool) bool {
	if quick && c.quickAddr == nil {
		return false
	}
	b := protocol.NewBuilder().Header(typ, flags, status, seqno).URL(url)
	if payload != nil {
		if err := b.Payload(payload); err != nil {
			return false
		}
	}
	pkt, err := b.Packet()
	if err != nil {
		return false
	}
	if quick {
		_, err = c.server.quick.WriteToUDP(pkt, c.quickAddr)
		return err == nil
	}
	return transport.WriteFull(c.conn, pkt, c.sendTO) == nil
}

func (c *RemoteClient) ID() uint32 { return c.id }
func (c *RemoteClient) Address() net.Addr { return c.conn.RemoteAddr() }
func (c *RemoteClient) SetAuthed(v bool) { c.authed = v }
func (c *RemoteClient) OnSubscribe(fn func(*RemoteClient, []string)) { c.onSubscribe = fn }
func (c *RemoteClient) OnUnsubscribe(fn func(*RemoteClient, []string)) { c.onUnsubscribe = fn }
func (c *RemoteClient) Close() { c.closeInternal() }
func (c *RemoteClient) IsClosed() bool { return c.closed }
func (c *RemoteClient) Keepalive(idle int) {
	if tc, ok := c.conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(time.Duration(idle) * time.Second)
	}
}
func (c *RemoteClient) GetPeerCert() any {
	if tc, ok := c.conn.(*tls.Conn); ok {
		state := tc.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			return state.PeerCertificates[0]
		}
	}
	return nil
}
func (c *RemoteClient) Priority() int { return c.priority }
func (c *RemoteClient) SetPriority(prio int) {
	if prio < 0 || prio > 7 {
		return
	}
	c.mu.Lock()
	c.priority = prio
	c.mu.Unlock()
}
func (c *RemoteClient) Reply(seqno uint32, payload *Payload, status uint8, tunid uint16) bool {
	b := protocol.NewBuilder().Header(protocol.TypeRPC, protocol.FlagReply, status, seqno).TunID(tunid)
	if payload != nil {
		if err := b.Payload(payload); err != nil { return false }
	}
	pkt, err := b.Packet()
	if err != nil { return false }
	return transport.WriteFull(c.conn, pkt, c.sendTO) == nil
}
func (c *RemoteClient) Datagram(url string, payload *Payload, quick bool) bool {
	return c.send(protocol.TypeDatagram, 0, 0, 0, url, payload, quick)
}
func (c *RemoteClient) IsSubscribed(url string) bool {
	if !c.authed {
		return false
	}
	for sub := range c.subs {
		if protocol.MatchSubscription(sub, url) {
			return true
		}
	}
	return false
}
func (c *RemoteClient) subscribe(url string, param any) uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	topics := []string{}
	if url != "" {
		if !strings.HasPrefix(url, "/") {
			return protocol.StatusArguments
		}
		c.subs[url] = struct{}{}
		topics = append(topics, url)
		if c.onSubscribe != nil {
			go c.onSubscribe(c, topics)
		}
		return 0
	}
	list, ok := param.([]any)
	if !ok {
		return protocol.StatusArguments
	}
	for _, item := range list {
		if s, ok := item.(string); ok && strings.HasPrefix(s, "/") {
			c.subs[s] = struct{}{}
			topics = append(topics, s)
		}
	}
	if c.onSubscribe != nil && len(topics) > 0 {
		go c.onSubscribe(c, topics)
	}
	return 0
}

func (c *RemoteClient) unsubscribe(url string, param any) uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	topics := []string{}
	if url != "" {
		if !strings.HasPrefix(url, "/") {
			return protocol.StatusArguments
		}
		delete(c.subs, url)
		topics = append(topics, url)
		if c.onUnsubscribe != nil {
			go c.onUnsubscribe(c, topics)
		}
		return 0
	}
	list, ok := param.([]any)
	if !ok {
		return protocol.StatusArguments
	}
	for _, item := range list {
		if s, ok := item.(string); ok {
			delete(c.subs, s)
			topics = append(topics, s)
		}
	}
	if c.onUnsubscribe != nil && len(topics) > 0 {
		go c.onUnsubscribe(c, topics)
	}
	return 0
}
func (c *RemoteClient) closeInternal() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	_ = c.conn.Close()
	c.server.mu.Lock()
	delete(c.server.clients, c.id)
	c.server.mu.Unlock()
	c.server.onClient(c, false)
}

func NewClient(raw bool) *Client {
	return &Client{
		unpacker:  protocol.NewUnpacker(raw),
		raw:       raw,
		sendTO:    500 * time.Millisecond,
		onConnect: func(*Client, bool, any) {},
		onMessage: func(*Client, string, Payload, bool) {},
		onData:    func(*Client, string, Payload, bool) {},
		pending:   map[uint32]chan *protocol.Decoded{},
	}
}

func (c *Client) SetPositionServers(servers ...string) { c.posServers = append([]string(nil), servers...) }
func (c *Client) Pendings() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

func (c *Client) OnConnect(fn func(*Client, bool, any)) { c.onConnect = fn }
func (c *Client) OnMessage(fn func(*Client, string, Payload, bool)) { c.onMessage = fn }
func (c *Client) OnData(fn func(*Client, string, Payload, bool)) { c.onData = fn }
func (c *Client) Connected() bool { return c.conn != nil && !c.closed }
func (c *Client) GetPeerCert() any {
	if tc, ok := c.conn.(*tls.Conn); ok {
		state := tc.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			return state.PeerCertificates[0]
		}
	}
	return nil
}
func (c *Client) SetRobotPingTurbo(d time.Duration) { c.robotPingTurbo = d }
func (c *Client) RobotPingTurbo() time.Duration { return c.robotPingTurbo }
func (c *Client) Linger(sec int) bool {
	if c.conn == nil || c.closed {
		return false
	}
	if tc, ok := c.conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(sec)
		return true
	}
	return false
}
func (c *Client) SendTimeout() time.Duration { return c.sendTO }
func (c *Client) SetSendTimeout(d time.Duration) { c.sendTO = d }

func ParseURL(raw string) (hostport string, path string, err error) {
	if !strings.HasPrefix(raw, "vsoa://") {
		return "", "", errors.New("invalid scheme")
	}
	u := strings.TrimPrefix(raw, "vsoa://")
	parts := strings.SplitN(u, "/", 2)
	hostport = parts[0]
	path = "/"
	if len(parts) == 2 && parts[1] != "" {
		path = "/" + parts[1]
	}
	return
}

func resolveHostPort(ctx context.Context, hostport string, posServers []string) (string, error) {
	if hostport == "" {
		return "", errors.New("empty host")
	}
	if strings.Contains(hostport, ":") {
		return hostport, nil
	}
	addr, err := vsoapos.Lookup(ctx, hostport, posServers)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

func (c *Client) Connect(rawURL, passwd string, timeout time.Duration, tlsOpt *transport.TLSOptions) error {
	hostport, _, err := ParseURL(rawURL)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	hostport, err = resolveHostPort(ctx, hostport, c.posServers)
	if err != nil {
		return err
	}
	dialer := net.Dialer{Timeout: timeout}
	var conn net.Conn
	if tlsOpt != nil {
		cfg, _, err := transport.NewClientTLSConfig(*tlsOpt)
		if err != nil {
			return err
		}
		conn, err = tls.DialWithDialer(&dialer, "tcp", hostport, cfg)
		if err != nil {
			return err
		}
		c.tlsOpt = tlsOpt
	} else {
		conn, err = dialer.Dial("tcp", hostport)
		if err != nil {
			return err
		}
		udpConn, _ := net.Dial("udp", hostport)
		c.quickConn = udpConn
	}
	c.conn = conn
	c.server = hostport
	var tunid uint16
	if c.quickConn != nil {
		if udpAddr, ok := c.quickConn.LocalAddr().(*net.UDPAddr); ok {
			tunid = uint16(udpAddr.Port)
		}
	}
	payload := (*Payload)(nil)
	if passwd != "" {
		payload = &Payload{Param: map[string]any{"passwd": passwd}}
	}
	b := protocol.NewBuilder().Header(protocol.TypeServInfo, 0, 0, 0).TunID(tunid)
	if payload != nil {
		_ = b.Payload(payload)
	}
	pkt, _ := b.Packet()
	if err := transport.WriteFull(c.conn, pkt, c.sendTO); err != nil {
		return err
	}
	buf := make([]byte, protocol.MaxPacketLength)
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	n, err := c.conn.Read(buf)
	_ = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}
	decoded, err := protocol.DecodePacket(buf[:n], protocol.DecodeOptions{})
	if err != nil {
		return err
	}
	if decoded.Header.Type != protocol.TypeServInfo || decoded.Header.Flags&protocol.FlagReply == 0 || decoded.Header.Status != 0 {
		return errors.New("invalid responding")
	}
	c.info = decoded.Payload.Param
	if len(decoded.Payload.Data) >= 4 {
		c.cid = binary.BigEndian.Uint32(decoded.Payload.Data[:4])
	}
	if len(decoded.Payload.Data) >= 6 && c.quickConn != nil {
		port := binary.BigEndian.Uint16(decoded.Payload.Data[4:6])
		_ = c.quickConn.Close()
		c.quickConn, _ = net.Dial("udp", connRemoteIPPort(c.conn, int(port)))
	}
	c.closed = false
	c.onConnect(c, true, c.info)
	go c.loop()
	return nil
}

func (c *Client) loop() {
	buf := make([]byte, protocol.MaxPacketLength)
	for {
		conn := c.conn
		if conn == nil {
			break
		}
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		if err := c.unpacker.Feed(buf[:n], func(pkt *protocol.Decoded) error {
			c.handlePacket(pkt, false)
			return nil
		}); err != nil {
			break
		}
	}
	if !c.closed {
		c.Close()
	}
}

func (c *Client) handlePacket(pkt *protocol.Decoded, quick bool) {
	switch pkt.Header.Type {
	case protocol.TypeDatagram:
		c.onData(c, pkt.URL, pkt.Payload, quick)
	case protocol.TypePublish:
		c.onMessage(c, pkt.URL, pkt.Payload, quick)
	case protocol.TypeQOSSetup:
		return
	default:
		if pkt.Header.Flags&protocol.FlagReply != 0 {
			c.mu.Lock()
			ch := c.pending[pkt.Header.SeqNo]
			delete(c.pending, pkt.Header.SeqNo)
			c.mu.Unlock()
			if ch != nil {
				ch <- pkt
			}
		}
	}
}

func (c *Client) nextSeq() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	if c.seq == 0 {
		c.seq++
	}
	return c.seq
}

func (c *Client) request(typ, flags uint8, url string, payload *Payload, timeout time.Duration, waitReply bool) (*protocol.Decoded, error) {
	if c.closed || c.conn == nil {
		return nil, errors.New("not connected")
	}
	seq := c.nextSeq()
	b := protocol.NewBuilder().Header(typ, flags, 0, seq).URL(url)
	if payload != nil {
		if err := b.Payload(payload); err != nil {
			return nil, err
		}
	}
	pkt, err := b.Packet()
	if err != nil {
		return nil, err
	}
	var ch chan *protocol.Decoded
	if waitReply {
		ch = make(chan *protocol.Decoded, 1)
		c.mu.Lock()
		c.pending[seq] = ch
		c.mu.Unlock()
	}
	if err := transport.WriteFull(c.conn, pkt, c.sendTO); err != nil {
		return nil, err
	}
	if !waitReply {
		return nil, nil
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
		return nil, errors.New("timeout")
	}
}

func (c *Client) Call(url string, method int, payload *Payload, timeout time.Duration) (*Header, *Payload, error) {
	flags := uint8(0)
	if method != 0 { flags = protocol.FlagSet }
	resp, err := c.request(protocol.TypeRPC, flags, url, payload, timeout, true)
	if err != nil { return nil, nil, err }
	return &resp.Header, &resp.Payload, nil
}

func (c *Client) CallAsync(url string, method int, payload *Payload, timeout time.Duration, callback func(*Header, *Payload, error)) error {
	go func() {
		h, p, err := c.Call(url, method, payload, timeout)
		callback(h, p, err)
	}()
	return nil
}

func (c *Client) Ping(timeout time.Duration) error {
	_, err := c.request(protocol.TypePingEcho, 0, "", nil, timeout, true)
	return err
}

func (c *Client) Subscribe(urls any, timeout time.Duration) error {
	var url string
	var payload *Payload
	switch x := urls.(type) {
	case string:
		url = x
	case []string:
		items := make([]any, 0, len(x))
		for _, s := range x { items = append(items, s) }
		payload = &Payload{Param: items}
	default:
		return errors.New("invalid url")
	}
	_, err := c.request(protocol.TypeSubscribe, 0, url, payload, timeout, true)
	return err
}

func (c *Client) Unsubscribe(urls any, timeout time.Duration) error {
	var url string
	var payload *Payload
	switch x := urls.(type) {
	case string:
		url = x
	case []string:
		items := make([]any, 0, len(x))
		for _, s := range x { items = append(items, s) }
		payload = &Payload{Param: items}
	default:
		return errors.New("invalid url")
	}
	_, err := c.request(protocol.TypeUnsubscribe, 0, url, payload, timeout, true)
	return err
}

func (c *Client) Datagram(url string, payload *Payload, quick bool) error {
	seq := uint32(0)
	if quick {
		if c.quickConn == nil || c.cid == 0 { return errors.New("quick unavailable") }
		seq = c.cid
	}
	b := protocol.NewBuilder().Header(protocol.TypeDatagram, 0, 0, seq).URL(url)
	if payload != nil {
		if err := b.Payload(payload); err != nil { return err }
	}
	pkt, err := b.Packet()
	if err != nil { return err }
	if quick {
		return transport.WriteFull(c.quickConn, pkt, c.sendTO)
	}
	return transport.WriteFull(c.conn, pkt, c.sendTO)
}

func (c *Client) CreateStream(tunid uint16, onlink func(net.Conn, bool), ondata func([]byte), timeout time.Duration) (net.Conn, error) {
	host, _, _ := net.SplitHostPort(c.server)
	addr := net.JoinHostPort(host, fmt.Sprint(tunid))
	dialer := net.Dialer{Timeout: timeout}
	var conn net.Conn
	var err error
	if c.tlsOpt != nil {
		cfg, _, cfgErr := transport.NewClientTLSConfig(*c.tlsOpt)
		if cfgErr != nil {
			return nil, cfgErr
		}
		conn, err = tls.DialWithDialer(&dialer, "tcp", addr, cfg)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	onlink(conn, true)
	go func() {
		buf := make([]byte, protocol.MaxPacketLength)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				break
			}
			if ondata != nil {
				ondata(append([]byte(nil), buf[:n]...))
			}
		}
		onlink(conn, false)
	}()
	return conn, nil
}

const clientAutoMaxPingLost = 3

type robotState struct {
	mu        sync.Mutex
	pingLost  int
	lastPing  time.Time
	lastTurbo time.Time
	keepalive time.Duration
}

func (c *Client) Robot(ctx context.Context, rawURL, passwd string, keepalive, connTimeout, reconnDelay time.Duration, tlsOpt *transport.TLSOptions) {
	rs := &robotState{keepalive: keepalive}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := c.Connect(rawURL, passwd, connTimeout, tlsOpt); err != nil {
				time.Sleep(reconnDelay)
				continue
			}
			rs.mu.Lock()
			rs.pingLost = 0
			rs.lastPing = time.Now()
			rs.lastTurbo = time.Now()
			rs.mu.Unlock()

			interval := keepalive
			if c.robotPingTurbo > 0 && c.robotPingTurbo < interval {
				interval = c.robotPingTurbo
			}
			ticker := time.NewTicker(interval)
			for c.Connected() {
				select {
				case <-ctx.Done():
					c.Close()
					ticker.Stop()
					return
				case <-ticker.C:
					rs.mu.Lock()
					elapsed := time.Since(rs.lastPing)
					turbo := c.robotPingTurbo
					rs.mu.Unlock()

					needPing := false
					if elapsed >= keepalive {
						needPing = true
					} else if turbo > 0 && c.Pendings() > 0 && elapsed >= turbo {
						needPing = true
					}
					if needPing {
						err := c.Ping(keepalive)
						rs.mu.Lock()
						if err != nil {
							rs.pingLost++
							if rs.pingLost > clientAutoMaxPingLost {
								rs.mu.Unlock()
								c.Close()
								break
							}
						} else {
							rs.pingLost = 0
						}
						rs.lastPing = time.Now()
						rs.lastTurbo = time.Now()
						rs.mu.Unlock()
					}
				}
			}
			ticker.Stop()
			time.Sleep(reconnDelay)
		}
	}()
}

func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conn := c.conn
	quick := c.quickConn
	c.conn = nil
	c.quickConn = nil
	c.mu.Unlock()
	if conn != nil { _ = conn.Close() }
	if quick != nil { _ = quick.Close() }
	c.onConnect(c, false, nil)
}

func Fetch(rawURL, passwd string, method int, payload *Payload, timeout time.Duration, raw bool, tlsOpt *transport.TLSOptions) (*Header, *Payload, error) {
	cli := NewClient(raw)
	if err := cli.Connect(rawURL, passwd, timeout, tlsOpt); err != nil {
		return nil, nil, err
	}
	defer cli.Close()
	_, path, err := ParseURL(rawURL)
	if err != nil {
		return nil, nil, err
	}
	return cli.Call(path, method, payload, timeout)
}

type PositionQuery = vsoapos.Query
type PositionServerInfo = vsoapos.ServerInfo

type PositionServer = vsoapos.Server

func ListenPosition(addr string, handler func(PositionQuery) *PositionServerInfo) (*PositionServer, error) {
	return vsoapos.Listen(addr, func(q vsoapos.Query) *vsoapos.ServerInfo { return handler(q) })
}

var globalPosServer []string

func SetPositionServer(addr string, port int) { globalPosServer = []string{net.JoinHostPort(addr, fmt.Sprint(port))} }
func LookupPosition(name string) (*net.UDPAddr, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return vsoapos.Lookup(ctx, name, globalPosServer)
}

func connRemoteIP(conn net.Conn) net.IP {
	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	return net.ParseIP(host)
}

func connRemoteIPPort(conn net.Conn, port int) string {
	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	return net.JoinHostPort(host, fmt.Sprint(port))
}
