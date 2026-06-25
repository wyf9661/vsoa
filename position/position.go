package position

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type ServerInfo struct {
	Name     string `json:"name,omitempty"`
	Addr     string `json:"addr"`
	Port     int    `json:"port"`
	Domain   int    `json:"domain,omitempty"`
	Security bool   `json:"security,omitempty"`
}

type Query struct {
	Name   string `json:"name"`
	Domain int    `json:"domain"`
}

type Handler func(Query) *ServerInfo

type Server struct {
	conn    *net.UDPConn
	handler Handler
}

func (s *Server) Addr() net.Addr { return s.conn.LocalAddr() }

func Listen(addr string, handler Handler) (*Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	s := &Server{conn: conn, handler: handler}
	go s.loop()
	return s, nil
}

func (s *Server) loop() {
	buf := make([]byte, 4096)
	for {
		n, remote, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var q Query
		if json.Unmarshal(buf[:n], &q) != nil || q.Name == "" {
			continue
		}
		resp := s.handler(q)
		if resp == nil {
			resp = &ServerInfo{}
		}
		data, _ := json.Marshal(resp)
		_, _ = s.conn.WriteToUDP(data, remote)
	}
}

func (s *Server) Close() error { return s.conn.Close() }

func Lookup(ctx context.Context, name string, servers []string) (*net.UDPAddr, error) {
	return LookupWithConfig(ctx, name, servers, defaultConfigPath())
}

func LookupWithConfig(ctx context.Context, name string, servers []string, configPath string) (*net.UDPAddr, error) {
	if len(servers) == 0 {
		if env := os.Getenv("VSOA_POS_SERVER"); env != "" {
			servers = strings.Split(env, ",")
		}
	}
	if len(servers) == 0 && configPath != "" {
		if b, err := os.ReadFile(configPath); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(line)
				if line != "" {
					servers = append(servers, line)
				}
			}
		}
	}
	for _, server := range servers {
		addr, err := queryOne(ctx, name, normalize(server))
		if err == nil {
			return addr, nil
		}
	}
	return nil, errors.New("server not found")
}

func queryOne(ctx context.Context, name, target string) (*net.UDPAddr, error) {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "udp", target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	body, _ := json.Marshal(Query{Name: name})
	if _, err := conn.Write(body); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	var info ServerInfo
	if err := json.Unmarshal(buf[:n], &info); err != nil {
		return nil, err
	}
	return net.ResolveUDPAddr("udp", net.JoinHostPort(info.Addr, itoa(info.Port)))
}

func normalize(s string) string {
	if strings.Contains(s, ":") {
		return s
	}
	return net.JoinHostPort(s, "54")
}

func defaultConfigPath() string {
	if p := os.Getenv("VSOA_POS_CONF"); p != "" {
		return p
	}
	return "/etc/vsoa.pos"
}

func itoa(i int) string { return strconv.Itoa(i) }
