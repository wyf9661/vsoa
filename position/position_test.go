package position_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyf9661/vsoa/position"
)

func TestListenAndLookup(t *testing.T) {
	s, err := position.Listen("127.0.0.1:0", func(q position.Query) *position.ServerInfo {
		if q.Name == "pyserver" {
			return &position.ServerInfo{Addr: "127.0.0.1", Port: 3005, Domain: int(net.ParseIP("127.0.0.1").To4()[0])}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addr := s.Addr().String()
	got, err := position.Lookup(ctx, "pyserver", []string{addr})
	if err != nil {
		t.Fatal(err)
	}
	if got.IP.String() != "127.0.0.1" || got.Port != 3005 {
		t.Fatalf("lookup result = %v", got)
	}
}

func TestLookupFromEnv(t *testing.T) {
	s, err := position.Listen("127.0.0.1:0", func(q position.Query) *position.ServerInfo {
		if q.Name == "envsrv" {
			return &position.ServerInfo{Addr: "127.0.0.1", Port: 3100}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	os.Setenv("VSOA_POS_SERVER", s.Addr().String())
	defer os.Unsetenv("VSOA_POS_SERVER")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := position.Lookup(ctx, "envsrv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != 3100 {
		t.Fatalf("port = %d", got.Port)
	}
}

func TestLookupFromConfigFile(t *testing.T) {
	s, err := position.Listen("127.0.0.1:0", func(q position.Query) *position.ServerInfo {
		if q.Name == "filesrv" {
			return &position.ServerInfo{Addr: "127.0.0.1", Port: 3200}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	dir := t.TempDir()
	conf := filepath.Join(dir, "vsoa.pos")
	if err := os.WriteFile(conf, []byte(s.Addr().String()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := position.LookupWithConfig(ctx, "filesrv", nil, conf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != 3200 {
		t.Fatalf("port = %d", got.Port)
	}
}
