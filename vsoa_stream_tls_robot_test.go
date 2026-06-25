package vsoa_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
	"github.com/wyf9661/vsoa/transport"
)

func TestStreamRoundTrip(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()
	var streamTun uint16
	s.Command("/stream", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		st, err := s.CreateStream(func(st *vsoa.Stream, conn bool) {
			if conn {
				_, _ = st.Send([]byte("hello-stream"))
			}
		}, nil, 3*time.Second)
		if err != nil {
			cli.Reply(req.SeqNo, nil, 2, 0)
			return
		}
		streamTun = st.TunID
		cli.Reply(req.SeqNo, nil, 0, st.TunID)
	})
	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	h, _, err := cli.Call("/stream", 0, nil, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil || h.TunID == 0 || h.TunID != streamTun {
		t.Fatalf("bad tunid: %+v want %d", h, streamTun)
	}
	var got atomic.Bool
	conn, err := cli.CreateStream(h.TunID, func(conn net.Conn, up bool) {}, func(data []byte) {
		if string(data) == "hello-stream" {
			got.Store(true)
		}
	}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(200 * time.Millisecond)
	if !got.Load() {
		t.Fatal("did not receive stream payload")
	}
}

func TestRobotReconnectAndTurboSetter(t *testing.T) {
	s, url := startTestServer(t, "")
	cli := vsoa.NewClient(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.Robot(ctx, url, "", 200*time.Millisecond, 2*time.Second, 100*time.Millisecond, nil)
	time.Sleep(400 * time.Millisecond)
	if !cli.Connected() {
		t.Fatal("robot did not connect")
	}
	cli.SetRobotPingTurbo(100 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if cli.RobotPingTurbo() != 100*time.Millisecond {
		t.Fatalf("turbo = %v", cli.RobotPingTurbo())
	}
	_ = s.Close()
	time.Sleep(400 * time.Millisecond)
	s2, _ := startTestServer(t, "")
	defer s2.Close()
	time.Sleep(600 * time.Millisecond)
	if !cli.Connected() {
		t.Fatal("robot did not reconnect")
	}
}

func TestTLSConnectAndPeerCert(t *testing.T) {
	certDir := t.TempDir()
	certFile, keyFile := makeSelfSigned(t, certDir)
	s := vsoa.NewServer("tls-server", "", false)
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	go func() {
		_ = s.Run("127.0.0.1:0", &transport.TLSOptions{Cert: certFile, Key: keyFile})
	}()
	time.Sleep(100 * time.Millisecond)
	addr := s.Address()
	if addr == nil {
		t.Fatal("nil addr")
	}
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://"+addr.String(), "", 3*time.Second, &transport.TLSOptions{Hostname: "127.0.0.1", InsecureSkipVerify: true}); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if cli.GetPeerCert() == nil {
		t.Fatal("expected peer cert")
	}
	if _, _, err := cli.Call("/echo", 0, &vsoa.Payload{Param: "tls"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func makeSelfSigned(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"127.0.0.1", "localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
