package vsoa_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
	"github.com/wyf9661/vsoa/transport"
)

func TestTLSMutualAuth(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := makeCA(t, dir)
	serverCert, serverKey := makeSignedCert(t, dir, caCert, caKey, "server", true)
	clientCert, clientKey := makeSignedCert(t, dir, caCert, caKey, "client", false)

	s := vsoa.NewServer("mtls-server", "", false)
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	go func() {
		_ = s.Run("127.0.0.1:0", &transport.TLSOptions{
			Cert: serverCert,
			Key: serverKey,
			CACert: caCert,
			RequireClientCert: true,
		})
	}()
	time.Sleep(150 * time.Millisecond)
	addr := s.Address()
	if addr == nil {
		t.Fatal("nil addr")
	}

	cli := vsoa.NewClient(false)
	err := cli.Connect("vsoa://"+addr.String(), "", 3*time.Second, &transport.TLSOptions{
		Hostname: "127.0.0.1",
		CACert: caCert,
		Cert: clientCert,
		Key: clientKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if _, _, err := cli.Call("/echo", 0, &vsoa.Payload{Param: "mtls"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestPositionTopLevelStyle(t *testing.T) {
	ps, err := vsoa.ListenPosition("127.0.0.1:0", func(q vsoa.PositionQuery) *vsoa.PositionServerInfo {
		if q.Name == "topsrv" {
			return &vsoa.PositionServerInfo{Addr: "127.0.0.1", Port: 3333}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	udp := ps.Addr().(*net.UDPAddr)
	vsoa.SetPositionServer(udp.IP.String(), udp.Port)
	addr, err := vsoa.LookupPosition("topsrv")
	if err != nil {
		t.Fatal(err)
	}
	if addr.Port != 3333 {
		t.Fatalf("port = %d", addr.Port)
	}
}

func TestRobotTurboWithPendingRPC(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()
	block := make(chan struct{})
	s.Command("/slow", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		<-block
		cli.Reply(req.SeqNo, &vsoa.Payload{Param: "ok"}, 0, 0)
	})
	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.SetRobotPingTurbo(100 * time.Millisecond)
	err := cli.CallAsync("/slow", 0, nil, 3*time.Second, func(h *vsoa.Header, p *vsoa.Payload, err error) {})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if cli.Pendings() == 0 {
		t.Fatal("expected pending during slow rpc")
	}
	close(block)
	time.Sleep(100 * time.Millisecond)
}

func makeCA(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { t.Fatal(err) }
	tpl := x509.Certificate{
		SerialNumber:          bigOne(),
		Subject:               pkix.Name{CommonName: "vsoa-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &priv.PublicKey, priv)
	if err != nil { t.Fatal(err) }
	certFile := filepath.Join(dir, "ca.crt")
	keyFile := filepath.Join(dir, "ca.key")
	writePEM(t, certFile, "CERTIFICATE", der, 0o644)
	writePEM(t, keyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv), 0o600)
	return certFile, keyFile
}

func makeSignedCert(t *testing.T, dir, caCertPath, caKeyPath, name string, server bool) (string, string) {
	t.Helper()
	caPEM, err := os.ReadFile(caCertPath)
	if err != nil { t.Fatal(err) }
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil { t.Fatal(err) }
	caBlock, _ := pem.Decode(caPEM)
	cakBlock, _ := pem.Decode(caKeyPEM)
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil { t.Fatal(err) }
	cak, err := x509.ParsePKCS1PrivateKey(cakBlock.Bytes)
	if err != nil { t.Fatal(err) }
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { t.Fatal(err) }
	tpl := x509.Certificate{
		SerialNumber: bigOne(),
		Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}
	if server {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tpl.DNSNames = []string{"localhost"}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, ca, &priv.PublicKey, cak)
	if err != nil { t.Fatal(err) }
	certFile := filepath.Join(dir, name+".crt")
	keyFile := filepath.Join(dir, name+".key")
	writePEM(t, certFile, "CERTIFICATE", der, 0o644)
	writePEM(t, keyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv), 0o600)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, kind string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), mode); err != nil {
		t.Fatal(err)
	}
}

func bigOne() *big.Int { return big.NewInt(time.Now().UnixNano()) }
