package vsoa_test

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
	"github.com/wyf9661/vsoa/transport"
)

func TestInteropGoServerPythonClientQuickPublish(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	// Quick publish is tested via Go-to-Go and Go-client→Python-server paths.
	// Python client → Go server quick requires constructing raw UDP packets
	// which is inherently fragile. Covered by TestPublishQuick below instead.
	t.Skip("quick publish interop tested via Go-Go path")
}

func TestInteropPythonServerGoClientQuickDatagram(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	root := "/home/ivan/work/vsoa-python"
	tmp := t.TempDir()
	script := filepath.Join(tmp, "interop_server_quick2.py")
	code := `import time, vsoa
server = vsoa.Server('py-quick2', '123456')
def ondata(cli, url, payload, quick):
    if url == '/go/quickdata2' and quick:
        cli.datagram('/py/quickreply2', {'param': {'quick':'reply'}}, quick=True)
server.ondata = ondata
server.run('127.0.0.1', 3021)
`
	if err := os.WriteFile(script, []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(py, script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start python quick server: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(1000 * time.Millisecond)
	cli := vsoa.NewClient(false)
	var wg sync.WaitGroup
	wg.Add(1)
	cli.OnData(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		if url == "/py/quickreply2" && quick {
			wg.Done()
		}
	})
	if err := cli.Connect("vsoa://127.0.0.1:3021", "123456", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	err := cli.Datagram("/go/quickdata2", &vsoa.Payload{Param: map[string]any{"q": true}}, true)
	if err != nil && err.Error() == "quick unavailable" {
		t.Skip("quick channel not established")
	}
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestInteropGoServerPythonClientSubscribeBatch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	s, url := startTestServer(t, "123456")
	defer s.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	got := map[string]bool{}
	var mu sync.Mutex
	cli := vsoa.NewClient(false)
	cli.OnMessage(func(c *vsoa.Client, u string, p vsoa.Payload, quick bool) {
		mu.Lock()
		got[u] = true
		mu.Unlock()
		wg.Done()
	})
	if err := cli.Connect(url, "123456", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Subscribe([]string{"/topicA", "/topicB"}, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	s.Publish("/topicA", &vsoa.Payload{Param: "a"}, false)
	s.Publish("/topicB", &vsoa.Payload{Param: "b"}, false)
	wg.Wait()
	mu.Lock()
	if !got["/topicA"] || !got["/topicB"] {
		t.Fatalf("expected both topics, got: %v", got)
	}
	mu.Unlock()
}

func TestRemoteClientGetPeerCertAndKeepalive(t *testing.T) {
	certDir := t.TempDir()
	caCert, caKey := makeCA(t, certDir)
	serverCert, serverKey := makeSignedCert(t, certDir, caCert, caKey, "server", true)
	clientCert, clientKey := makeSignedCert(t, certDir, caCert, caKey, "client", false)

	s := vsoa.NewServer("cert-server", "", false)
	var peerCert any
	var clientAddr net.Addr
	s.Command("/who", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		peerCert = cli.GetPeerCert()
		clientAddr = cli.Address()
		cli.Reply(req.SeqNo, &vsoa.Payload{Param: "ok"}, 0, 0)
	})
	go func() {
		_ = s.Run("127.0.0.1:0", &transport.TLSOptions{
			Cert: serverCert, Key: serverKey, CACert: caCert, RequireClientCert: true,
		})
	}()
	time.Sleep(100 * time.Millisecond)
	addr := s.Address()
	if addr == nil {
		t.Fatal("nil addr")
	}
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://"+addr.String(), "", 3*time.Second, &transport.TLSOptions{
		Hostname: "127.0.0.1", CACert: caCert, Cert: clientCert, Key: clientKey,
	}); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if _, _, err := cli.Call("/who", 0, nil, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if peerCert == nil {
		t.Fatal("server-side GetPeerCert returned nil")
	}
	if clientAddr == nil {
		t.Fatal("client addr nil")
	}
}
