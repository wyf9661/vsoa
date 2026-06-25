package vsoa_test

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
)

func TestInteropGoServerPythonClientDatagram(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	s, url := startTestServer(t, "123456")
	defer s.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	s.OnData(func(cli *vsoa.RemoteClient, url string, payload vsoa.Payload, quick bool) {
		if url == "/interop/data" {
			wg.Done()
		}
	})
	hostport := strings.TrimPrefix(url, "vsoa://")
	cmd := exec.Command(py, "-c", `import time,vsoa; c=vsoa.Client(); r=c.connect('vsoa://`+hostport+`','123456',3); assert r==0,r; c.datagram('/interop/data', {'param': {'python':'client'}}, False); time.sleep(0.5); c.close()`)
	cmd.Dir = "/home/ivan/work/vsoa-python"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python client datagram failed: %v\n%s", err, string(out))
	}
	wg.Wait()
}

func TestInteropPythonServerGoClientDatagram(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	root := "/home/ivan/work/vsoa-python"
	script := filepath.Join(root, "interop_server_datagram.py")
	code := `import time, vsoa
server = vsoa.Server('py-dgram', '123456')
seen = {'ok': False}
def ondata(cli, url, payload, quick):
    if url == '/go/data':
        seen['ok'] = True
        cli.datagram('/py/reply', {'param': {'from':'python'}}, quick=False)
server.ondata = ondata
server.run('127.0.0.1', 3015)
`
	if err := os.WriteFile(script, []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(py, script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start python datagram server: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(800 * time.Millisecond)
	cli := vsoa.NewClient(false)
	var wg sync.WaitGroup
	wg.Add(1)
	cli.OnData(func(c *vsoa.Client, url string, payload vsoa.Payload, quick bool) {
		if url == "/py/reply" {
			wg.Done()
		}
	})
	if err := cli.Connect("vsoa://127.0.0.1:3015", "123456", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Datagram("/go/data", &vsoa.Payload{Param: map[string]any{"from": "go"}}, false); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestInteropPythonServerGoClientStream(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	root := "/home/ivan/work/vsoa-python"
	script := filepath.Join(root, "interop_server_stream.py")
	code := `import vsoa
server = vsoa.Server('py-stream', '123456')
@server.command('/get_data')
def get_data(cli, request, payload):
    def onlink(stream, conn):
        if conn:
            stream.send(b'python-stream-data')
    st = server.create_stream(onlink)
    cli.reply(request.seqno, tunid=st.tunid)
server.run('127.0.0.1', 3016)
`
	if err := os.WriteFile(script, []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(py, script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start python stream server: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(800 * time.Millisecond)
	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3016", "123456", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	h, _, err := cli.Call("/get_data", 0, nil, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil || h.TunID == 0 {
		t.Fatalf("bad header: %+v", h)
	}
	var got string
	var wg sync.WaitGroup
	wg.Add(1)
	conn, err := cli.CreateStream(h.TunID, func(conn net.Conn, up bool) {}, func(data []byte) {
		got = string(data)
		wg.Done()
	}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	wg.Wait()
	if got != "python-stream-data" {
		t.Fatalf("got %q", got)
	}
}
