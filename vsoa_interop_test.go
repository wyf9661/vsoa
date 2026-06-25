package vsoa_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
)

func requirePythonExampleEnv(t *testing.T) string {
	t.Helper()
	root := "/home/ivan/work/vsoa-python"
	venvPython := filepath.Join(root, ".venv", "bin", "python")
	if _, err := os.Stat(venvPython); err != nil {
		t.Skip("python venv for vsoa-python not available")
	}
	return venvPython
}

func TestInteropGoServerPythonClientFetch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	s, url := startTestServer(t, "123456")
	defer s.Close()

	hostport := strings.TrimPrefix(url, "vsoa://")
	cmd := exec.Command(py, "-c", `import vsoa,sys; h,p,e=vsoa.fetch('vsoa://`+hostport+`/echo', passwd='123456', payload={'param':{'hello':'world'}}); print('OK' if h and p.param.get('hello')=='world' else 'FAIL'); sys.exit(0 if h and p.param.get('hello')=='world' else 1)`)
	cmd.Dir = "/home/ivan/work/vsoa-python"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python client fetch failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "OK") {
		t.Fatalf("unexpected python output: %s", string(out))
	}
}

func TestInteropPythonServerGoClientCall(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("interop test only runs on linux")
	}
	py := requirePythonExampleEnv(t)
	root := "/home/ivan/work/vsoa-python"
	script := filepath.Join(root, "example", "server.py")
	cmd := exec.Command(py, script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start python server: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	time.Sleep(800 * time.Millisecond)

	cli := vsoa.NewClient(false)
	if err := cli.Connect("vsoa://127.0.0.1:3005", "123456", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: map[string]any{"go": "client"}}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("nil header")
	}
	m, ok := p.Param.(map[string]any)
	if !ok || m["go"] != "client" {
		t.Fatalf("unexpected payload: %#v", p.Param)
	}
}
