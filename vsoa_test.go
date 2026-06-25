package vsoa_test

import (
	"sync"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
)

func startTestServer(t *testing.T, passwd string) (*vsoa.Server, string) {
	t.Helper()
	s := vsoa.NewServer("test-server", passwd, false)
	s.Command("/echo", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	s.Command("/set", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &vsoa.Payload{Param: map[string]any{"ok": true}}, 0, 0)
	})
	s.Command("/status", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, nil, 2, 0) // VSOA_STATUS_ARGUMENTS
	})
	s.Command("/datagram", nil, func(cli *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		cli.Reply(req.SeqNo, &payload, 0, 0)
	})
	addr := "127.0.0.1:0"
	go func() {
		if err := s.Run(addr, nil); err != nil {
			// server closed
		}
	}()
	time.Sleep(50 * time.Millisecond)
	a := s.Address()
	if a == nil {
		t.Fatal("server address nil")
	}
	return s, "vsoa://" + a.String()
}

func TestConnectAndCall(t *testing.T) {
	s, url := startTestServer(t, "pass123")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "pass123", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	h, p, err := cli.Call("/echo", 0, &vsoa.Payload{Param: "hello"}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("nil header")
	}
	if p.Param != "hello" {
		t.Fatalf("param = %v", p.Param)
	}
}

func TestConnectWrongPassword(t *testing.T) {
	s, url := startTestServer(t, "secret")
	defer s.Close()

	cli := vsoa.NewClient(false)
	err := cli.Connect(url, "wrong", 2*time.Second, nil)
	if err == nil {
		cli.Close()
		t.Fatal("expected error for wrong password")
	}
}

func TestCallSetMethod(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	h, _, err := cli.Call("/set", 1, &vsoa.Payload{Param: map[string]any{"x": 1}}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil || h.Status != 0 {
		t.Fatalf("unexpected header: %+v", h)
	}
}

func TestCallInvalidURL(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	h, _, err := cli.Call("/nonexist", 0, nil, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h.Status != 3 { // VSOA_STATUS_INVALID_URL
		t.Fatalf("status = %d, want 3", h.Status)
	}
}

func TestSubscribePublish(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	var got vsoa.Payload
	var wg sync.WaitGroup
	wg.Add(1)
	cli.OnMessage(func(c *vsoa.Client, u string, p vsoa.Payload, quick bool) {
		got = p
		wg.Done()
	})
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Subscribe("/sensor/", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	s.Publish("/sensor/temp", &vsoa.Payload{Param: 42.0}, false)

	wg.Wait()
	if got.Param != 42.0 {
		t.Fatalf("param = %v", got.Param)
	}
}

func TestPing(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Ping(3 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestIsSubscribed(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Subscribe("/a/", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	if !s.IsSubscribed("/a/b") {
		t.Fatal("expected /a/b subscribed")
	}
	if s.IsSubscribed("/ab") {
		t.Fatal("/ab should not be subscribed")
	}
}

func TestServerClientList(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	time.Sleep(30 * time.Millisecond)

	clients := s.Clients()
	if len(clients) != 1 {
		t.Fatalf("clients = %d", len(clients))
	}
	if clients[0].ID() == 0 {
		t.Fatal("client id should not be 0")
	}
}

func TestFetchHelper(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()

	h, p, err := vsoa.Fetch(url+"/echo", "", 0, &vsoa.Payload{Param: "test"}, 3*time.Second, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("nil header")
	}
	if p.Param != "test" {
		t.Fatalf("param = %v", p.Param)
	}
}
