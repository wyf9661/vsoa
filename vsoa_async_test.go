package vsoa_test

import (
	"sync"
	"testing"
	"time"

	"github.com/wyf9661/vsoa"
)

func TestAsyncCallCallback(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()
	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	var got string
	if err := cli.CallAsync("/echo", 0, &vsoa.Payload{Param: "async"}, 3*time.Second, func(h *vsoa.Header, p *vsoa.Payload, err error) {
		if err == nil && p != nil {
			got, _ = p.Param.(string)
		}
		wg.Done()
	}); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got != "async" {
		t.Fatalf("got = %q", got)
	}
}

func TestPendingCount(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()
	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if cli.Pendings() != 0 {
		t.Fatalf("pending = %d", cli.Pendings())
	}
	start := make(chan struct{})
	s.Command("/slow", nil, func(rc *vsoa.RemoteClient, req vsoa.Request, payload vsoa.Payload) {
		<-start
		rc.Reply(req.SeqNo, &vsoa.Payload{Param: "ok"}, 0, 0)
	})
	err := cli.CallAsync("/slow", 0, nil, 3*time.Second, func(h *vsoa.Header, p *vsoa.Payload, err error) {})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if cli.Pendings() == 0 {
		t.Fatal("expected pending > 0")
	}
	close(start)
	time.Sleep(50 * time.Millisecond)
	if cli.Pendings() != 0 {
		t.Fatalf("pending = %d", cli.Pendings())
	}
}

func TestSubscribeHooksAndDatagram(t *testing.T) {
	s, url := startTestServer(t, "")
	defer s.Close()
	cli := vsoa.NewClient(false)
	if err := cli.Connect(url, "", 3*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	connected := s.Clients()[0]
	var subWG sync.WaitGroup
	var unsubWG sync.WaitGroup
	subWG.Add(1)
	unsubWG.Add(1)
	connected.OnSubscribe(func(c *vsoa.RemoteClient, topics []string) {
		if len(topics) == 1 && topics[0] == "/topic1" {
			subWG.Done()
		}
	})
	connected.OnUnsubscribe(func(c *vsoa.RemoteClient, topics []string) {
		if len(topics) == 1 && topics[0] == "/topic1" {
			unsubWG.Done()
		}
	})
	if err := cli.Subscribe("/topic1", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	subWG.Wait()
	if err := cli.Unsubscribe("/topic1", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	unsubWG.Wait()

	var dataWG sync.WaitGroup
	dataWG.Add(1)
	cli.OnData(func(c *vsoa.Client, url string, p vsoa.Payload, quick bool) {
		if url == "/svr/data" {
			dataWG.Done()
		}
	})
	if !connected.Datagram("/svr/data", &vsoa.Payload{Param: map[string]any{"k": "v"}}, false) {
		t.Fatal("server datagram failed")
	}
	dataWG.Wait()
}
