package workqueue

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAddNamedDedup(t *testing.T) {
	q := New()
	var n atomic.Int32
	if !q.AddNamed("job", func() { time.Sleep(20 * time.Millisecond); n.Add(1) }) {
		t.Fatal("first add should succeed")
	}
	if q.AddNamed("job", func() { n.Add(1) }) {
		t.Fatal("duplicate add should fail")
	}
	time.Sleep(80 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("count=%d", n.Load())
	}
}
