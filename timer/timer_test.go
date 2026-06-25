package timer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestOneShotTimer(t *testing.T) {
	tm := New()
	var hit atomic.Int32
	tm.Start(20*time.Millisecond, func() { hit.Add(1) }, 0)
	time.Sleep(60 * time.Millisecond)
	if hit.Load() != 1 {
		t.Fatalf("hit=%d", hit.Load())
	}
}
