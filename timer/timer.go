package timer

import (
	"sync"
	"time"
)

type Timer struct {
	mu      sync.Mutex
	timer   *time.Timer
	ticker  *time.Ticker
	started bool
	stop    chan struct{}
}

func New() *Timer {
	return &Timer{}
}

func (t *Timer) Start(timeout time.Duration, callback func(), interval time.Duration) {
	t.Stop()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.started = true
	t.stop = make(chan struct{})
	if interval <= 0 {
		t.timer = time.AfterFunc(timeout, func() {
			callback()
			t.mu.Lock()
			t.started = false
			t.mu.Unlock()
		})
		return
	}
	go func(stop <-chan struct{}) {
		select {
		case <-time.After(timeout):
			callback()
		case <-stop:
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				callback()
			case <-stop:
				return
			}
		}
	}(t.stop)
}

func (t *Timer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	if t.ticker != nil {
		t.ticker.Stop()
		t.ticker = nil
	}
	if t.stop != nil {
		close(t.stop)
		t.stop = nil
	}
	t.started = false
}

func (t *Timer) Started() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.started
}
