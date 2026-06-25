package events

import "testing"

func TestOnce(t *testing.T) {
	e := New()
	count := 0
	e.Once("x", func(args ...any) { count++ })
	e.Emit("x")
	e.Emit("x")
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
}
