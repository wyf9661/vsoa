package events

import "sync"

type Listener func(args ...any)

type EventEmitter struct {
	mu     sync.RWMutex
	events map[string][]entry
}

type entry struct {
	fn   Listener
	once bool
}

func New() *EventEmitter {
	return &EventEmitter{events: map[string][]entry{}}
}

func (e *EventEmitter) On(event string, fn Listener) {
	e.add(event, fn, false)
}

func (e *EventEmitter) Once(event string, fn Listener) {
	e.add(event, fn, true)
}

func (e *EventEmitter) add(event string, fn Listener, once bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events[event] = append(e.events[event], entry{fn: fn, once: once})
}

func (e *EventEmitter) RemoveListener(event string, fn Listener) {
	e.mu.Lock()
	defer e.mu.Unlock()
	items := e.events[event]
	out := items[:0]
	for _, item := range items {
		if &item.fn != &fn {
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		delete(e.events, event)
	} else {
		e.events[event] = out
	}
}

func (e *EventEmitter) Emit(event string, args ...any) bool {
	e.mu.Lock()
	items := append([]entry(nil), e.events[event]...)
	keep := make([]entry, 0, len(items))
	for _, item := range items {
		if !item.once {
			keep = append(keep, item)
		}
	}
	if len(items) == 0 {
		e.mu.Unlock()
		return false
	}
	if len(keep) == 0 {
		delete(e.events, event)
	} else {
		e.events[event] = keep
	}
	e.mu.Unlock()
	for _, item := range items {
		item.fn(args...)
	}
	return true
}
