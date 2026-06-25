package workqueue

import "sync"

type Job func()

type Queue struct {
	mu      sync.Mutex
	jobs    []namedJob
	notify  chan struct{}
	closed  bool
	running map[string]bool
}

type namedJob struct {
	key string
	fn  Job
}

func New() *Queue {
	q := &Queue{
		notify:  make(chan struct{}, 1),
		running: make(map[string]bool),
	}
	go q.loop()
	return q
}

func (q *Queue) loop() {
	for range q.notify {
		for {
			q.mu.Lock()
			if len(q.jobs) == 0 {
				q.mu.Unlock()
				break
			}
			job := q.jobs[0]
			q.jobs = q.jobs[1:]
			delete(q.running, job.key)
			q.mu.Unlock()
			func() {
				defer func() { _ = recover() }()
				job.fn()
			}()
		}
	}
}

func (q *Queue) Add(fn Job) {
	q.AddNamed("", fn)
}

func (q *Queue) AddNamed(key string, fn Job) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if key != "" && q.running[key] {
		return false
	}
	q.jobs = append(q.jobs, namedJob{key: key, fn: fn})
	if key != "" {
		q.running[key] = true
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

func (q *Queue) Delete(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, job := range q.jobs {
		if job.key == key {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			delete(q.running, key)
			return true
		}
	}
	return false
}

func (q *Queue) IsQueued(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.running[key]
}
