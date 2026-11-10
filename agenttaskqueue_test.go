package agenttaskqueue_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Trysac/u226-agent-task-queue"
)

// fakeClock is a deterministic clock for testing.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{t: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// deterministicRand is a deterministic io.Reader for token generation.
type deterministicRand struct {
	counter uint64
}

func (r *deterministicRand) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r.counter)
		r.counter++
	}
	return len(p), nil
}

// helper to create queue with deterministic clock and rand
func newTestQueue(opts agenttaskqueue.Options) (*agenttaskqueue.Queue, *fakeClock) {
	clk := newFakeClock(time.Unix(0, 0))
	opts.Now = clk.Now
	if opts.Rand == nil {
		opts.Rand = &deterministicRand{}
	}
	q := agenttaskqueue.New(opts)
	return q, clk
}

// helper to enqueue a simple task
func enqueue(t *testing.T, q *agenttaskqueue.Queue, id string, priority int, payload []byte, maxAttempts int) {
	t.Helper()
	err := q.Enqueue(agenttaskqueue.Task{
		ID:          id,
		Priority:    priority,
		Payload:     payload,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("Enqueue(%q) unexpected error: %v", id, err)
	}
}

func TestPriorityAndFIFO(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{})
	enqueue(t, q, "a", 1, nil, 0)
	enqueue(t, q, "b", 5, nil, 0)
	enqueue(t, q, "c", 5, nil, 0)

	leased, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease to succeed")
	}
	if leased.Task.ID != "b" {
		t.Fatalf("first lease = %q, want %q", leased.Task.ID, "b")
	}

	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected second lease to succeed")
	}
	if leased.Task.ID != "c" {
		t.Fatalf("second lease = %q, want %q", leased.Task.ID, "c")
	}

	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected third lease to succeed")
	}
	if leased.Task.ID != "a" {
		t.Fatalf("third lease = %q, want %q", leased.Task.ID, "a")
	}

	_, ok = q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected fourth lease to fail")
	}
}

func TestLeaseWithNonPositiveDurationReturnsFalse(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{})
	enqueue(t, q, "a", 0, nil, 0)

	// capture events
	events := []agenttaskqueue.Event{}
	q = agenttaskqueue.New(agenttaskqueue.Options{
		Now:     func() time.Time { return time.Unix(0, 0) },
		OnEvent: func(e agenttaskqueue.Event) { events = append(events, e) },
		Rand:    &deterministicRand{},
	})
	// re-enqueue because above q replaced
	// Actually above line replaced q, so enqueue again
	// We'll rebuild properly
	q = agenttaskqueue.New(agenttaskqueue.Options{
		Now:     func() time.Time { return time.Unix(0, 0) },
		OnEvent: func(e agenttaskqueue.Event) { events = append(events, e) },
		Rand:    &deterministicRand{},
	})
	err := q.Enqueue(agenttaskqueue.Task{ID: "a", Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	events = nil // reset after enqueue
	_, ok := q.Lease("w1", 0)
	if ok {
		t.Fatal("expected ok=false for leaseFor=0")
	}
	if len(events) != 0 {
		t.Fatalf("expected no events, got %v", events)
	}
	_, ok = q.Lease("w1", -1*time.Second)
	if ok {
		t.Fatal("expected ok=false for negative leaseFor")
	}
}

func TestBackoffAndMaxAttempts(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{
		MaxAttempts: 3,
		Backoff:     agenttaskqueue.Backoff{Base: time.Second, Max: 30 * time.Second},
	})
	enqueue(t, q, "task", 0, nil, 0)

	// First lease
	leased, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease")
	}
	if leased.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", leased.Attempt)
	}

	// Nack with token
	err := q.Nack("task", leased.Token, "fail")
	if err != nil {
		t.Fatalf("Nack unexpected error: %v", err)
	}

	// After first Nack, delay = 1s. Should not be leaseable at 999ms
	clk.Advance(999 * time.Millisecond)
	_, ok = q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected no lease before backoff delay")
	}

	// At exactly 1s, should be leaseable
	clk.Advance(1 * time.Millisecond)
	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease after backoff")
	}
	if leased.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", leased.Attempt)
	}

	// Second Nack -> delay = 2s
	err = q.Nack("task", leased.Token, "fail")
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(1999 * time.Millisecond)
	_, ok = q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected no lease before 2s")
	}
	clk.Advance(1 * time.Millisecond)
	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease after second backoff")
	}
	if leased.Attempt != 3 {
		t.Fatalf("attempt = %d, want 3", leased.Attempt)
	}

	// Third Nack -> should go dead
	err = q.Nack("task", leased.Token, "final")
	if err != nil {
		t.Fatal(err)
	}
	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatalf("dead len = %d, want 1", len(dead))
	}
	if dead[0].Attempts != 3 {
		t.Fatalf("dead attempts = %d, want 3", dead[0].Attempts)
	}
	// No more lease
	_, ok = q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected no lease after dead")
	}
}

func TestLeaseExpiry(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{
		MaxAttempts: 3,
	})
	enqueue(t, q, "a", 0, nil, 0)

	leased, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease")
	}
	oldToken := leased.Token

	// Advance past deadline
	clk.Advance(10 * time.Second)
	// Now lease again: should get task with attempt 2 and new token
	leased2, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected re-lease after expiry")
	}
	if leased2.Task.ID != "a" {
		t.Fatalf("id = %q, want a", leased2.Task.ID)
	}
	if leased2.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", leased2.Attempt)
	}
	if leased2.Token == oldToken {
		t.Fatal("token should be different after expiry")
	}

	// Ack with old token should return stale_lease
	err := q.Ack("a", oldToken)
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) {
		t.Fatalf("expected *Error, got %v", err)
	}
	if qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("code = %q, want stale_lease", qErr.Code)
	}
}

func TestDuplicateAndInvalid(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{})
	// duplicate
	enqueue(t, q, "a", 1, nil, 0)
	err := q.Enqueue(agenttaskqueue.Task{ID: "a", Priority: 1})
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeDuplicate {
		t.Fatalf("expected duplicate, got %v", err)
	}

	// after ack, can enqueue again
	leased, _ := q.Lease("w1", 10*time.Second)
	err = q.Ack("a", leased.Token)
	if err != nil {
		t.Fatal(err)
	}
	err = q.Enqueue(agenttaskqueue.Task{ID: "a", Priority: 1})
	if err != nil {
		t.Fatalf("enqueue after ack should succeed, got %v", err)
	}

	// invalid ID
	err = q.Enqueue(agenttaskqueue.Task{ID: "  "})
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeInvalidTask {
		t.Fatalf("expected invalid_task for blank ID, got %v", err)
	}
	err = q.Enqueue(agenttaskqueue.Task{ID: "b", Priority: -1})
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeInvalidTask {
		t.Fatalf("expected invalid_task for priority -1, got %v", err)
	}
	err = q.Enqueue(agenttaskqueue.Task{ID: "b", Priority: 10})
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeInvalidTask {
		t.Fatalf("expected invalid_task for priority 10, got %v", err)
	}
	err = q.Enqueue(agenttaskqueue.Task{ID: "b", MaxAttempts: -1})
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeInvalidTask {
		t.Fatalf("expected invalid_task for negative MaxAttempts, got %v", err)
	}
}

func TestQueueFull(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{MaxTasks: 2})
	enqueue(t, q, "a", 0, nil, 0)
	enqueue(t, q, "b", 0, nil, 0)
	err := q.Enqueue(agenttaskqueue.Task{ID: "c"})
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeQueueFull {
		t.Fatalf("expected queue_full, got %v", err)
	}
}

func TestPayloadSizeLimit(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{MaxPayloadBytes: 65536})
	payload := make([]byte, 65536)
	err := q.Enqueue(agenttaskqueue.Task{ID: "a", Payload: payload})
	if err != nil {
		t.Fatalf("enqueue with exactly limit should succeed, got %v", err)
	}
	payload = make([]byte, 65537)
	err = q.Enqueue(agenttaskqueue.Task{ID: "b", Payload: payload})
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodePayloadTooLarge {
		t.Fatalf("expected payload_too_large, got %v", err)
	}
}

func TestMaxDeadEviction(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{
		MaxAttempts: 1,
		MaxDead:     1,
	})
	// enqueue two tasks and immediately nack each (attempt exhausted)
	enqueue(t, q, "a", 0, nil, 0)
	leased, _ := q.Lease("w1", 10*time.Second)
	q.Nack("a", leased.Token, "fail")

	enqueue(t, q, "b", 0, nil, 0)
	leased, _ = q.Lease("w1", 10*time.Second)
	q.Nack("b", leased.Token, "fail")

	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatalf("dead len = %d, want 1", len(dead))
	}
	if dead[0].Task.ID != "b" {
		t.Fatalf("dead task = %q, want b", dead[0].Task.ID)
	}
	stats := q.Stats()
	if stats != (agenttaskqueue.Stats{Ready: 0, Leased: 0, Delayed: 0, Dead: 1}) {
		t.Fatalf("stats = %+v, want {0 0 0 1}", stats)
	}
}

func TestEvents(t *testing.T) {
	var events []agenttaskqueue.Event
	q, clk := newTestQueue(agenttaskqueue.Options{
		OnEvent:     func(e agenttaskqueue.Event) { events = append(events, e) },
		MaxAttempts: 2,
	})
	enqueue(t, q, "a", 0, []byte("secret"), 0)

	leased, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("lease failed")
	}
	q.Nack("a", leased.Token, "reason")
	clk.Advance(1 * time.Second) // backoff Base = 1s
	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("lease after backoff failed")
	}
	q.Ack("a", leased.Token)

	// Check event sequence
	expectedTypes := []string{"enqueued", "leased", "retried", "leased", "acked"}
	if len(events) != len(expectedTypes) {
		t.Fatalf("got %d events, want %d", len(events), len(expectedTypes))
	}
	for i, typ := range expectedTypes {
		if events[i].Type != typ {
			t.Fatalf("event %d type = %q, want %q", i, events[i].Type, typ)
		}
		if events[i].TaskID != "a" {
			t.Fatalf("event %d taskID = %q, want a", i, events[i].TaskID)
		}
		// attempt check
		switch i {
		case 0: // enqueued
			if events[i].Attempt != 0 {
				t.Fatalf("event 0 attempt = %d, want 0", events[i].Attempt)
			}
		case 1: // leased (first)
			if events[i].Attempt != 1 {
				t.Fatalf("event 1 attempt = %d, want 1", events[i].Attempt)
			}
		case 2: // retried
			if events[i].Attempt != 1 {
				t.Fatalf("event 2 attempt = %d, want 1", events[i].Attempt)
			}
		case 3: // leased (second)
			if events[i].Attempt != 2 {
				t.Fatalf("event 3 attempt = %d, want 2", events[i].Attempt)
			}
		case 4: // acked
			if events[i].Attempt != 2 {
				t.Fatalf("event 4 attempt = %d, want 2", events[i].Attempt)
			}
		}
	}
	// Ensure events never contain payload; Event struct has no payload field, so just no check.
}

func TestPayloadCopyOnEnqueueAndLease(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{})
	payload := []byte("original")
	err := q.Enqueue(agenttaskqueue.Task{ID: "a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	// Mutate original payload
	payload[0] = 'X'
	leased, _ := q.Lease("w1", 10*time.Second)
	if string(leased.Task.Payload) != "original" {
		t.Fatalf("leased payload = %q, want original", leased.Task.Payload)
	}
	// Mutate leased payload and ensure queue internal unchanged? Not directly testable, but we trust copy.
}

func TestAckAndNackErrors(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{})
	enqueue(t, q, "a", 0, nil, 0)

	// Unknown ID
	err := q.Ack("missing", "token")
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeNotFound {
		t.Fatalf("expected not_found for unknown Ack, got %v", err)
	}
	err = q.Nack("missing", "token", "")
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeNotFound {
		t.Fatalf("expected not_found for unknown Nack, got %v", err)
	}

	// Lease and test token mismatch
	leased, _ := q.Lease("w1", 10*time.Second)
	err = q.Ack("a", "wrongtoken")
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("expected stale_lease for wrong token Ack, got %v", err)
	}
	err = q.Nack("a", "wrongtoken", "")
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("expected stale_lease for wrong token Nack, got %v", err)
	}

	// Test expired lease returns stale_lease and processes expiry
	clk := newFakeClock(time.Unix(0, 0))
	q, clk = newTestQueue(agenttaskqueue.Options{Now: clk.Now, Rand: &deterministicRand{}})
	enqueue(t, q, "b", 0, nil, 0)
	leased, _ = q.Lease("w1", 10*time.Second)
	clk.Advance(10 * time.Second)
	err = q.Ack("b", leased.Token)
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("expected stale_lease for expired Ack, got %v", err)
	}
	// After expiry, task should be re-queued and can be leased again
	leased2, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected re-lease after expiry")
	}
	if leased2.Attempt != 2 {
		t.Fatalf("attempt after expiry = %d, want 2", leased2.Attempt)
	}

	// Nack expired similar; not necessary but add?
}

type failOnceRand struct {
	failed bool
	inner  deterministicRand
}

func (r *failOnceRand) Read(p []byte) (int, error) {
	if !r.failed {
		r.failed = true
		return 0, io.ErrUnexpectedEOF
	}
	return r.inner.Read(p)
}

func TestTokenFailureRestoresTask(t *testing.T) {
	r := &failOnceRand{}
	q, _ := newTestQueue(agenttaskqueue.Options{Rand: r})
	enqueue(t, q, "a", 0, nil, 0)

	if _, ok := q.Lease("w1", 10*time.Second); ok {
		t.Fatal("expected lease to fail when token generation fails")
	}
	if stats := q.Stats(); stats.Ready != 1 || stats.Leased != 0 || stats.Delayed != 0 {
		t.Fatalf("stats after token failure = %+v, want ready=1", stats)
	}
	leased, ok := q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected task to remain leaseable after token failure")
	}
	if leased.Task.ID != "a" || leased.Attempt != 1 {
		t.Fatalf("leased = %+v, want a at attempt 1", leased)
	}
}

func TestStaleLeaseAfterExpiryRequeue(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{MaxAttempts: 3})
	enqueue(t, q, "a", 5, nil, 0)
	leasedA, ok := q.Lease("w1", 10*time.Second)
	if !ok || leasedA.Task.ID != "a" {
		t.Fatalf("expected lease of a, got %+v ok=%v", leasedA, ok)
	}

	// Expire a, then trigger its re-queue with a Lease for a higher-priority task.
	clk.Advance(10 * time.Second)
	enqueue(t, q, "b", 9, nil, 0)
	leasedB, ok := q.Lease("w1", 10*time.Second)
	if !ok || leasedB.Task.ID != "b" {
		t.Fatalf("expected lease of b, got %+v ok=%v", leasedB, ok)
	}

	err := q.Ack("a", leasedA.Token)
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("Ack for re-queued task: expected stale_lease, got %v", err)
	}
	err = q.Nack("a", leasedA.Token, "")
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeStaleLease {
		t.Fatalf("Nack for re-queued task: expected stale_lease, got %v", err)
	}
}

func TestDeadTaskReturnsNotFound(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{MaxAttempts: 1})
	enqueue(t, q, "a", 0, nil, 0)
	leased, _ := q.Lease("w1", 10*time.Second)
	q.Nack("a", leased.Token, "fail")

	err := q.Ack("a", leased.Token)
	var qErr *agenttaskqueue.Error
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeNotFound {
		t.Fatalf("expected not_found for dead task Ack, got %v", err)
	}
	err = q.Nack("a", leased.Token, "")
	if !errors.As(err, &qErr) || qErr.Code != agenttaskqueue.CodeNotFound {
		t.Fatalf("expected not_found for dead task Nack, got %v", err)
	}
}

func TestReasonTruncation(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{MaxAttempts: 1})
	enqueue(t, q, "a", 0, nil, 0)
	leased, _ := q.Lease("w1", 10*time.Second)
	longReason := strings.Repeat("x", 300)
	q.Nack("a", leased.Token, longReason)
	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatal("expected dead task")
	}
	if len(dead[0].Reason) > 200 {
		t.Fatalf("reason length = %d, want <=200", len(dead[0].Reason))
	}
	// Ensure it's valid UTF-8 (truncated on rune boundary); we use ASCII so simple
	if !utf8.ValidString(dead[0].Reason) {
		t.Fatal("reason is not valid UTF-8")
	}
}

func TestConcurrentLeaseAck(t *testing.T) {
	const numTasks = 100
	const numWorkers = 4
	q, _ := newTestQueue(agenttaskqueue.Options{})
	for i := 0; i < numTasks; i++ {
		enqueue(t, q, fmt.Sprintf("task-%d", i), 0, nil, 0)
	}

	var wg sync.WaitGroup
	acks := make(chan string, numTasks)
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				leased, ok := q.Lease(workerID, 10*time.Second)
				if !ok {
					return
				}
				if err := q.Ack(leased.Task.ID, leased.Token); err != nil {
					t.Errorf("worker %s ack error: %v", workerID, err)
					return
				}
				acks <- leased.Task.ID
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	wg.Wait()
	close(acks)

	// Verify each task acked exactly once
	seen := make(map[string]int)
	for id := range acks {
		seen[id]++
	}
	if len(seen) != numTasks {
		t.Fatalf("acked %d unique tasks, want %d", len(seen), numTasks)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("task %s acked %d times, want 1", id, count)
		}
	}
}

// Additional tests for default options
func TestDefaultOptions(t *testing.T) {
	q := agenttaskqueue.New(agenttaskqueue.Options{})
	stats := q.Stats()
	if stats != (agenttaskqueue.Stats{}) {
		t.Fatalf("stats = %+v, want zero", stats)
	}
	// Enqueue with no MaxAttempts and no Now; should use time.Now internally; we can't easily test time, but ensure it doesn't panic.
	err := q.Enqueue(agenttaskqueue.Task{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	// Lease should work with real time; but to avoid flaky, we use small leaseFor and Ack quickly.
	leased, ok := q.Lease("w1", 100*time.Millisecond)
	if !ok {
		t.Fatal("lease failed")
	}
	if leased.Token == "" {
		t.Fatal("token empty")
	}
	if len(leased.Token) != 32 { // 16 bytes hex = 32 chars
		t.Fatalf("token length %d, want 32", len(leased.Token))
	}
	// Ack should succeed before deadline (but could expire if slow; use longer leaseFor already 100ms, should be fine)
	err = q.Ack("a", leased.Token)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBackoffOverflow(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{
		MaxAttempts: 70,
		Backoff:     agenttaskqueue.Backoff{Base: time.Second, Max: 30 * time.Second},
	})
	enqueue(t, q, "a", 0, nil, 0)

	for i := 1; i <= 56; i++ {
		leased, ok := q.Lease("w1", 10*time.Second)
		if !ok {
			t.Fatalf("expected lease at attempt %d", i)
		}
		if err := q.Nack("a", leased.Token, "fail"); err != nil {
			t.Fatalf("Nack at attempt %d: %v", i, err)
		}
		if i < 56 {
			delay := 30 * time.Second
			if i <= 5 {
				delay = time.Second << uint(i-1)
			}
			clk.Advance(delay)
		}
	}

	// After the 56th Nack the capped backoff must still hold the task for Max.
	if _, ok := q.Lease("w1", 10*time.Second); ok {
		t.Fatal("expected no lease: 56th backoff must not collapse to zero")
	}
	clk.Advance(30 * time.Second)
	if _, ok := q.Lease("w1", 10*time.Second); !ok {
		t.Fatal("expected lease after capped backoff")
	}
}

func TestErrorIs(t *testing.T) {
	err := &agenttaskqueue.Error{Code: agenttaskqueue.CodeDuplicate, TaskID: "x"}
	target := &agenttaskqueue.Error{Code: agenttaskqueue.CodeDuplicate}
	if !errors.Is(err, target) {
		t.Fatal("errors.Is should match by code")
	}
	target2 := &agenttaskqueue.Error{Code: agenttaskqueue.CodeInvalidTask}
	if errors.Is(err, target2) {
		t.Fatal("errors.Is should not match different code")
	}
}

func TestLeaseAfterAllDeadAndDelayed(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{MaxAttempts: 1})
	enqueue(t, q, "a", 0, nil, 0)
	leased, _ := q.Lease("w1", 10*time.Second)
	q.Nack("a", leased.Token, "fail")
	// a is dead, no ready, no delayed
	_, ok := q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected no lease")
	}
	// enqueue b, lease, nack with longer MaxAttempts to create delayed
	q.Enqueue(agenttaskqueue.Task{ID: "b", MaxAttempts: 2})
	leased, _ = q.Lease("w1", 10*time.Second)
	q.Nack("b", leased.Token, "fail")
	// b is delayed; now no ready, so lease false
	_, ok = q.Lease("w1", 10*time.Second)
	if ok {
		t.Fatal("expected no lease when only delayed")
	}
	// advance past backoff and lease
	clk.Advance(1 * time.Second) // base 1s
	leased, ok = q.Lease("w1", 10*time.Second)
	if !ok {
		t.Fatal("expected lease after delayed time")
	}
	if leased.Task.ID != "b" {
		t.Fatalf("leased id = %q, want b", leased.Task.ID)
	}
}

func TestPayloadCopyOnDead(t *testing.T) {
	q, _ := newTestQueue(agenttaskqueue.Options{MaxAttempts: 1})
	payload := []byte("secret")
	err := q.Enqueue(agenttaskqueue.Task{ID: "a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	leased, _ := q.Lease("w1", 10*time.Second)
	q.Nack("a", leased.Token, "fail")
	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatal("expected dead")
	}
	if string(dead[0].Task.Payload) != "secret" {
		t.Fatalf("dead payload = %q, want secret", dead[0].Task.Payload)
	}
	// Mutate original payload and ensure dead copy unchanged
	payload[0] = 'X'
	dead2 := q.Dead()
	if string(dead2[0].Task.Payload) != "secret" {
		t.Fatalf("dead payload after mutate = %q, want secret", dead2[0].Task.Payload)
	}
}

func TestStatsAfterVariousStates(t *testing.T) {
	q, clk := newTestQueue(agenttaskqueue.Options{MaxAttempts: 2})
	// enqueue two tasks
	enqueue(t, q, "a", 0, nil, 0)
	enqueue(t, q, "b", 0, nil, 0)
	// lease one -> ready=1, leased=1
	leased, _ := q.Lease("w1", 10*time.Second)
	stats := q.Stats()
	if stats.Ready != 1 || stats.Leased != 1 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("stats = %+v, want ready=1 leased=1", stats)
	}
	// nack the leased one -> delayed=1, leased=0
	q.Nack("a", leased.Token, "fail")
	stats = q.Stats()
	if stats.Ready != 1 || stats.Leased != 0 || stats.Delayed != 1 || stats.Dead != 0 {
		t.Fatalf("stats = %+v, want ready=1 delayed=1", stats)
	}
	// advance past backoff and lease the delayed one
	clk.Advance(1 * time.Second)
	leased, _ = q.Lease("w1", 10*time.Second)
	stats = q.Stats()
	if stats.Ready != 1 || stats.Leased != 1 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("stats = %+v, want ready=1 leased=1", stats)
	}
	// ack both? Actually we have b still ready, a leased; but a already has failed attempt, so after this lease a is attempt 2; ack completes.
	q.Ack("a", leased.Token)
	stats = q.Stats()
	if stats.Ready != 1 || stats.Leased != 0 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("stats = %+v, want ready=1", stats)
	}
	// ack b
	leased, _ = q.Lease("w1", 10*time.Second)
	q.Ack("b", leased.Token)
	stats = q.Stats()
	if stats.Ready != 0 || stats.Leased != 0 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("stats = %+v, want all zero", stats)
	}
}
