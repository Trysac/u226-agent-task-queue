package agenttaskqueue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Error codes returned by the queue. They are stable and can be used with errors.Is.
const (
	CodeInvalidTask     = "invalid_task"
	CodeDuplicate       = "duplicate"
	CodeQueueFull       = "queue_full"
	CodePayloadTooLarge = "payload_too_large"
	CodeNotFound        = "not_found"
	CodeStaleLease      = "stale_lease"
	CodeInvalidOptions  = "invalid_options"
)

// Task represents a unit of work to be processed.
//
// ID must be non-empty after trimming whitespace. Priority must be between 0 and 9 inclusive.
// Payload is copied on Enqueue; mutating the original slice after Enqueue does not affect the queue.
// MaxAttempts, if > 0, overrides the queue's default MaxAttempts for this task.
type Task struct {
	ID          string
	Kind        string
	Priority    int
	Payload     []byte
	MaxAttempts int
}

// Backoff defines the exponential backoff used after a Nack.
// The delay for failed attempt n is min(Base * 2^(n-1), Max).
// Defaults (when zero) are Base = 1s, Max = 5m.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Options configures a Queue. All fields are optional; sensible defaults are used when zero.
// Any field with a negative or zero value (except OnEvent and Rand) is replaced by the default.
// Now is the clock function; default is time.Now.
// Rand is the source of random bytes for tokens; default is crypto/rand.Reader.
// OnEvent, if non-nil, is called for each queue event after the internal lock is released.
type Options struct {
	Now             func() time.Time
	Backoff         Backoff
	MaxAttempts     int // default 5
	MaxTasks        int // default 10_000
	MaxDead         int // default 1_000
	MaxPayloadBytes int // default 65_536
	OnEvent         func(Event)
	Rand            io.Reader // for token generation
}

// Event represents a significant occurrence in the queue. Type is one of:
// "enqueued", "leased", "acked", "retried", "expired", "dead".
// The Payload is never included in an event.
type Event struct {
	Type    string
	TaskID  string
	Attempt int
}

// Leased is returned by Lease and contains the leased task along with the unique token needed for Ack/Nack.
type Leased struct {
	Task     Task
	Token    string
	Attempt  int
	Deadline time.Time
}

// DeadTask represents a task that exhausted its attempts.
type DeadTask struct {
	Task     Task
	Attempts int
	Reason   string
	DeadAt   time.Time
}

// Stats provides current queue counts. It does not process pending expirations or delayed tasks.
type Stats struct {
	Ready   int
	Leased  int
	Delayed int
	Dead    int
}

// Error is returned by queue methods. It includes a stable machine-readable Code and the TaskID concerned.
type Error struct {
	Code   string
	TaskID string
}

func (e *Error) Error() string {
	if e.TaskID == "" {
		return fmt.Sprintf("agenttaskqueue: %s", e.Code)
	}
	return fmt.Sprintf("agenttaskqueue: %s (task %q)", e.Code, e.TaskID)
}

// Is allows errors.Is to compare only the Code field.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// taskState holds the internal state of a task.
type taskState struct {
	task        Task   // copy of the task with Payload copied
	maxAttempts int    // effective max attempts
	seq         uint64 // monotonically increasing sequence for FIFO tie-breaking
	attempts    int    // number of failed attempts so far (0 initially)

	// state-specific fields
	state     string    // "ready", "delayed", "leased"
	available time.Time // for delayed
	deadline  time.Time // for leased
	token     string    // for leased
	workerID  string    // for leased (not used in logic)
	reason    string    // last Nack reason, truncated
}

// Queue is an in-memory priority task queue with leases, retries, and a bounded dead list.
// It is safe for concurrent use by multiple goroutines. The zero value is not usable; use New.
type Queue struct {
	opts Options

	mu sync.Mutex

	// tasks holds all active tasks (ready, delayed, leased).
	tasks map[string]*taskState

	ready   []*taskState // sorted by priority desc, seq asc
	delayed []*taskState // sorted by available asc, seq asc
	leased  map[string]*taskState

	deadList []DeadTask // oldest at index 0, newest at end

	nextSeq uint64
	rand    io.Reader
}

// New creates a Queue with the given options. Defaults are applied for zero values.
func New(opts Options) *Queue {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Backoff.Base <= 0 {
		opts.Backoff.Base = time.Second
	}
	if opts.Backoff.Max <= 0 {
		opts.Backoff.Max = 5 * time.Minute
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.MaxTasks <= 0 {
		opts.MaxTasks = 10_000
	}
	if opts.MaxDead <= 0 {
		opts.MaxDead = 1_000
	}
	if opts.MaxPayloadBytes <= 0 {
		opts.MaxPayloadBytes = 65_536
	}
	if opts.Rand == nil {
		opts.Rand = rand.Reader
	}

	return &Queue{
		opts:     opts,
		tasks:    make(map[string]*taskState),
		ready:    make([]*taskState, 0),
		delayed:  make([]*taskState, 0),
		leased:   make(map[string]*taskState),
		deadList: make([]DeadTask, 0),
		rand:     opts.Rand,
	}
}

// Enqueue adds a task to the queue. It returns an *Error with the appropriate Code on failure.
func (q *Queue) Enqueue(t Task) error {
	q.mu.Lock()
	var events []Event
	err := q.enqueueLocked(t, &events)
	q.mu.Unlock()
	q.emitEvents(events)
	return err
}

func (q *Queue) enqueueLocked(t Task, events *[]Event) error {
	// Validate ID (non-empty after trim)
	if strings.TrimSpace(t.ID) == "" {
		return &Error{Code: CodeInvalidTask, TaskID: t.ID}
	}
	if t.Priority < 0 || t.Priority > 9 {
		return &Error{Code: CodeInvalidTask, TaskID: t.ID}
	}
	if t.MaxAttempts < 0 {
		return &Error{Code: CodeInvalidTask, TaskID: t.ID}
	}
	if len(t.Payload) > q.opts.MaxPayloadBytes {
		return &Error{Code: CodePayloadTooLarge, TaskID: t.ID}
	}

	// Check duplicate
	if _, exists := q.tasks[t.ID]; exists {
		return &Error{Code: CodeDuplicate, TaskID: t.ID}
	}

	// Check capacity
	active := len(q.tasks)
	if active >= q.opts.MaxTasks {
		return &Error{Code: CodeQueueFull, TaskID: t.ID}
	}

	// Determine effective max attempts
	maxAttempts := q.opts.MaxAttempts
	if t.MaxAttempts > 0 {
		maxAttempts = t.MaxAttempts
	}

	// Copy payload
	payloadCopy := make([]byte, len(t.Payload))
	copy(payloadCopy, t.Payload)
	taskCopy := Task{
		ID:          t.ID,
		Kind:        t.Kind,
		Priority:    t.Priority,
		Payload:     payloadCopy,
		MaxAttempts: t.MaxAttempts, // keep original; but effective is stored in state
	}

	q.nextSeq++
	ts := &taskState{
		task:        taskCopy,
		maxAttempts: maxAttempts,
		seq:         q.nextSeq,
		attempts:    0,
		state:       "ready",
	}
	q.tasks[t.ID] = ts
	q.insertReady(ts)

	*events = append(*events, Event{Type: "enqueued", TaskID: t.ID, Attempt: 0})
	return nil
}

// Lease attempts to lease the highest-priority ready task. If no task is available or leaseFor <= 0, it returns ok == false.
// The returned token must be used for Ack/Nack. The Payload in the returned Leased.Task is a copy.
func (q *Queue) Lease(workerID string, leaseFor time.Duration) (Leased, bool) {
	q.mu.Lock()
	var events []Event
	leased, ok := q.leaseLocked(workerID, leaseFor, &events)
	q.mu.Unlock()
	q.emitEvents(events)
	return leased, ok
}

func (q *Queue) leaseLocked(workerID string, leaseFor time.Duration, events *[]Event) (Leased, bool) {
	if leaseFor <= 0 {
		return Leased{}, false
	}
	now := q.opts.Now()

	// Process expired leases
	for id, ts := range q.leased {
		if !ts.deadline.After(now) { // deadline <= now
			q.handleExpired(id, ts, now, events)
		}
	}

	// Process delayed tasks whose available time has arrived
	for len(q.delayed) > 0 {
		ts := q.delayed[0]
		if ts.available.After(now) {
			break
		}
		// Remove from delayed and insert into ready
		q.delayed = q.delayed[1:]
		if _, stillActive := q.tasks[ts.task.ID]; stillActive && ts.state == "delayed" {
			q.moveToReady(ts, events)
		}
	}

	if len(q.ready) == 0 {
		return Leased{}, false
	}

	// Pop highest priority, then FIFO
	ts := q.ready[0]
	q.ready = q.ready[1:]

	// Generate token
	token, err := q.generateToken()
	if err != nil {
		// Token generation failed: restore the popped task so it stays leaseable.
		q.insertReady(ts)
		return Leased{}, false
	}

	attempt := ts.attempts + 1
	deadline := now.Add(leaseFor)

	ts.state = "leased"
	ts.deadline = deadline
	ts.token = token
	ts.workerID = workerID
	q.leased[ts.task.ID] = ts

	// Copy payload for return
	payloadCopy := make([]byte, len(ts.task.Payload))
	copy(payloadCopy, ts.task.Payload)
	leasedTask := Task{
		ID:          ts.task.ID,
		Kind:        ts.task.Kind,
		Priority:    ts.task.Priority,
		Payload:     payloadCopy,
		MaxAttempts: ts.task.MaxAttempts,
	}

	*events = append(*events, Event{Type: "leased", TaskID: ts.task.ID, Attempt: attempt})
	return Leased{
		Task:     leasedTask,
		Token:    token,
		Attempt:  attempt,
		Deadline: deadline,
	}, true
}

// Ack acknowledges successful completion of a leased task. The token must match the one returned by Lease.
// After a successful Ack, the task is forgotten and its ID can be enqueued again.
func (q *Queue) Ack(id, token string) error {
	q.mu.Lock()
	var events []Event
	err := q.ackLocked(id, token, &events)
	q.mu.Unlock()
	q.emitEvents(events)
	return err
}

func (q *Queue) ackLocked(id, token string, events *[]Event) error {
	ts, ok := q.tasks[id]
	if !ok {
		return &Error{Code: CodeNotFound, TaskID: id}
	}
	if ts.state != "leased" {
		// Existing but re-queued (ready/delayed): the lease the caller holds is stale.
		return &Error{Code: CodeStaleLease, TaskID: id}
	}
	if ts.token != token {
		return &Error{Code: CodeStaleLease, TaskID: id}
	}
	now := q.opts.Now()
	if !ts.deadline.After(now) {
		// Lease expired; process expiry and return stale_lease
		q.handleExpired(id, ts, now, events)
		return &Error{Code: CodeStaleLease, TaskID: id}
	}

	// Successful ack
	delete(q.leased, id)
	delete(q.tasks, id)
	*events = append(*events, Event{Type: "acked", TaskID: id, Attempt: ts.attempts + 1})
	return nil
}

// Nack reports failure of a leased task. The token must match. The task will be retried after backoff or moved to dead if attempts exhausted.
func (q *Queue) Nack(id, token, reason string) error {
	q.mu.Lock()
	var events []Event
	err := q.nackLocked(id, token, reason, &events)
	q.mu.Unlock()
	q.emitEvents(events)
	return err
}

func (q *Queue) nackLocked(id, token, reason string, events *[]Event) error {
	ts, ok := q.tasks[id]
	if !ok {
		return &Error{Code: CodeNotFound, TaskID: id}
	}
	if ts.state != "leased" {
		// Existing but re-queued (ready/delayed): the lease the caller holds is stale.
		return &Error{Code: CodeStaleLease, TaskID: id}
	}
	if ts.token != token {
		return &Error{Code: CodeStaleLease, TaskID: id}
	}
	now := q.opts.Now()
	if !ts.deadline.After(now) {
		q.handleExpired(id, ts, now, events)
		return &Error{Code: CodeStaleLease, TaskID: id}
	}

	// Remove from leased and increment attempts
	delete(q.leased, id)
	ts.attempts++
	if ts.attempts >= ts.maxAttempts {
		// Dead
		q.moveToDead(ts, reason, now, events)
	} else {
		// Schedule retry with backoff
		delay := q.backoffDelay(ts.attempts)
		ts.state = "delayed"
		ts.available = now.Add(delay)
		ts.reason = truncateReason(reason)
		q.insertDelayed(ts)
		*events = append(*events, Event{Type: "retried", TaskID: ts.task.ID, Attempt: ts.attempts})
	}
	return nil
}

// Dead returns a copy of the dead tasks list. The slice is ordered from oldest to newest.
func (q *Queue) Dead() []DeadTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]DeadTask, len(q.deadList))
	copy(result, q.deadList)
	// Also copy payloads to avoid mutation
	for i := range result {
		result[i].Task.Payload = make([]byte, len(result[i].Task.Payload))
		copy(result[i].Task.Payload, q.deadList[i].Task.Payload)
	}
	return result
}

// Stats returns current counts of ready, leased, delayed, and dead tasks.
// It does not process pending expirations or delayed tasks; those are handled on the next Lease.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{
		Ready:   len(q.ready),
		Leased:  len(q.leased),
		Delayed: len(q.delayed),
		Dead:    len(q.deadList),
	}
}

// handleExpired processes a lease that has expired. It increments attempts and either requeues the task (ready) or moves it to dead.
// It emits the appropriate event.
func (q *Queue) handleExpired(id string, ts *taskState, now time.Time, events *[]Event) {
	delete(q.leased, id)
	ts.attempts++
	*events = append(*events, Event{Type: "expired", TaskID: id, Attempt: ts.attempts})
	if ts.attempts >= ts.maxAttempts {
		q.moveToDead(ts, "lease expired", now, events)
	} else {
		q.moveToReady(ts, events)
	}
}

// moveToReady moves a taskState back to the ready list. The event is emitted by the caller.
func (q *Queue) moveToReady(ts *taskState, events *[]Event) {
	ts.state = "ready"
	q.insertReady(ts)
	// The event is emitted by the caller; here we just change state.
}

// moveToDead moves a task to the dead list, removes it from active maps, and emits a "dead" event.
func (q *Queue) moveToDead(ts *taskState, reason string, now time.Time, events *[]Event) {
	delete(q.tasks, ts.task.ID)
	// Remove from any other list/map just in case
	delete(q.leased, ts.task.ID)
	// If it was in ready or delayed, those slices need cleanup (should not happen if state is leased, but be safe)
	// For now, assume state is leased when this is called.

	deadTask := DeadTask{
		Task: Task{
			ID:          ts.task.ID,
			Kind:        ts.task.Kind,
			Priority:    ts.task.Priority,
			Payload:     append([]byte(nil), ts.task.Payload...),
			MaxAttempts: ts.task.MaxAttempts,
		},
		Attempts: ts.attempts,
		Reason:   truncateReason(reason),
		DeadAt:   now,
	}
	q.deadList = append(q.deadList, deadTask)
	if len(q.deadList) > q.opts.MaxDead {
		// Evict oldest (index 0) and shift
		copy(q.deadList, q.deadList[1:])
		q.deadList = q.deadList[:len(q.deadList)-1]
	}
	*events = append(*events, Event{Type: "dead", TaskID: ts.task.ID, Attempt: ts.attempts})
}

// insertReady inserts ts into the ready slice maintaining order: higher priority first, then lower seq.
func (q *Queue) insertReady(ts *taskState) {
	i := sort.Search(len(q.ready), func(i int) bool {
		other := q.ready[i]
		if ts.task.Priority != other.task.Priority {
			return ts.task.Priority > other.task.Priority // we want higher priority to come before lower
		}
		return ts.seq < other.seq // lower seq first
	})
	q.ready = append(q.ready, nil)
	copy(q.ready[i+1:], q.ready[i:])
	q.ready[i] = ts
}

// insertDelayed inserts ts into the delayed slice maintaining order by available time ascending, tie by seq ascending.
func (q *Queue) insertDelayed(ts *taskState) {
	i := sort.Search(len(q.delayed), func(i int) bool {
		other := q.delayed[i]
		if ts.available != other.available {
			return ts.available.Before(other.available)
		}
		return ts.seq < other.seq
	})
	q.delayed = append(q.delayed, nil)
	copy(q.delayed[i+1:], q.delayed[i:])
	q.delayed[i] = ts
}

// generateToken returns 16 random bytes hex-encoded.
func (q *Queue) generateToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(q.rand, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// backoffDelay computes the delay for the nth failure (1-indexed).
func (q *Queue) backoffDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	base := q.opts.Backoff.Base
	max := q.opts.Backoff.Max
	// delay = min(Base * 2^(n-1), Max). Clamp before multiplying to avoid overflow.
	shift := attempt - 1
	if shift >= 63 || base > max>>uint(shift) {
		return max
	}
	return base << uint(shift)
}

// truncateReason truncates a string to at most maxBytes bytes on a rune boundary.
func truncateReason(reason string) string {
	const maxBytes = 200
	if len(reason) <= maxBytes {
		return reason
	}
	// Take runes until adding one would exceed maxBytes
	var b strings.Builder
	size := 0
	for _, r := range reason {
		runeLen := utf8.RuneLen(r)
		if size+runeLen > maxBytes {
			break
		}
		b.WriteRune(r)
		size += runeLen
	}
	return b.String()
}

// emitEvents calls opts.OnEvent for each event in order, outside the lock.
func (q *Queue) emitEvents(events []Event) {
	if q.opts.OnEvent == nil {
		return
	}
	for _, ev := range events {
		q.opts.OnEvent(ev)
	}
}

// Ensure errors.Is works with sentinel errors? We don't have sentinel errors, but user can create an Error with Code to compare.
// Provide a helper to create an Error for comparison? Not required.
