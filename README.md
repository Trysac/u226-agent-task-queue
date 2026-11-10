# agent-task-queue

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u226-agent-task-queue.svg)](https://pkg.go.dev/github.com/Trysac/u226-agent-task-queue)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u226-agent-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u226-agent-task-queue/actions/workflows/ci.yml)

In-memory priority task queue for agent workers in Go: leases with tokens, retries with capped exponential backoff, a bounded dead list and an injected clock

## Problem

Agent runtimes hand work to several workers: tool calls, sub-agent jobs and summarisation tasks. A slice guarded by a mutex loses tasks when a worker crashes mid-task, retries immediately in a hot loop and lets a stale worker acknowledge a task that was already handed to someone else. This package gives one process priorities, leases that expire, token-checked acknowledgements, capped exponential backoff and a bounded dead list, testable without sleeping.

## Install

```sh
go get github.com/Trysac/u226-agent-task-queue
```

Requires Go 1.23 or newer. No dependencies outside the standard library.

## Usage

```go
package main
import ("time"; agenttaskqueue "github.com/Trysac/u226-agent-task-queue")
func main() {
	q := agenttaskqueue.New(agenttaskqueue.Options{})
	_ = q.Enqueue(agenttaskqueue.Task{ID: "job1", Priority: 5})
	leased, ok := q.Lease("worker1", 30*time.Second)
	if ok { _ = q.Ack(leased.Task.ID, leased.Token) }
}
```

## Public API

Package `agenttaskqueue`, module `github.com/Trysac/u226-agent-task-queue`.

- `type Task struct { ID string; Kind string; Priority int; Payload []byte; MaxAttempts int }`
- `type Backoff struct { Base, Max time.Duration }`: delay for failed attempt `n` is `min(Base * 2^(n-1), Max)`; defaults are `1s` and `5m`.
- `type Options struct { Now func() time.Time; Backoff Backoff; MaxAttempts int; MaxTasks int; MaxDead int; MaxPayloadBytes int; OnEvent func(Event) }` with defaults `MaxAttempts = 5`, `MaxTasks = 10_000`, `MaxDead = 1_000`, `MaxPayloadBytes = 65_536`.
- `func New(opts Options) *Queue`
- `func (q *Queue) Enqueue(t Task) error`
- `func (q *Queue) Lease(workerID string, leaseFor time.Duration) (Leased, bool)`
- `func (q *Queue) Ack(id, token string) error`
- `func (q *Queue) Nack(id, token string, reason string) error`
- `func (q *Queue) Dead() []DeadTask`
- `func (q *Queue) Stats() Stats`
- `type Leased struct { Task Task; Token string; Attempt int; Deadline time.Time }`
- `type DeadTask` carries the task, its attempt count, the last reason truncated to 200 bytes on a rune boundary and `DeadAt`.
- `type Error struct { Code string; TaskID string }` with codes `invalid_task`, `duplicate`, `queue_full`, `payload_too_large`, `not_found`, `stale_lease`, `invalid_options`.
- `type Event struct { Type string; TaskID string; Attempt int }`; `OnEvent` is called for `enqueued`, `leased`, `acked`, `retried`, `expired` and `dead` outside the lock and never receives the payload.

Lease selection first moves expired leases back (each expiry counts as a failed attempt; it is re-queued without backoff or moved to dead when attempts are exhausted), then promotes delayed tasks whose available time is at or before `Now()`, then returns the highest `Priority`; ties use enqueue sequence (FIFO) and a re-queued task keeps its original sequence. Tokens are 16 random bytes hex-encoded. `Ack` and `Nack` return `not_found` for an unknown or terminal ID, and `stale_lease` for a token mismatch or a lease already expired at `Now()`. `Payload` is copied on `Enqueue` and on `Lease`. The dead list evicts the oldest entry at `MaxDead`.

## Errors and limits

- `invalid_task`: empty `ID` or `Priority` outside `0..9`.
- `duplicate`: the same ID is already ready, delayed or leased; after `Ack` the ID can be enqueued again.
- `queue_full`: `MaxTasks` is reached.
- `payload_too_large`: `Payload` is larger than `MaxPayloadBytes`.
- `not_found`: unknown ID, or a late `Ack`/`Nack` for a task already acked or dead.
- `stale_lease`: token mismatch, or the lease has expired at `Now()`.
- `invalid_options`: defined error code for invalid option values.
- `Lease` with `leaseFor <= 0` returns `ok == false` and emits no event; `Lease` never panics.
- A `Nack` on the last attempt moves the task straight to dead; attempts are never reset.

## Why not a channel or a Postgres queue

A Go channel has no priorities, leases, token-checked acknowledgements or retry backoff. A database-backed queue adds a dependency and a network round trip that are unnecessary for a queue inside one process.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Testing](docs/TESTING.md)
- [Security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md)
- [Stack](docs/STACK.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
