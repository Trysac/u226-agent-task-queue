# In-memory priority queue with leases and backoff

Status: accepted (generated, pending human review)

## Context

Agent runtimes need a low-latency, in-process work queue that supports priorities, leases to handle worker crashes, token-based acknowledgements to prevent stale completions, and bounded retries with backoff. The queue must be testable without real delays and safe for concurrent use.

## Decision

Implement the queue using sorted slices for ready and delayed tasks (ordered by priority/FIFO and available time respectively), a map for leased tasks, and a bounded slice for dead tasks. Use a mutex for thread safety. Time and token generation are injectable via Options for deterministic tests. Events are emitted outside the lock to avoid deadlock. Payloads are copied to prevent mutation.

## Consequences

The queue is suitable for a single process. It does not provide persistence or cross-process coordination. The sorted slice approach is O(n) for insertion but is adequate for the default max of 10,000 tasks. The token-based lease system ensures that only the current lease holder can acknowledge or reject a task.
