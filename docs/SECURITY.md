# Security notes

## Threat model

`github.com/Trysac/u226-agent-task-queue` is an in-memory Go library without persistence or network or filesystem I/O. Token generation uses a random source; tokens are 16 random bytes hex-encoded.

Defended:

- Invalid input: empty `ID` and `Priority` outside `0..9` are rejected with `invalid_task`.
- Duplicate IDs while the task is ready, delayed or leased are rejected with `duplicate`; after `Ack` the ID can be enqueued again.
- Queue capacity is bounded by `MaxTasks` (default `10_000`) and `queue_full` is returned when it is reached.
- Payloads larger than `MaxPayloadBytes` (default `65_536`) are rejected with `payload_too_large`.
- Stale workers cannot ack or nack with an old token: token mismatch or a lease already expired at `Now()` returns `stale_lease`; unknown or terminal IDs return `not_found`.
- Lease expiry counts as a failed attempt and requeues the task without backoff, or moves it to dead when attempts are exhausted.
- `Payload` is copied on `Enqueue` and on `Lease`, so later caller mutation does not change queued state.
- The dead list is bounded by `MaxDead` (default `1_000`) and evicts the oldest entry; dead reasons are truncated to 200 bytes on a rune boundary.
- `OnEvent` receives `Type`, `TaskID` and `Attempt`, never the payload, and runs outside the lock. The library performs no logging.

Not defended:

- Same-process callers can call the public API directly; there is no authentication or authorization of `workerID`.
- No encryption: payloads and tokens are held in memory.
- No persistence or crash recovery; process exit loses all queue state.
- No cross-process coordination; `MaxTasks` and `MaxDead` apply to one `Queue`.
- No protection against memory use beyond the configured caps or against callers retaining payloads.
- No wall-clock scheduling, task dependencies or fairness across kinds.
- Lease expiry and backoff depend on the injected `Options.Now`; an incorrect clock makes those decisions incorrect.

## Publishing

Releases are git tags served through the Go module proxy and checksum database. No registry token is stored in the repository.

To report a vulnerability, use the repository's security reporting process.
