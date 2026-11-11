# Architecture

## Purpose

`github.com/Trysac/u226-agent-task-queue` is a small, dependency-free Go library: an in-memory priority task queue for agent workers with leases that carry tokens, retries with capped exponential backoff, a bounded dead list and an injected clock.

## Shape

```text
caller code --> package agenttaskqueue (repository root)
                    |
                    +-- Queue: mutex-protected in-memory state
                    +-- ready and delayed tasks in sorted slices
                    +-- leased tasks in a map
                    +-- bounded dead list
```

The package performs no network or filesystem I/O. Token generation uses a random source and tokens are 16 random bytes hex-encoded. `Options.Now` supplies the clock used for lease expiry and delayed-task promotion.

## Public API

The public API is the exported identifiers of package `agenttaskqueue`: `Task`, `Backoff`, `Options`, `New`, `Queue` with `Enqueue`, `Lease`, `Ack`, `Nack`, `Dead` and `Stats`, plus `Leased`, `DeadTask`, `Stats`, `Event` and `Error`. `Error` carries a `Code` and `TaskID`; the codes are `invalid_task`, `duplicate`, `queue_full`, `payload_too_large`, `not_found`, `stale_lease` and `invalid_options`.

## Concurrency and ordering

A mutex protects queue state. `OnEvent`, when set, runs outside the lock. `Payload` is copied on `Enqueue` and on `Lease`. `Lease` first requeues expired leases (counting each expiry as a failed attempt), then promotes delayed tasks whose available time is at or before `Now()`, then selects the highest `Priority`; ties use enqueue sequence, and a re-queued task keeps its original sequence.

## Quality contract

The package is standard-library-only. Unexported state and helpers are not part of the contract. Invalid input returns an error; the library does not panic on input, print or exit. Terminal tasks are not rewritten: a late `Ack` or `Nack` for an acked or dead task returns `not_found`.

## Decisions

The recorded architectural choices are the single-process in-memory design, the injected clock for deterministic tests, token-checked leases, capped exponential backoff and a bounded dead list.
