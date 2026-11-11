# Project record

## Objective

In-memory priority task queue for agent workers in Go: leases with tokens, retries with capped exponential backoff, a bounded dead list and an injected clock.

## Scope

- In scope: one-process, in-memory task queue with priorities, leases that expire, token-checked `Ack` and `Nack`, capped exponential backoff, a bounded dead list, `Stats`, an `OnEvent` hook and an injected clock through `Options.Now`.
- In scope: validation of IDs, priorities, payload size and queue capacity; duplicate detection; payload copying on enqueue and lease; deterministic token generation in tests.
- Out of scope: persistence, multiple processes, delayed scheduling by wall-clock time, task dependencies (a DAG runner does that) and fairness across kinds.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 1067883 / 98674 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.1434 |

## Factory run u226 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | deepseek-v4-pro | yes | 1437 | 25959 | 0 | 3328 | 0.0524 |
| 2026-11-10 | generation | deepseek-v4-pro | yes | 29 | 23758 | 0 | 4736 | 0.0472 |
| 2026-11-10 | fix-1 | deepseek-flash | no | 26270 | 10224 | 0 | 409728 | 0.0113 |
| 2026-11-10 | audit | deepseek-flash | no | 29118 | 19009 | 0 | 355456 | 0.0168 |
| 2026-11-10 | fix-2 | deepseek-flash | no | 17051 | 7971 | 0 | 216192 | 0.0080 |
| 2026-11-10 | docs | deepseek-flash | no | 4154 | 11753 | 0 | 384 | 0.0077 |

Total: $0.1434. Prices: deepseek line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
