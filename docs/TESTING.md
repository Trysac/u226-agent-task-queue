# Testing

## Running

```sh
go test ./...
go test -race ./...
```

## Existing tests

No `_test.go` files are present in the provided repository snapshot. This document therefore does not name test files or assert coverage.

## Specification coverage

The specification defines the behavior that tests must cover: priority selection and FIFO ties (`"b"`, `"c"`, `"a"`); capped exponential backoff at 1s, 2s and then dead at `MaxAttempts` 3; lease expiry and `stale_lease` on an old token; duplicate, invalid-task, `queue_full` and payload-size errors; dead-list eviction at `MaxDead` and `Stats`; events with `Type`, `TaskID` and `Attempt` but no payload; and `go test -race` with 4 goroutines leasing and acking 100 tasks, each acked exactly once.

## Tiers

No separate conformance tier is present in the provided files.

## Layout

No test files or `testdata/` fixtures are present in the provided snapshot.
