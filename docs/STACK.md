# Stack

## Used

| Tool | Why |
|---|---|
| Go >= 1.23 | Oldest supported release line; one toolchain covers build, test and format. |
| Go modules | Built-in dependency management with checksum verification (`go.sum`). |
| Standard library | The module has no external runtime dependencies; `sync` guards state, `time` handles clocks and durations, and `crypto/rand` is the default token source. |
| gofmt + go vet | The canonical formatter and static checks. |
| go test (race detector) | Built-in tests, runnable examples, race detection and coverage when tests are present. |

## Discarded

| Tool | Why not |
|---|---|
| Go channels as the queue | Channels have no priorities, leases, token-checked acknowledgements or retry backoff. |
| Postgres or another database queue | Adds persistence and network coordination outside the one-process scope of this package. |
| External assertion libraries | The module remains standard-library-only. |
