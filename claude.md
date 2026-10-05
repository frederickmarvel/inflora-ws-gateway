# AI Repository Guide: inflora-ws-gateway

## Purpose

`inflora-ws-gateway` is the OBS overlay HTTP and WebSocket gateway for the active Inflora platform. It authenticates overlay clients, consumes alert events, preserves per-stream sequencing, and fans donation display messages out to connected browser sources. The canonical port is `8083`.

The gateway is a delivery edge, not the source of truth for payments, donation state, pricing, or ledger balances.

## Important paths

- `cmd/gateway/`: process composition and entrypoint.
- `internal/auth/`: overlay authentication.
- `internal/handler/`: HTTP/WebSocket endpoints.
- `internal/subscriber/`: event-bus consumption.
- `internal/seq/`: ordering/sequence behavior.
- `internal/ws/`: connection management and fan-out.
- `internal/config/`: environment configuration.

## Contract and correctness rules

- Use `/Users/frederickmarvel/Inflora/almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/` for event and connection contracts.
- Consume canonical versioned events; do not infer payment success from non-final statuses.
- Treat overlay tokens as secrets. Never log raw tokens or place them in metrics labels.
- Preserve ordering and duplicate-handling semantics across reconnects and event redelivery.
- Apply bounded queues, backpressure, timeouts, and cleanup so slow clients cannot exhaust the process.
- Render/display duration comes from the immutable donation snapshot; do not recalculate pricing here.

## Commands

```sh
make tidy
make lint
make test
make build
make run
```

Run `go test ./...`, `go vet ./...`, and `git diff --check`. Test authentication failure, reconnect, duplicate, ordering, and slow-client paths when changing WebSocket behavior.
