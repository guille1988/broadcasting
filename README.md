# Broadcasting Microservice

Kafka consumer + WebSocket server that delivers real-time notifications to connected clients. Like `email`, it has no HTTP write API — its only input is Kafka events from `auth`, and its only output is a push over an already-open WebSocket connection. Its one synchronous dependency is a periodic gRPC call to `auth` that revalidates the tokens of open connections (see [Token Revalidation](#token-revalidation)).

---

## Features

- **Per-user real-time delivery**: a login event reaches only the WebSocket connections belonging to that specific user, via `Hub.SendToUser(uuid, message)` — not a blind broadcast to every connected client.
- **Mutex-free client registry**: the hub's connection map is owned by a single goroutine; all registration, unregistration, and broadcast operations happen through channels, not locks (see [The Hub Pattern](#the-hub-pattern)).
- **Backpressure handling**: a slow or stalled client is dropped from the hub instead of blocking message delivery to every other connected client.
- **Connection health via heartbeat**: periodic WebSocket ping/pong with read/write deadlines detects and cleans up dead connections that never sent a proper close frame.
- **Path-based identity, established by the gateway**: each client connects to `/ws/:uuid`. The handshake is authenticated one layer up, by the gateway's forward-auth step (Traefik locally, nginx-ingress in production), which calls `auth`'s `/api/auth/validate` before ever letting the WebSocket request through. See the root README's [Infrastructure Architecture](../../README.md#infrastructure-architecture) for the exact configuration.
- **Token revalidation for long-lived connections**: the gateway only authenticates the handshake — a connection could otherwise outlive its credentials indefinitely. A background job re-asks `auth` (over gRPC) whether each connection's token is still valid every N minutes, and closes stale ones with application close code **4401** (see [Token Revalidation](#token-revalidation)).
- **Prometheus metrics** (`/metrics`): a client-side interceptor counts every outgoing gRPC call in `grpc_requests_total{method,code}` — the first metrics endpoint this service exposes.

---

## Tech Stack

- **Language**: Go 1.25
- **Messaging**: Kafka via [`twmb/franz-go`](https://github.com/twmb/franz-go)
- **WebSockets**: [Gorilla WebSocket](https://github.com/gorilla/websocket)
- **RPC**: [gRPC](https://grpc.io/) client for `auth`'s `AuthService` (contract in `go-app-shared`, see [Token Revalidation](#token-revalidation))
- **Testing**: [Testify](https://github.com/stretchr/testify), in-process WebSocket server (no external containers needed)

---

## Folder Structure

> For the general architecture patterns used here — the layered `handlers/actions` structure, dependency injection, and typed config — see the **[Architecture section of the root README](../../README.md#architecture)**. This section covers only what's specific to `broadcasting`.

```text
internal/
├── bootstrap/
│   ├── consumer.go        # Kafka client setup + WebSocket server, shared lifecycle
│   └── revalidation.go    # Wires the token revalidation job (gRPC client + ticker)
├── domain/
│   ├── notification/
│   │   ├── actions/        # BroadcastLogin, RevalidateTokens
│   │   ├── handlers/         # Kafka message → DTO → action
│   │   └── module.go          # Registers GET /ws/:uuid
│   └── health/
├── infrastructure/
│   ├── websocket/
│   │   └── hub.go            # Connection registry, ping/pong, backpressure-safe broadcast
│   ├── jobs/
│   │   └── runner.go         # Generic ticker loop: runs a function every interval until shutdown
│   └── providers/
│       ├── grpc/              # AuthClient — gRPC client for auth's AuthService
│       └── messaging/          # Kafka consumer (same balancer/commit pattern as email)
└── internal/shared/               # go-app-shared submodule (Kafka DTOs, gRPC contracts, routing keys)
```

---

## The Hub Pattern

The WebSocket hub avoids a `sync.Mutex` around its client map entirely. Instead, a single goroutine owns the map and reacts to three channels: `register`, `unregister`, and `broadcast`. Every other goroutine (one read pump + one write pump per connected client) only ever *sends* to those channels — never reads or writes the map directly.

This gives two properties for free:
- **No lock contention**, even with thousands of concurrent connections, because there's fundamentally nothing to lock — the map has exactly one reader/writer.
- **Safe backpressure**: when broadcasting, a send to a client's outbound channel uses a non-blocking `select` with a `default` case. If a client's buffer is full (it's not reading fast enough), that one client is dropped from the hub instead of the whole hub stalling waiting for it.

### Connection Health

Every connection is guarded by:
- A **write deadline** on every outbound write (including pings).
- A **read deadline** reset every time a pong is received (`SetPongHandler`).
- A **ticker** on the write pump that sends a ping at a fixed interval.

If a client stops responding to pings, its read deadline expires, the read pump errors out, and the connection is cleaned up from the hub — without waiting for a TCP-level timeout that could take minutes.

---

## Consumers

| Consumer group | Topic | Payload | Action |
|---|---|---|---|
| `broadcasting.service` | `user.logged_in` | `UserLoggedIn` (UUID, name) | `BroadcastLogin.Execute` → `Hub.SendToUser(uuid, "Hello {name}, ...")` |

---

## WebSocket Connection

```
ws://localhost:8081/ws/{uuid}
```

The `{uuid}` in the path identifies which user this connection belongs to — it's how the hub knows which connections to target when a `UserLoggedIn` event for that same UUID arrives. The `Authorization: Bearer` header (which the gateway's forward-auth already requires) is captured at the handshake and stored on the connection, so the revalidation job can later re-check it.

---

## Token Revalidation

The gateway authenticates the WebSocket **handshake**, but a connection can stay open for hours — long after its token expired or its user logged out. This service closes that gap with a background job (`RevalidateTokens`, run by `jobs.Run` every `TOKEN_REVALIDATION_INTERVAL_MINUTES`):

1. **Snapshot** the hub's open connections (through a channel, preserving the hub's single-goroutine ownership — no locks added).
2. **Validate each unique token once** against `auth`'s gRPC `AuthService/ValidateToken` (contract in `go-app-shared`'s `rpc/auth/v1`). `auth` checks both the JWT itself and whether the user still holds a live session, so a logout revokes connections within one tick even while the JWT is still cryptographically valid.
3. **Close** the connections whose token came back invalid, with application close code **4401** and the reason (`EXPIRED`, `REVOKED`, ...) as the close message.

**The client contract**: on close code `4401`, refresh your token and reconnect. It is deliberately distinguishable from a network failure (`1006`) so clients can react differently.

**Failure semantics are asymmetric on purpose:**
- **Fail open** on infrastructure errors: if `auth` (or its session store) is unreachable, the tick logs a warning and keeps every connection — users are never punished for an internal outage; the next successful tick enforces expiry.
- **Fail closed** on missing tokens: a connection with no captured token never passed the gateway (it dialed the service directly) and is closed without asking `auth`.

The worst-case lifetime of a stale connection is `token expiry + revalidation interval` — tune `TOKEN_REVALIDATION_INTERVAL_MINUTES` to trade Redis/gRPC load against that window.

**Instrumented on both ends**: a `UnaryClientInterceptor` increments `grpc_requests_total{method,code}` on broadcasting's own `/metrics`, mirroring the counter `auth` keeps on the server side of the same RPC — so a `ValidateToken` failure is visible from whichever service you're looking at.

---

## Messaging — Consuming a New Event

To add a new consumer without touching messaging infrastructure:

**1. Add the DTO** to the shared module (`internal/shared/messaging/kafka/dtos/`).

**2. Create the action** in `internal/domain/notification/actions/`, depending on `*websocket.Hub` and calling either `SendToUser` (targeted) or `Broadcast` (all connected clients — use deliberately, it's the exception, not the default).

**3. Create the handler** in `internal/domain/notification/handlers/`:
```go
func (h *UserUpdated) Handle(body []byte) error {
    var dto dtos.UserUpdated
    if err := json.Unmarshal(body, &dto); err != nil {
        return fmt.Errorf("failed to unmarshal user_updated dto: %w", err)
    }
    return h.action.Execute(dto.UUID, dto.Name)
}
```

**4. Register it** in `internal/bootstrap/consumer.go`.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `APP_NAME` | `broadcasting` | Service name |
| `APP_ENV` | `local` | `local` \| `testing` \| `staging` \| `production` |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap servers |
| `AUTH_GRPC_ADDRESS` | `auth:9090` | auth's gRPC endpoint for token revalidation |
| `TOKEN_REVALIDATION_INTERVAL_MINUTES` | `5` | How often the revalidation job runs |
| `TOKEN_REVALIDATION_TIMEOUT_SECONDS` | `5` | Per-call deadline for the gRPC validation |
| `LOG_DRIVER` | `stdout` | `stdout` \| `file` |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |

---

## Getting Started

```bash
go run cmd/consumer/main.go
```

Or from the repo root: `make up`, `make test` (see the [root README](../../README.md)). Tests spin up an in-process WebSocket server — no external containers required for this service.

> After adding a dependency to `go.mod`, regenerate `go.sum` inside the container: `docker exec broadcasting go mod tidy`.
