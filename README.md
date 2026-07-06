# Broadcasting Microservice

Kafka consumer + WebSocket server that delivers real-time notifications to connected clients. Like `email`, it has no HTTP write API — its only input is Kafka events from `auth`, and its only output is a push over an already-open WebSocket connection.

---

## Features

- **Per-user real-time delivery**: a login event reaches only the WebSocket connections belonging to that specific user, via `Hub.SendToUser(uuid, message)` — not a blind broadcast to every connected client.
- **Mutex-free client registry**: the hub's connection map is owned by a single goroutine; all registration, unregistration, and broadcast operations happen through channels, not locks (see [The Hub Pattern](#the-hub-pattern)).
- **Backpressure handling**: a slow or stalled client is dropped from the hub instead of blocking message delivery to every other connected client.
- **Connection health via heartbeat**: periodic WebSocket ping/pong with read/write deadlines detects and cleans up dead connections that never sent a proper close frame.
- **Path-based identity, established by the gateway**: each client connects to `/ws/:uuid`. The `broadcasting` service itself never validates a JWT or checks who the caller is — that's enforced one layer up, by the gateway's forward-auth step (Traefik locally, nginx-ingress in production), which calls `auth`'s `/api/auth/validate` before ever letting the WebSocket request through. See the root README's [Infrastructure Architecture](../../README.md#infrastructure-architecture) for the exact configuration.

---

## Tech Stack

- **Language**: Go 1.25
- **Messaging**: Kafka via [`twmb/franz-go`](https://github.com/twmb/franz-go)
- **WebSockets**: [Gorilla WebSocket](https://github.com/gorilla/websocket)
- **Testing**: [Testify](https://github.com/stretchr/testify), in-process WebSocket server (no external containers needed)

---

## Folder Structure

> For the general architecture patterns used here — the layered `handlers/actions` structure, dependency injection, and typed config — see the **[Architecture section of the root README](../../README.md#architecture)**. This section covers only what's specific to `broadcasting`.

```text
internal/
├── bootstrap/
│   └── consumer.go        # Kafka client setup + WebSocket server, shared lifecycle
├── domain/
│   ├── notification/
│   │   ├── actions/        # BroadcastLogin — formats and routes the message to the hub
│   │   ├── handlers/         # Kafka message → DTO → action
│   │   └── module.go          # Registers GET /ws/:uuid
│   └── health/
├── infrastructure/
│   ├── websocket/
│   │   └── hub.go            # Connection registry, ping/pong, backpressure-safe broadcast
│   └── providers/messaging/    # Kafka consumer (same balancer/commit pattern as email)
└── internal/shared/               # go-app-shared submodule (Kafka DTOs, routing keys)
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

The `{uuid}` in the path identifies which user this connection belongs to — it's how the hub knows which connections to target when a `UserLoggedIn` event for that same UUID arrives.

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
| `LOG_DRIVER` | `stdout` | `stdout` \| `file` |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |

---

## Getting Started

```bash
go run cmd/consumer/main.go
```

Or from the repo root: `make up`, `make test` (see the [root README](../../README.md)). Tests spin up an in-process WebSocket server — no external containers required for this service.

> After adding a dependency to `go.mod`, regenerate `go.sum` inside the container: `docker exec broadcasting go mod tidy`.
