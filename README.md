### Broadcasting Microservice in Go

This is a specialized broadcasting microservice built with **Go**, designed to deliver real-time notifications to connected clients using **WebSockets**. It consumes events from **Kafka** and instantly pushes messages to all active WebSocket connections.

---

### Features

*   **Real-time Notifications**: Pushes messages to all connected WebSocket clients instantly upon receiving a Kafka event.
*   **WebSocket Server**: Exposes a `/ws` endpoint for clients to subscribe to live updates.
*   **Asynchronous Processing**: Consumes events from Kafka without blocking the HTTP server.
*   **Hub Pattern**: Thread-safe client management using Go channels — no mutexes required.
*   **Clean Architecture**: Strict separation of concerns (domain, infrastructure, and application layers).
*   **Containerized**: Fully Dockerized for seamless integration with the microservices ecosystem.
*   **Testing Suite**: Includes integration tests using an in-process WebSocket server.

---

### Tech Stack

*   **Language**: Go 1.25+
*   **Messaging**: [Kafka (twmb/franz-go)](https://github.com/twmb/franz-go)
*   **WebSockets**: [Gorilla WebSocket](https://github.com/gorilla/websocket)
*   **Testing**: [Testify](https://github.com/stretchr/testify)

---

### Prerequisites

*   [Docker](https://www.docker.com/) and Docker Compose.
*   [Go](https://golang.org/) (optional, for local development).
*   `make` (utility to run Makefile commands from the root).

---

### Getting Started

1.  **Clone the repository** (if not done yet):
    ```bash
    git clone <repository-url>
    cd broadcasting
    ```

2.  **Environment Setup**:
    Ensure the `.env` file is configured with your Kafka broker address.

3.  **Run the Consumer**:
    This service operates as a Kafka consumer and WebSocket server simultaneously.
    ```bash
    go run cmd/consumer/main.go
    ```

---

### Development Commands

From the root `Makefile`, you can manage this service:

| Command | Description |
| :--- | :--- |
| `make up` | Start all infrastructure including Kafka. |
| `make compile` | Compile the broadcasting consumer binary. |
| `make test` | Run tests for the broadcasting microservice. |

---

### WebSocket Connection

Connect any WebSocket client to receive real-time notifications:

```
ws://localhost:8081/ws
```

Example notification received on login:
```
Hello Alice, we are very happy to have you here!!!!
```

---

### Message Consumers

#### User Logged In (`broadcasting.service`)
*   **Topic**: `user.logged_in`
*   **Payload**: `UserLoggedIn` (contains user email and name)
*   **Action**: Broadcasts `"Hello {name}, we are very happy to have you here!!!!"` to all connected WebSocket clients.

---

### Messaging — Consuming a new message

To consume a new message from Kafka, follow these 4 steps without touching any messaging infrastructure files:

**1. Create the DTO** in `internal/shared/messaging/kafka/dtos/`:
```go
// internal/shared/messaging/kafka/dtos/user_updated.go
type UserUpdated struct {
    ID   uint   `json:"id"`
    Name string `json:"name"`
}
```

**2. Create the action** in `internal/domain/notification/actions/`:
```go
// internal/domain/notification/actions/broadcast_user_updated.go
func (a *BroadcastUserUpdated) Execute(name string) error {
    message := fmt.Sprintf("User %s has been updated", name)
    a.hub.Broadcast([]byte(message))
    return nil
}
```

**3. Create the handler** in `internal/domain/notification/handlers/`:
```go
// internal/domain/notification/handlers/user_updated.go
func (h *UserUpdated) Handle(body []byte) error {
    var dto dtos.UserUpdated
    if err := json.Unmarshal(body, &dto); err != nil {
        return fmt.Errorf("failed to unmarshal user_updated dto: %w", err)
    }
    return h.action.Execute(dto.Name)
}
```

**4. Register the handler** in `internal/bootstrap/consumer.go`:
```go
provider.Register(
    "broadcasting.service",
    "",
    "",
    "user.updated",
    handlers.NewUserUpdated(broadcastUserUpdatedAction),
)
```

No infrastructure files need to be modified.

---

### Project Structure

```text
├── cmd/                # Entry point (Consumer)
├── internal/
│   ├── bootstrap/      # App initialization logic (Kafka, WebSocket server)
│   ├── domain/         # Business logic (Notification module)
│   │   └── notification/ # Actions, handlers
│   ├── infrastructure/ # Frameworks & Drivers (Kafka, WebSocket Hub, Logger)
│   ├── shared/         # Shared DTOs for messaging
├── tests/              # Integration tests
├── Dockerfile          # Container build configuration
└── go.mod              # Dependencies
```

---

### Environment Variables

Key configurations:
*   `APP_NAME`: Service name (default: `broadcasting`).
*   `APP_ENV`: Environment (`local`, `testing`, `staging`, `production`).
*   `KAFKA_BROKERS`: Kafka broker address (e.g. `kafka:9092`).
*   `LOG_DRIVER`: Log output driver (`stdout` or `file`).
*   `LOG_LEVEL`: Log level (`debug`, `info`, `warn`, `error`).

---

### Testing

Run tests using the project root Makefile:
```bash
make test
```

The tests use an in-process WebSocket server to verify that incoming Kafka messages are correctly broadcasted to connected clients. No external containers are required.

> **Note**: After adding new dependencies to `go.mod`, run `go mod tidy` inside the container to regenerate `go.sum`:
> ```bash
> docker exec broadcasting go mod tidy
> ```
