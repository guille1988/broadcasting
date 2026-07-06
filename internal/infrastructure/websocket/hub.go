package websocket

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// defaultWriteWait is the time allowed to write a message (data or ping) to the peer.
	defaultWriteWait = 10 * time.Second
	// defaultPongWait is the time allowed to read the next pong from the peer.
	defaultPongWait = 60 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// CheckOrigin allows all origins in development; tighten for production.
	CheckOrigin: func(request *http.Request) bool {
		return true
	},
}

// Client represents a single WebSocket connection with a send buffer.
type Client struct {
	conn     *websocket.Conn
	send     chan []byte
	userUUID string
}

// targetedMessage carries a message meant for a single user's connections.
type targetedMessage struct {
	userUUID string
	message  []byte
}

// Hub manages all active WebSocket clients and routes broadcast messages.
type Hub struct {
	clients    map[string]map[*Client]bool // userUUID → set of clients
	broadcast  chan []byte
	directed   chan targetedMessage
	register   chan *Client
	unregister chan *Client
	writeWait  time.Duration
	pongWait   time.Duration
	pingPeriod time.Duration
}

// NewHub creates an initialized Hub ready to be started, using production heartbeat timeouts.
func NewHub() *Hub {
	return NewHubWithTimeouts(defaultWriteWait, defaultPongWait)
}

// NewHubWithTimeouts creates a Hub with custom write/pong timeouts (pingPeriod is
// derived as 90% of pongWait). Exposed so tests can use short timeouts instead of
// waiting on production-length (60s) heartbeat intervals.
func NewHubWithTimeouts(writeWait, pongWait time.Duration) *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		directed:   make(chan targetedMessage, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		writeWait:  writeWait,
		pongWait:   pongWait,
		pingPeriod: (pongWait * 9) / 10,
	}
}

// Run manages the client lifecycle. Must be called in its own goroutine.
// It is the only goroutine that accesses h.clients, so no mutex is needed.
func (hub *Hub) Run() {
	for {
		select {
		case client := <-hub.register:
			if hub.clients[client.userUUID] == nil {
				hub.clients[client.userUUID] = make(map[*Client]bool)
			}
			hub.clients[client.userUUID][client] = true
			slog.Info("websocket client connected", "user_uuid", client.userUUID, "total_users", len(hub.clients))

		case client := <-hub.unregister:
			mapping, okMapping := hub.clients[client.userUUID]

			if okMapping {
				_, okClient := mapping[client]

				if okClient {
					delete(mapping, client)
					close(client.send)

					if len(mapping) == 0 {
						delete(hub.clients, client.userUUID)
					}

					slog.Info(
						"websocket client disconnected",
						"user_uuid", client.userUUID,
						"total_users", len(hub.clients),
					)
				}
			}

		case message := <-hub.broadcast:
			for _, clients := range hub.clients {
				for client := range clients {
					select {
					case client.send <- message:
					default:
						close(client.send)
						delete(clients, client)
					}
				}
			}

		case targeted := <-hub.directed:
			for client := range hub.clients[targeted.userUUID] {
				select {
				case client.send <- targeted.message:
				default:
					close(client.send)
					delete(hub.clients[targeted.userUUID], client)
				}
			}
		}
	}
}

// Broadcast enqueues a message to be delivered to all connected clients.
func (hub *Hub) Broadcast(message []byte) {
	hub.broadcast <- message
}

// SendToUser enqueues a message to be delivered only to the given user's connected clients.
func (hub *Hub) SendToUser(userUUID string, message []byte) {
	hub.directed <- targetedMessage{userUUID: userUUID, message: message}
}

// ServeWS upgrades an HTTP connection to WebSocket, registers the client,
// and starts the read and write pumps in separate goroutines.
// It expects the X-User-UUID header to be set by the API gateway (Traefik).
func (hub *Hub) ServeWS(writer http.ResponseWriter, request *http.Request) {
	headerUUID := request.Header.Get("X-User-UUID")

	if headerUUID == "" {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}

	parts := strings.Split(strings.TrimSuffix(request.URL.Path, "/"), "/")
	pathUUID := parts[len(parts)-1]

	if pathUUID == "" {
		http.Error(writer, "missing user uuid", http.StatusBadRequest)
		return
	}

	if pathUUID != headerUUID {
		http.Error(writer, "forbidden", http.StatusForbidden)
		return
	}

	conn, err := upgrader.Upgrade(writer, request, nil)

	if err != nil {
		slog.Error("websocket upgrade failed", "error", err)
		return
	}

	client := &Client{conn: conn, send: make(chan []byte, 256), userUUID: headerUUID}
	hub.register <- client

	// Write pump: forwards messages from the channel to the connection and
	// pings the peer periodically so dead connections get detected and closed.
	go func() {
		ticker := time.NewTicker(hub.pingPeriod)
		defer func() {
			ticker.Stop()
			hub.unregister <- client
			_ = conn.Close()
		}()
		for {
			select {
			case message, ok := <-client.send:
				_ = conn.SetWriteDeadline(time.Now().Add(hub.writeWait))

				if !ok {
					_ = conn.WriteMessage(websocket.CloseMessage, []byte{})
					return
				}

				if writeErr := conn.WriteMessage(websocket.TextMessage, message); writeErr != nil {
					return
				}

			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(hub.writeWait))

				if writeErr := conn.WriteMessage(websocket.PingMessage, nil); writeErr != nil {
					return
				}
			}
		}
	}()

	// Read pump: keeps the connection alive, detects client disconnection,
	// and resets the read deadline whenever a pong (or any message) arrives.
	go func() {
		defer func() {
			hub.unregister <- client
			_ = conn.Close()
		}()

		_ = conn.SetReadDeadline(time.Now().Add(hub.pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(hub.pongWait))
		})

		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}()
}
