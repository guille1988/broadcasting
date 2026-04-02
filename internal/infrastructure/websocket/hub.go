package websocket

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
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

// Hub manages all active WebSocket clients and routes broadcast messages.
type Hub struct {
	clients    map[string]map[*Client]bool // userUUID → set of clients
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

// NewHub creates an initialized Hub ready to be started.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
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
		}
	}
}

// Broadcast enqueues a message to be delivered to all connected clients.
func (hub *Hub) Broadcast(message []byte) {
	hub.broadcast <- message
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

	pathUUID := strings.TrimPrefix(request.URL.Path, "/ws/")

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

	// Write pump: forwards messages from the channel to the connection.
	go func() {
		defer func() {
			hub.unregister <- client
			err = conn.Close()

			if err != nil {
				return
			}
		}()
		for message := range client.send {
			if err = conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		}
	}()

	// Read pump: keeps the connection alive and detects client disconnection.
	go func() {
		defer func() {
			hub.unregister <- client
			err = conn.Close()

			if err != nil {
				return
			}
		}()
		for {
			_, _, err = conn.ReadMessage()

			if err != nil {
				return
			}
		}
	}()
}
