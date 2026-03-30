package websocket

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// CheckOrigin allows all origins in development; tighten for production.
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client represents a single WebSocket connection with a send buffer.
type Client struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub manages all active WebSocket clients and routes broadcast messages.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

// NewHub creates an initialised Hub ready to be started.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run manages the client lifecycle. Must be called in its own goroutine.
// It is the only goroutine that accesses h.clients, so no mutex is needed.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			slog.Info("websocket client connected", "total_clients", len(h.clients))

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				slog.Info("websocket client disconnected", "total_clients", len(h.clients))
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Slow client: drop the connection to avoid blocking the hub.
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

// Broadcast enqueues a message to be delivered to all connected clients.
func (h *Hub) Broadcast(message []byte) {
	h.broadcast <- message
}

// ServeWS upgrades an HTTP connection to WebSocket, registers the client,
// and starts the read and write pumps in separate goroutines.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "error", err)
		return
	}

	client := &Client{conn: conn, send: make(chan []byte, 256)}
	h.register <- client

	// Write pump: forwards messages from the send channel to the connection.
	go func() {
		defer func() {
			h.unregister <- client
			conn.Close()
		}()
		for message := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		}
	}()

	// Read pump: keeps the connection alive and detects client disconnection.
	go func() {
		defer func() {
			h.unregister <- client
			conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}
