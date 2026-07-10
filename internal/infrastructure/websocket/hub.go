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
	token    string
	/*
	 closeReason, when set before close(send), is the close frame the writing
	 pump delivers instead of the default empty one. Written only by the hub
	 goroutine before closing sending; the channel close synchronizes-before the
	 writing pump's read, so no lock is needed.
	*/
	closeReason []byte
}

// targetedMessage carries a message meant for a single user's connections.
type targetedMessage struct {
	userUUID string
	message  []byte
}

// ClientSnapshot is a point-in-time view of one connected client.
type ClientSnapshot struct {
	Client   *Client
	UserUUID string
	Token    string
}

// disconnectRequest asks the hub to close one client with a close code.
type disconnectRequest struct {
	client *Client
	code   int
	reason string
}

// Hub manages all active WebSocket clients and routes broadcast messages.
type Hub struct {
	clients    map[string]map[*Client]bool // userUUID → set of clients
	broadcast  chan []byte
	directed   chan targetedMessage
	register   chan *Client
	unregister chan *Client
	snapshot   chan chan []ClientSnapshot
	disconnect chan disconnectRequest
	writeWait  time.Duration
	pongWait   time.Duration
	pingPeriod time.Duration
}

// NewHub creates an initialized Hub ready to be started, using production heartbeat timeouts.
func NewHub() *Hub {
	return NewHubWithTimeouts(defaultWriteWait, defaultPongWait)
}

/*
NewHubWithTimeouts creates a Hub with custom write/pong timeouts (pingPeriod is
derived as 90% of pongWait). Exposed so tests can use short timeouts instead of
waiting on production-length (60s) heartbeat intervals.
*/
func NewHubWithTimeouts(writeWait, pongWait time.Duration) *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		directed:   make(chan targetedMessage, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		snapshot:   make(chan chan []ClientSnapshot),
		disconnect: make(chan disconnectRequest),
		writeWait:  writeWait,
		pongWait:   pongWait,
		pingPeriod: (pongWait * 9) / 10,
	}
}

/*
Run manages the client lifecycle. Must be called in its own goroutine.
It is the only goroutine that accesses h.clients, so no mutex is needed.
*/
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
			hub.removeClient(client)

		case request := <-hub.disconnect:
			/*
			 Set the close frame before removeClient closes sending: the
			 channel close synchronizes-before the writing pump reading it.
			*/
			request.client.closeReason = websocket.FormatCloseMessage(request.code, request.reason)
			hub.removeClient(request.client)

		case reply := <-hub.snapshot:
			var clients []ClientSnapshot

			for userUUID, mapping := range hub.clients {
				for client := range mapping {
					clients = append(clients, ClientSnapshot{Client: client, UserUUID: userUUID, Token: client.token})
				}
			}

			reply <- clients

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

/*
removeClient deletes the client from the map and closes its send channel.
Must run on the hub goroutine. The presence check makes a second removal of
the same client a no-op, so a Disconnect racing a pump-driven unregister can
never double-close send.
*/
func (hub *Hub) removeClient(client *Client) {
	mapping, okMapping := hub.clients[client.userUUID]

	if !okMapping {
		return
	}

	_, okClient := mapping[client]

	if !okClient {
		return
	}

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

// Broadcast enqueues a message to be delivered to all connected clients.
func (hub *Hub) Broadcast(message []byte) {
	hub.broadcast <- message
}

// Snapshot returns a point-in-time view of every connected client.
func (hub *Hub) Snapshot() []ClientSnapshot {
	// Buffered so the hub goroutine never blocks delivering the reply.
	reply := make(chan []ClientSnapshot, 1)
	hub.snapshot <- reply

	return <-reply
}

/*
Disconnect closes the given client's connection, delivering a close frame
with the given application close code (4000-4999) and reason to the peer.
Safe to call with a client that is already disconnected: it becomes a no-op.
*/
func (hub *Hub) Disconnect(client *Client, code int, reason string) {
	hub.disconnect <- disconnectRequest{client: client, code: code, reason: reason}
}

// SendToUser enqueues a message to be delivered only to the given user's connected clients.
func (hub *Hub) SendToUser(userUUID string, message []byte) {
	hub.directed <- targetedMessage{userUUID: userUUID, message: message}
}

/*
extractToken returns the bearer token from the Authorization header, or ""
when absent. The gateway's forward-auth already requires this header, so
every proxied connection carries it; connections without one are handled by
the revalidation job, not rejected at upgrade time.
*/
func extractToken(request *http.Request) string {
	parts := strings.SplitN(request.Header.Get("Authorization"), " ", 2)

	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}

	return parts[1]
}

/*
ServeWS upgrades an HTTP connection to WebSocket, registers the client,
and starts the read and write pumps in separate goroutines.
It expects the X-User-UUID header to be set by the API gateway (Traefik).
*/
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

	client := &Client{conn: conn, send: make(chan []byte, 256), userUUID: headerUUID, token: extractToken(request)}
	hub.register <- client

	/*
	 Write pump: forwards messages from the channel to the connection and
	 pings the peer periodically so dead connections get detected and closed.
	*/
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
					closeMessage := client.closeReason

					if closeMessage == nil {
						closeMessage = []byte{}
					}

					_ = conn.WriteMessage(websocket.CloseMessage, closeMessage)
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

	/*
	 Read pump: keeps the connection alive, detects client disconnection,
	 and resets the read deadline whenever a pong (or any message) arrives.
	*/
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
