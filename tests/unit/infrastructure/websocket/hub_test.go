package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ws "broadcasting/internal/infrastructure/websocket"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func newTestHub() *ws.Hub {
	hub := ws.NewHubWithTimeouts(time.Second, 2*time.Second)
	go hub.Run()

	return hub
}

func dialWithHeaders(test *testing.T, server *httptest.Server, userUUID string, headers http.Header) *websocket.Conn {
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/" + userUUID
	headers.Set("X-User-UUID", userUUID)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	assert.NoError(test, err)

	return conn
}

func dial(test *testing.T, server *httptest.Server, userUUID, token string) *websocket.Conn {
	headers := http.Header{}

	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	return dialWithHeaders(test, server, userUUID, headers)
}

// waitForClients polls Snapshot until it holds the expected number of clients.
func waitForClients(test *testing.T, hub *ws.Hub, expected int) []ws.ClientSnapshot {
	deadline := time.Now().Add(3 * time.Second)

	for {
		snapshot := hub.Snapshot()

		if len(snapshot) == expected {
			return snapshot
		}

		if time.Now().After(deadline) {
			test.Fatalf("expected %d clients, hub still has %d", expected, len(snapshot))
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestSnapshot(test *testing.T) {
	test.Run("it should return every connected client with its user uuid and token", func(test *testing.T) {
		hub := newTestHub()
		server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
		defer server.Close()

		firstConn := dial(test, server, "user-1", "token-1")
		defer func() { _ = firstConn.Close() }()

		secondConn := dial(test, server, "user-2", "")
		defer func() { _ = secondConn.Close() }()

		snapshot := waitForClients(test, hub, 2)

		tokensByUser := make(map[string]string)

		for _, client := range snapshot {
			tokensByUser[client.UserUUID] = client.Token
		}

		assert.Equal(test, map[string]string{"user-1": "token-1", "user-2": ""}, tokensByUser)
	})

	test.Run("it should capture no token from a non-bearer Authorization header", func(test *testing.T) {
		hub := newTestHub()
		server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
		defer server.Close()

		headers := http.Header{}
		headers.Set("Authorization", "Basic dXNlcjpwYXNz")

		conn := dialWithHeaders(test, server, "user-1", headers)
		defer func() { _ = conn.Close() }()

		snapshot := waitForClients(test, hub, 1)
		assert.Equal(test, "", snapshot[0].Token)
	})
}

func TestDisconnect(test *testing.T) {
	test.Run("it should remove the client and deliver the close code and reason to the peer", func(test *testing.T) {
		hub := newTestHub()
		server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
		defer server.Close()

		conn := dial(test, server, "user-1", "token-1")
		defer func() { _ = conn.Close() }()

		snapshot := waitForClients(test, hub, 1)
		hub.Disconnect(snapshot[0].Client, 4401, "token expired")

		assert.NoError(test, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
		_, _, err := conn.ReadMessage()

		var closeErr *websocket.CloseError
		assert.ErrorAs(test, err, &closeErr)
		assert.Equal(test, 4401, closeErr.Code)
		assert.Equal(test, "token expired", closeErr.Text)

		waitForClients(test, hub, 0)
	})

	test.Run("it should be a no-op for a client that already unregistered", func(test *testing.T) {
		hub := newTestHub()
		server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
		defer server.Close()

		conn := dial(test, server, "user-1", "token-1")
		snapshot := waitForClients(test, hub, 1)

		assert.NoError(test, conn.Close())
		waitForClients(test, hub, 0)

		// A stale snapshot entry must not panic (no double close of send).
		assert.NotPanics(test, func() {
			hub.Disconnect(snapshot[0].Client, 4401, "token expired")
		})
	})
}
