package notifications

import (
	wshub "broadcasting/internal/infrastructure/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func dialTestClient(test *testing.T, serverURL, userUUID string) *websocket.Conn {
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/api/ws/" + userUUID
	headers := http.Header{}
	headers.Set("X-User-UUID", userUUID)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	assert.NoError(test, err)
	return conn
}

func TestWebSocketHeartbeat(test *testing.T) {
	test.Run("it should keep a responsive connection alive past the pong wait window", func(test *testing.T) {
		hub := wshub.NewHubWithTimeouts(50*time.Millisecond, 500*time.Millisecond)
		go hub.Run()

		mux := http.NewServeMux()
		mux.HandleFunc("/api/ws/", func(writer http.ResponseWriter, request *http.Request) {
			hub.ServeWS(writer, request)
		})
		server := httptest.NewServer(mux)
		defer server.Close()

		userUUID := "heartbeat-alive-user"
		conn := dialTestClient(test, server.URL, userUUID)
		defer func() { _ = conn.Close() }()

		received := make(chan string, 1)

		/*
			Actively read so the client library processes incoming Ping control
			frames and auto-responds with Pong (gorilla's default ping handler),
			and capture any text message the hub sends us.
		*/
		go func() {
			for {
				messageType, message, err := conn.ReadMessage()

				if err != nil {
					return
				}

				if messageType == websocket.TextMessage {
					received <- string(message)
				}
			}
		}()

		/*
			Longer than the 500ms pongWait; the 450ms ping keeps resetting it as
			long as the client keeps responding.
		*/
		time.Sleep(700 * time.Millisecond)

		hub.SendToUser(userUUID, []byte("still alive"))

		select {
		case message := <-received:
			assert.Equal(test, "still alive", message)
		case <-time.After(2 * time.Second):
			test.Fatal("connection did not survive past the pong wait window despite responding to pings")
		}
	})

	test.Run("it should close an unresponsive connection after the pong wait window elapses", func(test *testing.T) {
		hub := wshub.NewHubWithTimeouts(50*time.Millisecond, 300*time.Millisecond)
		go hub.Run()

		mux := http.NewServeMux()
		mux.HandleFunc("/api/ws/", func(writer http.ResponseWriter, request *http.Request) {
			hub.ServeWS(writer, request)
		})
		server := httptest.NewServer(mux)
		defer server.Close()

		userUUID := "heartbeat-dead-user"
		conn := dialTestClient(test, server.URL, userUUID)
		defer func() { _ = conn.Close() }()

		/*
			Deliberately never read: the client never processes the server's Ping
			frames, so no Pong is ever sent back, and the server's read deadline
			must elapse without being reset.
		*/
		time.Sleep(700 * time.Millisecond)

		assert.NoError(test, conn.SetReadDeadline(time.Now().Add(1*time.Second)))
		_, _, err := conn.ReadMessage()
		assert.Error(test, err, "the server must have closed the unresponsive connection by now")
	})
}
