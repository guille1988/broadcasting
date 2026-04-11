package notifications

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/domain/notification/handlers"
	"broadcasting/tests/integration"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/guille1988/go-app-shared/messaging/kafka/dtos"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestNotificationModule(test *testing.T) {
	integration.TestCase(test, "it should broadcast a login notification to all connected websocket clients", func(test *testing.T) {
		name := "Alice"
		email := "alice@example.com"
		userUUID := "test-user-uuid"
		wsURL := "ws" + strings.TrimPrefix(integration.TestServer.URL, "http") + "/api/ws/" + userUUID

		headers := http.Header{}
		headers.Set("X-User-UUID", userUUID)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
		assert.NoError(test, err)

		defer func(conn *websocket.Conn) {
			err = conn.Close()

			if err != nil {
				return
			}
		}(conn)

		broadcastAction := actions.NewBroadcastLogin(integration.TestApp.Container.Hub)
		handler := handlers.NewUserLoggedIn(broadcastAction)

		body, _ := json.Marshal(dtos.UserLoggedIn{Email: email, Name: name})
		err = handler.Handle(body)
		assert.NoError(test, err)

		err = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		assert.NoError(test, err)

		var message []byte
		_, message, err = conn.ReadMessage()
		assert.NoError(test, err)

		expected := fmt.Sprintf("Hello %s, we are very happy to have you here!!!!", name)
		assert.Equal(test, expected, string(message))
	})
}
