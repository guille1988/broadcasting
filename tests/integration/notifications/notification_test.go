package notifications

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/domain/notification/handlers"
	"broadcasting/internal/shared/messaging/rabbitmq/dtos"
	"broadcasting/tests/integration"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestNotificationModule(test *testing.T) {
	integration.TestCase(test, "it should broadcast a login notification to all connected websocket clients", func(test *testing.T) {
		name := "Alice"
		email := "alice@example.com"

		// Connect a WebSocket client to the test server.
		wsURL := "ws" + strings.TrimPrefix(integration.TestServer.URL, "http")
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		assert.NoError(test, err)
		defer conn.Close()

		// Trigger the handler directly, mirroring how the RabbitMQ consumer would.
		broadcastAction := actions.NewBroadcastLogin(integration.TestApp.Container.Hub)
		handler := handlers.NewUserLoggedIn(broadcastAction)

		body, _ := json.Marshal(dtos.UserLoggedIn{Email: email, Name: name})
		err = handler.Handle(body)
		assert.NoError(test, err)

		// Read the broadcasted message with a short deadline to avoid hanging.
		err = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		assert.NoError(test, err)

		_, message, err := conn.ReadMessage()
		assert.NoError(test, err)

		expected := fmt.Sprintf("Hello %s, we are very happy to have you here!!!!", name)
		assert.Equal(test, expected, string(message))
	})
}
