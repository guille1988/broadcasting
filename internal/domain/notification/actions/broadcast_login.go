package actions

import (
	"broadcasting/internal/infrastructure/websocket"
	"fmt"
)

// BroadcastLogin formats a welcome notification and sends it to the logged-in user's WebSocket clients.
type BroadcastLogin struct {
	hub *websocket.Hub
}

// NewBroadcastLogin creates a BroadcastLogin action backed by the given hub.
func NewBroadcastLogin(hub *websocket.Hub) *BroadcastLogin {
	return &BroadcastLogin{hub: hub}
}

// Execute formats the notification message and sends it only to the given user's connected clients.
func (action *BroadcastLogin) Execute(userUUID, name string) error {
	message := fmt.Sprintf("Hello %s, we are very happy to have you here!!!!", name)
	action.hub.SendToUser(userUUID, []byte(message))
	return nil
}
