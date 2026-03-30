package actions

import (
	"broadcasting/internal/infrastructure/websocket"
	"fmt"
)

// BroadcastLogin formats a welcome notification and sends it to all WebSocket clients.
type BroadcastLogin struct {
	hub *websocket.Hub
}

// NewBroadcastLogin creates a BroadcastLogin action backed by the given hub.
func NewBroadcastLogin(hub *websocket.Hub) *BroadcastLogin {
	return &BroadcastLogin{hub: hub}
}

// Execute formats the notification message and broadcasts it to all connected clients.
func (action *BroadcastLogin) Execute(name string) error {
	message := fmt.Sprintf("Hello %s, we are very happy to have you here!!!!", name)
	action.hub.Broadcast([]byte(message))
	return nil
}
