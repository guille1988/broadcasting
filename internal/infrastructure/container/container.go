package container

import (
	"broadcasting/internal/infrastructure/websocket"
)

// Container holds the application's shared dependencies.
type Container struct {
	Hub *websocket.Hub
}

// New creates a container and starts the WebSocket hub in its own goroutine.
func New() *Container {
	hub := websocket.NewHub()
	go hub.Run()

	return &Container{Hub: hub}
}
