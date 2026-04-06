package notification

import (
	"broadcasting/internal/infrastructure/websocket"

	"github.com/gin-gonic/gin"
)

type Module struct {
	hub *websocket.Hub
}

func NewModule(hub *websocket.Hub) *Module {
	return &Module{hub: hub}
}

func (module *Module) Register(group *gin.RouterGroup) {
	group.GET("/ws/:uuid", func(c *gin.Context) {
		module.hub.ServeWS(c.Writer, c.Request)
	})
}
