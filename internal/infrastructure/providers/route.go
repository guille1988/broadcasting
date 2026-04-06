package providers

import (
	"broadcasting/internal/domain/health"
	"broadcasting/internal/domain/notification"
	"broadcasting/internal/infrastructure/app"

	"github.com/gin-gonic/gin"
)

// RouteRegister is the interface for registering routes in a module.
type RouteRegister interface {
	Register(group *gin.RouterGroup)
}

// RegisterRoutes handles the wiring of dependencies and route registration.
func RegisterRoutes(engine *gin.Engine, app *app.App) {
	api := engine.Group("/api")

	registers := []RouteRegister{
		health.NewModule(),
		notification.NewModule(app.Container.Hub),
	}

	for _, register := range registers {
		register.Register(api)
	}
}
