package health

import (
	"broadcasting/internal/domain/health/handlers"

	"github.com/gin-gonic/gin"
)

type Module struct{}

func NewModule() *Module {
	return &Module{}
}

func (module *Module) Register(group *gin.RouterGroup) {
	group.GET("/health", handlers.Health)
}
