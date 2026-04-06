package bootstrap

import (
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/config"
	"broadcasting/internal/infrastructure/container"
	"broadcasting/internal/infrastructure/logger"
	"broadcasting/internal/infrastructure/middlewares"
	"broadcasting/internal/infrastructure/providers"
	"net/http"

	"github.com/gin-gonic/gin"
)

// NewTestingApp initializes the app optimized for tests.
func NewTestingApp(cfg *config.Config) (*app.App, error) {
	cfg.App.Env = config.TestingEnv

	if err := logger.New(cfg.Log, cfg.App.Name); err != nil {
		return nil, err
	}

	return &app.App{
		Config:    cfg,
		Container: container.New(),
	}, nil
}

// NewTestingHandler returns the Gin engine without an HTTP server.
func NewTestingHandler(appInstance *app.App) http.Handler {
	gin.SetMode(gin.TestMode)
	engine := gin.New()

	middlewares.RegisterMiddlewares(engine)
	providers.RegisterRoutes(engine, appInstance)

	return engine
}
