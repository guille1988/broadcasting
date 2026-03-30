package bootstrap

import (
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/config"
	"broadcasting/internal/infrastructure/container"
	"broadcasting/internal/infrastructure/logger"
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
