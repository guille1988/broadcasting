package bootstrap

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/domain/notification/handlers"
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/config"
	"broadcasting/internal/infrastructure/container"
	"broadcasting/internal/infrastructure/logger"
	"broadcasting/internal/infrastructure/providers/messaging"
	"context"
	"log/slog"
)

// NewConsumer initializes the app instance with all necessary configuration.
func NewConsumer() (*app.App, error) {
	cfg, err := config.New()
	if err != nil {
		return nil, err
	}

	if err = logger.New(cfg.Log, cfg.App.Name); err != nil {
		return nil, err
	}

	ctr := container.New()

	return &app.App{
		Config:    cfg,
		Container: ctr,
	}, nil
}

// RunConsumer starts the RabbitMQ consumer and the HTTP server.
func RunConsumer(appInstance *app.App) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer appInstance.CloseAll()

	broadcastLoginAction := actions.NewBroadcastLogin(appInstance.Container.Hub)

	provider := messaging.NewRabbitMQRegister(appInstance.Config.RabbitMQ)

	appInstance.AddCloser(func() error {
		err := provider.Close()

		if err != nil {
			slog.Error("failed to close rabbitmq provider", "error", err)
		}

		return nil
	})

	err := provider.Register(
		"broadcasting.service",
		"auth.events",
		"topic",
		"user.logged_in",
		handlers.NewUserLoggedIn(broadcastLoginAction),
	)

	if err != nil {
		return err
	}

	err = provider.StartAll(ctx)

	if err != nil {
		return err
	}

	return Run(appInstance)
}
