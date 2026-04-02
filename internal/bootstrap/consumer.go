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
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

// RunConsumer starts the RabbitMQ consumer and the WebSocket HTTP server.
func RunConsumer(appInstance *app.App) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer appInstance.CloseAll()

	broadcastLoginAction := actions.NewBroadcastLogin(appInstance.Container.Hub)

	provider := messaging.NewRabbitMQRegister(appInstance.Config.RabbitMQ)
	defer func() {
		if err := provider.Close(); err != nil {
			slog.Error("failed to close rabbitmq provider", "error", err)
		}
	}()

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

	if err = provider.StartAll(ctx); err != nil {
		return err
	}

	// Serve WebSocket connections on /ws alongside the RabbitMQ consumer.
	http.HandleFunc("/ws/", appInstance.Container.Hub.ServeWS)

	go func() {
		slog.Info("websocket server starting", "addr", ":8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			slog.Error("websocket server error", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	slog.Info("consumer is running and waiting for messages...")
	<-stop
	slog.Info("consumer stopped safely")

	return nil
}
