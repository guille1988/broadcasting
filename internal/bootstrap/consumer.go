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

	"github.com/guille1988/go-app-shared/messaging/kafka/constants"
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

/*
RunConsumer starts the Kafka consumer and the HTTP server and orchestrates
their shutdown in the right order: stop accepting new Kafka batches, let
the in-flight one finish, close the Kafka client, then close everything else.
*/
func RunConsumer(appInstance *app.App) error {
	ctx, cancel := context.WithCancel(context.Background())

	broadcastLoginAction := actions.NewBroadcastLogin(appInstance.Container.Hub)

	provider := messaging.NewKafkaConsumer(
		appInstance.Config.Kafka.Brokers,
		appInstance.Config.Kafka.RebalanceTimeoutMs,
		appInstance.Config.Kafka.WorkerPoolSize,
	)

	/*
		Runs exactly once, on every return path (including early errors):
		stop the poll loop from starting new batches, wait for the in-flight
		one to finish, close the Kafka client, then close everything else.
	*/
	defer func() {
		slog.Info("shutting down: waiting for in-flight messages to finish...")
		cancel()

		if closeErr := provider.Close(); closeErr != nil {
			slog.Error("failed to close Kafka provider", "error", closeErr)
		}

		appInstance.CloseAll()

		slog.Info("consumer stopped safely")
	}()

	err := provider.Register(
		"broadcasting.service",
		"",
		"",
		constants.RouteUserLoggedIn,
		handlers.NewUserLoggedIn(broadcastLoginAction),
	)

	if err != nil {
		return err
	}

	if err = startRevalidationJob(ctx, appInstance); err != nil {
		return err
	}

	err = provider.StartAll(ctx)

	if err != nil {
		return err
	}

	return Run(appInstance)
}
