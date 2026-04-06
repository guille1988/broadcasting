package bootstrap

import (
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/middlewares"
	"broadcasting/internal/infrastructure/providers"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

// Run starts the HTTP server and manages its lifecycle.
func Run(appInstance *app.App) error {
	srv := newServer(appInstance)

	serverErrors := make(chan error, 1)

	go func() {
		slog.Info("server is starting", "addr", srv.Addr)
		err := srv.ListenAndServe()

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	err := wait(srv, serverErrors)

	if err != nil {
		return err
	}

	appInstance.CloseAll()
	slog.Info("application stopped safely")

	return nil
}

// newServer initializes the HTTP engine and server configuration.
func newServer(appInstance *app.App) *http.Server {
	engine := gin.New()

	middlewares.RegisterMiddlewares(engine)
	providers.RegisterRoutes(engine, appInstance)

	return &http.Server{
		Addr:    fmt.Sprintf("%s:%s", appInstance.Config.App.Host, appInstance.Config.App.Port),
		Handler: engine,
	}
}

// wait manages the application lifecycle (blocking until signal or error).
func wait(srv *http.Server, serverErrors chan error) error {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)
	case sig := <-shutdown:
		slog.Info("starting graceful shutdown", "signal", sig.String())

		return shutdownServer(srv)
	}
}

// shutdownServer stops the HTTP server gracefully.
func shutdownServer(srv *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()

		return fmt.Errorf("could not stop server gracefully: %w", err)
	}

	return nil
}
