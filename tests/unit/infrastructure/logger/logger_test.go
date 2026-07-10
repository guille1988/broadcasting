package logger

import (
	"broadcasting/internal/infrastructure/config"
	"broadcasting/internal/infrastructure/logger"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAppliesConfiguredLogLevel(test *testing.T) {
	err := logger.New(config.LogConfig{Driver: config.StdoutFormat, Level: config.ErrorLevel}, "test-service")
	assert.NoError(test, err)

	handler := slog.Default().Handler()
	ctx := context.Background()

	assert.False(test, handler.Enabled(ctx, slog.LevelInfo), "info logs should be disabled when LOG_LEVEL=error")
	assert.False(test, handler.Enabled(ctx, slog.LevelWarn), "warn logs should be disabled when LOG_LEVEL=error")
	assert.True(test, handler.Enabled(ctx, slog.LevelError), "error logs should be enabled when LOG_LEVEL=error")
}
