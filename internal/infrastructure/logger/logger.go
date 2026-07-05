package logger

import (
	"broadcasting/internal/infrastructure/config"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// New initializes the global slog logger based on the provided driver, path, and level.
func New(log config.LogConfig, serviceName string) error {
	level := parseLevel(log.Level)
	var output io.Writer = os.Stdout

	if log.Driver == config.File {
		dir := filepath.Dir(log.Path)
		err := os.MkdirAll(dir, 0755)

		if err != nil {
			return err
		}

		var file *os.File
		file, err = os.OpenFile(log.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			return err
		}

		output = file
	}

	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}).
		WithAttrs([]slog.Attr{
			slog.String("service", serviceName),
		})

	logger := slog.New(handler)

	slog.SetDefault(logger)

	return nil
}

// Fatal logs the error and exits the process.
func Fatal(err error) {
	slog.Error(err.Error())
	os.Exit(1)
}

// parseLevel maps the configured log level to its slog equivalent, defaulting to info.
func parseLevel(level config.LogLevel) slog.Level {
	switch level {
	case config.DebugLevel:
		return slog.LevelDebug
	case config.WarnLevel:
		return slog.LevelWarn
	case config.ErrorLevel:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
