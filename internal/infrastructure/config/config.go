package config

import (
	"broadcasting/internal/infrastructure/env"

	"github.com/joho/godotenv"
)

// Config represents the application configuration.
type Config struct {
	App   AppConfig
	Log   LogConfig
	Kafka KafkaConfig
}

// AppConfig represents the application configuration.
type AppConfig struct {
	Name string
	Env  Env
	Host string
	Port string
}

type KafkaConfig struct {
	Brokers            string
	RebalanceTimeoutMs int
}

type LogConfig struct {
	Driver LogDriver
	Path   string
	Level  LogLevel
}

type Env string

const (
	LocalEnv      Env = "local"
	TestingEnv    Env = "testing"
	StagingEnv    Env = "staging"
	ProductionEnv Env = "production"
)

type LogLevel string

const (
	DebugLevel LogLevel = "debug"
	InfoLevel  LogLevel = "info"
	WarnLevel  LogLevel = "warn"
	ErrorLevel LogLevel = "error"
)

type LogDriver string

const (
	StdoutFormat LogDriver = "stdout"
	File         LogDriver = "file"
)

// New creates a new configuration instance.
func New() (*Config, error) {
	_ = godotenv.Load()

	return &Config{
		App: AppConfig{
			Name: env.GetEnvAsString("APP_NAME", "broadcasting"),
			Env:  Env(env.GetEnvAsString("APP_ENV", string(LocalEnv))),
			Host: env.GetEnvAsString("APP_HOST", ""),
			Port: env.GetEnvAsString("APP_PORT", "8080"),
		},
		Log: LogConfig{
			Driver: LogDriver(env.GetEnvAsString("LOG_DRIVER", string(StdoutFormat))),
			Path:   env.GetEnvAsString("LOG_PATH", "logs/broadcasting.log"),
			Level:  LogLevel(env.GetEnvAsString("LOG_LEVEL", string(InfoLevel))),
		},
		Kafka: KafkaConfig{
			Brokers:            env.GetEnvAsString("KAFKA_BROKERS", "kafka:9092"),
			RebalanceTimeoutMs: env.GetEnvAsInt("KAFKA_REBALANCE_TIMEOUT_MS", 600000),
		},
	}, nil
}
