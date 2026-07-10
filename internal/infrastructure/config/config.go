package config

import (
	"broadcasting/internal/infrastructure/env"
	"time"

	"github.com/joho/godotenv"
)

// Config represents the application configuration.
type Config struct {
	App   AppConfig
	Log   LogConfig
	Kafka KafkaConfig
	Auth  AuthClientConfig
}

/*
AuthClientConfig configures the gRPC client for the auth service and the
token revalidation job that uses it.
*/
type AuthClientConfig struct {
	GRPCAddress          string
	RevalidationInterval time.Duration
	RevalidationTimeout  time.Duration
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
	WorkerPoolSize     int
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
			WorkerPoolSize:     env.GetEnvAsInt("KAFKA_WORKER_POOL_SIZE", 20),
		},
		Auth: AuthClientConfig{
			GRPCAddress:          env.GetEnvAsString("AUTH_GRPC_ADDRESS", "auth:9090"),
			RevalidationInterval: time.Duration(env.GetEnvAsInt("TOKEN_REVALIDATION_INTERVAL_MINUTES", 5)) * time.Minute,
			RevalidationTimeout:  time.Duration(env.GetEnvAsInt("TOKEN_REVALIDATION_TIMEOUT_SECONDS", 5)) * time.Second,
		},
	}, nil
}
