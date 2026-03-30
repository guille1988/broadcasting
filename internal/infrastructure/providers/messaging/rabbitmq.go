package messaging

import (
	"context"
	"broadcasting/internal/infrastructure/config"
	"broadcasting/internal/infrastructure/rabbitmq"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueEntry struct {
	consumer *rabbitmq.Consumer
	handlers map[string]rabbitmq.MessageHandler
}

// RabbitMQRegister manages multiple queue consumers and routes messages to handlers.
type RabbitMQRegister struct {
	cfg    config.RabbitMQConfig
	queues map[string]*queueEntry
}

// NewRabbitMQRegister creates an empty register backed by the given config.
func NewRabbitMQRegister(cfg config.RabbitMQConfig) *RabbitMQRegister {
	return &RabbitMQRegister{
		cfg:    cfg,
		queues: make(map[string]*queueEntry),
	}
}

// Register binds a handler to a queue/exchange/routingKey combination.
func (provider *RabbitMQRegister) Register(queue, exchange, exchangeType, routingKey string, handler rabbitmq.MessageHandler) error {
	entry, ok := provider.queues[queue]

	if !ok {
		consumer, err := rabbitmq.NewConsumer(provider.cfg)

		if err != nil {
			return err
		}

		entry = &queueEntry{
			consumer: consumer,
			handlers: make(map[string]rabbitmq.MessageHandler),
		}

		provider.queues[queue] = entry
	}

	if err := entry.consumer.Bind(queue, exchange, exchangeType, routingKey); err != nil {
		return err
	}

	entry.handlers[routingKey] = handler

	return nil
}

// StartAll begins consuming all registered queues in their own goroutines.
func (provider *RabbitMQRegister) StartAll(ctx context.Context) error {
	for queueName, entry := range provider.queues {
		err := entry.consumer.Consume(ctx, queueName, func(delivery amqp.Delivery) error {
			handler, ok := entry.handlers[delivery.RoutingKey]

			if !ok {
				slog.Warn(
					"no handler registered for routing key",
					"routing_key",
					delivery.RoutingKey,
				)

				return nil
			}

			slog.Info(
				"message received from rabbitmq",
				"routing_key",
				delivery.RoutingKey,
			)

			return handler.Handle(delivery.Body)
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// Close shuts down all underlying consumers.
func (provider *RabbitMQRegister) Close() error {
	for _, entry := range provider.queues {
		if err := entry.consumer.Close(); err != nil {
			return err
		}
	}
	return nil
}
