package rabbitmq

import (
	"broadcasting/internal/infrastructure/config"
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler processes a raw RabbitMQ message body.
type MessageHandler interface {
	Handle(body []byte) error
}

// Consumer wraps a single AMQP connection and channel.
type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewConsumer dials RabbitMQ and opens a channel, closing both on failure.
func NewConsumer(cfg config.RabbitMQConfig) (*Consumer, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", cfg.User, cfg.Password, cfg.Host, cfg.Port)

	const maxRetries = 5
	backoff := 2 * time.Second

	var connection *amqp.Connection
	var err error

	for i := 1; i <= maxRetries; i++ {
		connection, err = amqp.Dial(url)

		if err == nil {
			break
		}

		slog.Warn("failed to connect to RabbitMQ, retrying...", "attempt", i, "error", err)
		time.Sleep(backoff)
		backoff *= 2
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w", maxRetries, err)
	}

	var channel *amqp.Channel
	var ok bool

	defer func() {
		if !ok {
			if channel != nil {
				_ = channel.Close()
			}
			_ = connection.Close()
		}
	}()

	channel, err = connection.Channel()

	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	err = channel.Qos(cfg.PrefetchCount, 0, false)

	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	ok = true

	return &Consumer{
		connection: connection,
		channel:    channel,
	}, nil
}

// Bind declares the exchange, queue, and binding for the given routing key.
func (consumer *Consumer) Bind(queue, exchange, exchangeType, routingKey string) error {
	err := consumer.channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	_, err = consumer.channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	return consumer.channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
}

// Consume starts a non-blocking goroutine that feeds deliveries to handler.
func (consumer *Consumer) Consume(ctx context.Context, queue string, handler func(delivery amqp.Delivery) error) error {
	messages, err := consumer.channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case delivery, ok := <-messages:
				if !ok {
					return
				}

				err = handler(delivery)

				if err != nil {
					slog.Error("failed to handle message", "error", err)

					_ = delivery.Nack(false, true)
					continue
				}

				_ = delivery.Ack(false)
			}
		}
	}()

	slog.Info("consumer started", "queue", queue)

	return nil
}

// Close shuts down the channel and connection.
func (consumer *Consumer) Close() error {
	err := consumer.channel.Close()

	if err != nil {
		return err
	}

	return consumer.connection.Close()
}
