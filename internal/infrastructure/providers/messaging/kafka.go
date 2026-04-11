package messaging

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type MessageHandler interface {
	Handle(body []byte) error
}

type topicEntry struct {
	topic   string
	handler MessageHandler
}

type KafkaConsumer struct {
	brokers []string
	groupID string
	entries []topicEntry
	client  *kgo.Client
}

func NewKafkaConsumer(brokers string) *KafkaConsumer {
	return &KafkaConsumer{brokers: []string{brokers}}
}

func (consumer *KafkaConsumer) Register(queue, _, _, routingKey string, handler MessageHandler) error {
	if consumer.groupID == "" {
		consumer.groupID = queue
	}
	consumer.entries = append(consumer.entries, topicEntry{topic: routingKey, handler: handler})
	return nil
}

func (consumer *KafkaConsumer) StartAll(ctx context.Context) error {
	topics := make([]string, 0, len(consumer.entries))

	for _, e := range consumer.entries {
		topics = append(topics, e.topic)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(consumer.brokers...),
		kgo.ConsumerGroup(consumer.groupID),
		kgo.ConsumeTopics(topics...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return err
	}
	consumer.client = client

	handlers := make(map[string]MessageHandler, len(consumer.entries))

	for _, entries := range consumer.entries {
		handlers[entries.topic] = entries.handler
	}

	go func() {
		for {
			fetches := consumer.client.PollFetches(ctx)

			if ctx.Err() != nil {
				return
			}

			errs := fetches.Errors()

			if len(errs) > 0 {
				for _, e := range errs {
					slog.Error("kafka fetch error", "error", e.Err)
				}
				continue
			}

			fetches.EachRecord(func(record *kgo.Record) {
				slog.Info("message received from kafka", "topic", record.Topic)
				if handler, ok := handlers[record.Topic]; ok {
					err = handler.Handle(record.Value)

					if err != nil {
						slog.Error("handler error", "error", err)
					}
				}
			})
		}
	}()

	return nil
}

func (consumer *KafkaConsumer) Close() error {
	if consumer.client != nil {
		consumer.client.Close()
	}
	return nil
}
