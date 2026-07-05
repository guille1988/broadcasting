package messaging

import (
	"context"
	"log/slog"
	"sync"
	"time"

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
	brokers          []string
	groupID          string
	entries          []topicEntry
	client           *kgo.Client
	rebalanceTimeout time.Duration
	workerPoolSize   int
}

func NewKafkaConsumer(brokers string, rebalanceTimeoutMs int, workerPoolSize int) *KafkaConsumer {
	return &KafkaConsumer{
		brokers:          []string{brokers},
		rebalanceTimeout: time.Duration(rebalanceTimeoutMs) * time.Millisecond,
		workerPoolSize:   workerPoolSize,
	}
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
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(consumer.rebalanceTimeout),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
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
				consumer.client.AllowRebalance()
				return
			}

			errs := fetches.Errors()

			if len(errs) > 0 {
				for _, e := range errs {
					slog.Error("kafka fetch error", "error", e.Err)
				}
				consumer.client.AllowRebalance()
				continue
			}

			sem := make(chan struct{}, consumer.workerPoolSize)
			var waitGroup sync.WaitGroup

			fetches.EachRecord(func(record *kgo.Record) {
				waitGroup.Add(1)
				sem <- struct{}{}

				go func(rec *kgo.Record) {
					defer waitGroup.Done()
					defer func() { <-sem }()

					slog.Info("message received from kafka", "topic", rec.Topic)

					if handler, ok := handlers[rec.Topic]; ok {
						if handlerErr := handler.Handle(rec.Value); handlerErr != nil {
							slog.Error("handler error", "error", handlerErr)
						}
					}
				}(record)
			})
			waitGroup.Wait()

			if commitErr := consumer.client.CommitUncommittedOffsets(ctx); commitErr != nil {
				slog.Error("failed to commit offsets", "error", commitErr)
			}
			consumer.client.AllowRebalance()
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
