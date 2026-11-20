package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher writes JSON documents to any topic on a single broker.
type Publisher struct {
	writer *kafka.Writer
}

// NewPublisher opens a writer. Topics are created on first publish when the
// broker allows auto topic creation.
func NewPublisher(broker string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			// Flush each JSON document immediately so the demo is visible.
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Close flushes pending records and releases the writer.
func (p *Publisher) Close() error {
	return p.writer.Close()
}

// PublishJSON marshals payload and writes it to topic, keyed by key.
// The key keeps related records on the same partition.
func (p *Publisher) PublishJSON(ctx context.Context, topic, key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s: %w", topic, err)
	}
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	})
	if err != nil {
		return fmt.Errorf("write %s: %w", topic, err)
	}
	return nil
}
