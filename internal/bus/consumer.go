package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Handler decodes one record. The raw JSON value is passed through unchanged.
type Handler func(value []byte) error

// Consume reads topic until ctx is canceled. A canceled context is a clean stop.
func Consume(ctx context.Context, broker, topic, groupID string, handle Handler) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("read %s: %w", topic, err)
		}
		if err := handle(msg.Value); err != nil {
			return fmt.Errorf("handle %s: %w", topic, err)
		}
	}
}

// ConsumeTopics runs one reader per topic. The first handler or read error
// cancels the remaining readers.
func ConsumeTopics(ctx context.Context, broker, groupID string, handlers map[string]Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(handlers))
	for topic, handle := range handlers {
		wg.Add(1)
		go func(topic string, handle Handler) {
			defer wg.Done()
			// Each topic gets its own group. One shared group would require
			// every member to subscribe to the same topics.
			group := groupID + "-" + topic
			if err := Consume(ctx, broker, topic, group, handle); err != nil {
				errCh <- err
				cancel()
			}
		}(topic, handle)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
