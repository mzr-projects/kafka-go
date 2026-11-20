package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka/internal/bus"
	"kafka/internal/config"
	"kafka/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "produce":
		err = produce(ctx, os.Args[2:])
	case "consume":
		err = consume(ctx)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		slog.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Publish and read JSON on the %q and %q topics.

Usage:
  go run . produce [-n 5]   publish order/payment pairs (0 runs until interrupted)
  go run . consume          read both topics until interrupted

Broker: %s (override with KAFKA_BROKER)
Start the broker with: docker compose up -d
`, config.TopicOrders, config.TopicPayments, config.Broker())
}

func produce(ctx context.Context, args []string) error {
	count := 5
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	fs.IntVar(&count, "n", 5, "number of order/payment pairs to publish; 0 runs until interrupted")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if count < 0 {
		return fmt.Errorf("-n must be >= 0")
	}

	publisher := bus.NewPublisher(config.Broker())
	defer publisher.Close()

	slog.Info("producing", "broker", config.Broker(), "orders", config.TopicOrders, "payments", config.TopicPayments, "count", count)

	for seq := 1; count == 0 || seq <= count; seq++ {
		order, payment := model.NewPair(seq, time.Now())
		// Key both records by the order id so a given order stays on one partition.
		if err := publisher.PublishJSON(ctx, config.TopicOrders, order.ID, order); err != nil {
			return ignoreCancel(err)
		}
		if err := publisher.PublishJSON(ctx, config.TopicPayments, order.ID, payment); err != nil {
			return ignoreCancel(err)
		}
		slog.Info("published", "order_id", order.ID, "payment_id", payment.ID, "total_usd", order.TotalUSD)

		if count != 0 && seq == count {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}

func consume(ctx context.Context) error {
	slog.Info("consuming",
		"broker", config.Broker(),
		"orders_group", config.GroupID+"-"+config.TopicOrders,
		"payments_group", config.GroupID+"-"+config.TopicPayments,
	)

	handlers := map[string]bus.Handler{
		config.TopicOrders:   printJSON[model.Order](config.TopicOrders),
		config.TopicPayments: printJSON[model.Payment](config.TopicPayments),
	}
	return bus.ConsumeTopics(ctx, config.Broker(), config.GroupID, handlers)
}

func ignoreCancel(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func printJSON[T any](topic string) bus.Handler {
	return func(value []byte) error {
		var event T
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("decode json: %w", err)
		}
		pretty, err := json.MarshalIndent(event, "", "  ")
		if err != nil {
			return fmt.Errorf("format json: %w", err)
		}
		fmt.Printf("topic=%s\n%s\n\n", topic, pretty)
		return nil
	}
}
