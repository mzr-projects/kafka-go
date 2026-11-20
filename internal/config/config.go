package config

import "os"

const (
	// TopicOrders carries Order JSON documents.
	TopicOrders = "orders"
	// TopicPayments carries Payment JSON documents.
	TopicPayments = "payments"
	// GroupID is the consumer-group prefix. Each topic appends its own name
	// so the two readers do not join a single subscription.
	GroupID = "json-demo"
)

// Broker is the Kafka bootstrap address. Override it with KAFKA_BROKER.
func Broker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return "localhost:9092"
}
