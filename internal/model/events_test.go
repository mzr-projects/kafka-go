package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewPairLinksPaymentToOrder(t *testing.T) {
	at := time.Date(2026, 11, 20, 17, 0, 0, 0, time.UTC)
	order, payment := NewPair(1, at)

	if payment.OrderID != order.ID {
		t.Fatalf("payment.OrderID = %q, want %q", payment.OrderID, order.ID)
	}
	if payment.AmountUSD != order.TotalUSD {
		t.Fatalf("payment.AmountUSD = %v, want %v", payment.AmountUSD, order.TotalUSD)
	}
	if order.Quantity < 1 {
		t.Fatalf("quantity = %d, want at least 1", order.Quantity)
	}
	if order.ID != "ord-0001" || payment.ID != "pay-0001" {
		t.Fatalf("ids = %s %s, want ord-0001 pay-0001", order.ID, payment.ID)
	}
}

func TestPairJSONRoundTrip(t *testing.T) {
	order, payment := NewPair(2, time.Date(2026, 11, 20, 17, 0, 0, 0, time.UTC))

	orderBody, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}
	paymentBody, err := json.Marshal(payment)
	if err != nil {
		t.Fatalf("marshal payment: %v", err)
	}

	var decodedOrder Order
	if err := json.Unmarshal(orderBody, &decodedOrder); err != nil {
		t.Fatalf("unmarshal order: %v", err)
	}
	var decodedPayment Payment
	if err := json.Unmarshal(paymentBody, &decodedPayment); err != nil {
		t.Fatalf("unmarshal payment: %v", err)
	}

	if decodedOrder != order {
		t.Fatalf("order round trip = %+v, want %+v", decodedOrder, order)
	}
	if decodedPayment != payment {
		t.Fatalf("payment round trip = %+v, want %+v", decodedPayment, payment)
	}
}
