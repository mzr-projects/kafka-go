package model

import (
	"fmt"
	"time"
)

// Order is the JSON document published to the orders topic.
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	SKU       string    `json:"sku"`
	Quantity  int       `json:"quantity"`
	TotalUSD  float64   `json:"total_usd"`
	CreatedAt time.Time `json:"created_at"`
}

// Payment is the JSON document published to the payments topic.
// OrderID links a payment back to the order that caused it.
type Payment struct {
	ID        string    `json:"id"`
	OrderID   string    `json:"order_id"`
	AmountUSD float64   `json:"amount_usd"`
	Method    string    `json:"method"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type catalogItem struct {
	customer string
	sku      string
	price    float64
}

var catalog = []catalogItem{
	{customer: "Ada Lovelace", sku: "notebook", price: 12.50},
	{customer: "Alan Turing", sku: "keyboard", price: 79.00},
	{customer: "Grace Hopper", sku: "compiler-pin", price: 8.00},
}

var methods = []string{"card", "bank", "wallet"}

// NewPair builds an order and the payment that settles it.
// seq starts at 1 and is used only to vary the sample data.
func NewPair(seq int, at time.Time) (Order, Payment) {
	item := catalog[seq%len(catalog)]
	quantity := (seq % 3) + 1
	total := roundCents(item.price * float64(quantity))

	order := Order{
		ID:        fmt.Sprintf("ord-%04d", seq),
		Customer:  item.customer,
		SKU:       item.sku,
		Quantity:  quantity,
		TotalUSD:  total,
		CreatedAt: at.UTC(),
	}
	payment := Payment{
		ID:        fmt.Sprintf("pay-%04d", seq),
		OrderID:   order.ID,
		AmountUSD: total,
		Method:    methods[seq%len(methods)],
		Status:    "authorized",
		CreatedAt: at.UTC(),
	}
	return order, payment
}

func roundCents(amount float64) float64 {
	return float64(int(amount*100+0.5)) / 100
}
