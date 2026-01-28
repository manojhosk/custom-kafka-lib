// Package consumer provides a high-level Kafka consumer with schema support.
package consumer

import (
	"time"
)

// Message represents a consumed message from Kafka.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       string
	Value     interface{}
	Headers   map[string]string
}

// NewMessage creates a new Message.
func NewMessage(topic string, partition int32, offset int64, timestamp time.Time, key string, value interface{}, headers map[string]string) *Message {
	return &Message{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}
}

// MessageHandler is a function type for handling consumed messages.
type MessageHandler func(msg *Message) error

// ErrorHandler is a function type for handling errors during consumption.
type ErrorHandler func(msg *Message, err error)

// FatalErrorHandler is a function type for handling fatal errors.
type FatalErrorHandler func(err error)

// DefaultErrorHandler is the default error handler that logs errors.
func DefaultErrorHandler(msg *Message, err error) {
	if msg != nil {
		println("Error processing message from topic", msg.Topic, ":", err.Error())
	} else {
		println("Error processing message:", err.Error())
	}
}

// DefaultFatalErrorHandler is the default fatal error handler that logs and panics.
func DefaultFatalErrorHandler(err error) {
	println("Fatal consumer error:", err.Error())
}

