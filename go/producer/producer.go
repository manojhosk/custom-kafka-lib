// Package producer provides a high-level Kafka producer with schema validation.
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/example/custom-kafka-lib/config"
	"github.com/example/custom-kafka-lib/errors"
	"github.com/example/custom-kafka-lib/registry"
	"github.com/example/custom-kafka-lib/schema"
)

// SendResult holds the result of a send operation.
type SendResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Timestamp time.Time
}

// Producer is a high-level Kafka producer with schema validation.
type Producer struct {
	config          *config.Config
	producer        *kafka.Producer
	registryManager *registry.Manager
	validator       schema.Validator
	schemaCache     map[string]string
	schemaCacheMu   sync.RWMutex
	closed          bool
	mu              sync.Mutex
}

// NewProducer creates a new Kafka producer with the given configuration.
func NewProducer(cfg *config.Config) (*Producer, error) {
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":   cfg.BootstrapServers,
		"acks":                cfg.Acks,
		"retries":             cfg.Retries,
		"retry.backoff.ms":    cfg.RetryBackoffMs,
		"delivery.timeout.ms": cfg.DeliveryTimeoutMs,
		"enable.idempotence":  cfg.EnableIdempotence,
	}

	// Add additional properties
	for k, v := range cfg.AdditionalProperties {
		if err := kafkaConfig.SetKey(k, v); err != nil {
			return nil, fmt.Errorf("failed to set kafka config %s: %w", k, err)
		}
	}

	p, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, errors.NewProducerError("failed to create Kafka producer", "", err)
	}

	// Create validator based on schema type
	factory := schema.NewValidatorFactory()
	validator := factory.Create(string(cfg.SchemaType))

	producer := &Producer{
		config:          cfg,
		producer:        p,
		registryManager: registry.NewManager(cfg),
		validator:       validator,
		schemaCache:     make(map[string]string),
	}

	log.Printf("KafkaProducer initialized with bootstrap servers: %s", cfg.BootstrapServers)
	return producer, nil
}

// RegisterSchema registers a schema for a topic.
func (p *Producer) RegisterSchema(topic, schemaStr string) (int, error) {
	subject := registry.ValueSubject(topic)
	schemaID, err := p.registryManager.RegisterSchema(subject, schemaStr)
	if err != nil {
		return 0, err
	}

	p.schemaCacheMu.Lock()
	p.schemaCache[topic] = schemaStr
	p.schemaCacheMu.Unlock()

	log.Printf("Registered schema for topic '%s' with ID: %d", topic, schemaID)
	return schemaID, nil
}

// Send sends a message to Kafka synchronously.
func (p *Producer) Send(topic, key string, value interface{}) (*SendResult, error) {
	return p.SendWithHeaders(topic, key, value, nil)
}

// SendWithHeaders sends a message to Kafka synchronously with headers.
func (p *Producer) SendWithHeaders(topic, key string, value interface{}, headers map[string]string) (*SendResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.GetDeliveryTimeout())
	defer cancel()
	return p.SendWithContext(ctx, topic, key, value, headers)
}

// SendWithContext sends a message to Kafka with a context for cancellation.
func (p *Producer) SendWithContext(ctx context.Context, topic, key string, value interface{}, headers map[string]string) (*SendResult, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.NewProducerError("producer is closed", topic, nil)
	}
	p.mu.Unlock()

	// Validate against schema
	if err := p.validateMessage(topic, value); err != nil {
		return nil, err
	}

	// Convert value to JSON bytes
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, errors.NewProducerError("failed to marshal value to JSON", topic, err)
	}

	// Convert headers
	var kafkaHeaders []kafka.Header
	if headers != nil {
		kafkaHeaders = make([]kafka.Header, 0, len(headers))
		for k, v := range headers {
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
		}
	}

	// Create Kafka message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:   valueBytes,
		Headers: kafkaHeaders,
	}

	if key != "" {
		msg.Key = []byte(key)
	}

	// Delivery channel
	deliveryChan := make(chan kafka.Event)

	// Produce the message
	if err := p.producer.Produce(msg, deliveryChan); err != nil {
		return nil, errors.NewProducerError("failed to produce message", topic, err)
	}

	// Wait for delivery
	select {
	case <-ctx.Done():
		return nil, errors.NewProducerError("context cancelled while waiting for delivery", topic, ctx.Err())
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return nil, errors.NewProducerError("failed to deliver message", topic, m.TopicPartition.Error)
		}
		return &SendResult{
			Topic:     *m.TopicPartition.Topic,
			Partition: m.TopicPartition.Partition,
			Offset:    int64(m.TopicPartition.Offset),
			Key:       key,
			Timestamp: m.Timestamp,
		}, nil
	}
}

// SendAsync sends a message to Kafka asynchronously.
func (p *Producer) SendAsync(topic, key string, value interface{}, callback func(*SendResult, error)) error {
	return p.SendAsyncWithHeaders(topic, key, value, nil, callback)
}

// SendAsyncWithHeaders sends a message to Kafka asynchronously with headers.
func (p *Producer) SendAsyncWithHeaders(topic, key string, value interface{}, headers map[string]string, callback func(*SendResult, error)) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.NewProducerError("producer is closed", topic, nil)
	}
	p.mu.Unlock()

	// Validate against schema
	if err := p.validateMessage(topic, value); err != nil {
		if callback != nil {
			callback(nil, err)
		}
		return err
	}

	// Convert value to JSON bytes
	valueBytes, err := json.Marshal(value)
	if err != nil {
		err := errors.NewProducerError("failed to marshal value to JSON", topic, err)
		if callback != nil {
			callback(nil, err)
		}
		return err
	}

	// Convert headers
	var kafkaHeaders []kafka.Header
	if headers != nil {
		kafkaHeaders = make([]kafka.Header, 0, len(headers))
		for k, v := range headers {
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
		}
	}

	// Create Kafka message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:   valueBytes,
		Headers: kafkaHeaders,
	}

	if key != "" {
		msg.Key = []byte(key)
	}

	// Start goroutine to handle delivery
	go func() {
		deliveryChan := make(chan kafka.Event)
		if err := p.producer.Produce(msg, deliveryChan); err != nil {
			if callback != nil {
				callback(nil, errors.NewProducerError("failed to produce message", topic, err))
			}
			return
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			if callback != nil {
				callback(nil, errors.NewProducerError("failed to deliver message", topic, m.TopicPartition.Error))
			}
			return
		}

		if callback != nil {
			callback(&SendResult{
				Topic:     *m.TopicPartition.Topic,
				Partition: m.TopicPartition.Partition,
				Offset:    int64(m.TopicPartition.Offset),
				Key:       key,
				Timestamp: m.Timestamp,
			}, nil)
		}
	}()

	return nil
}

// validateMessage validates a message against the registered schema.
func (p *Producer) validateMessage(topic string, value interface{}) error {
	p.schemaCacheMu.RLock()
	schemaStr, ok := p.schemaCache[topic]
	p.schemaCacheMu.RUnlock()

	if !ok {
		return errors.NewSchemaValidationError(
			"no schema registered for topic",
			"",
			fmt.Sprintf("topic '%s' has no registered schema", topic),
		)
	}

	return p.validator.Validate(value, schemaStr)
}

// Flush waits for all messages to be delivered.
func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

// Close closes the producer.
func (p *Producer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	// Flush remaining messages
	p.producer.Flush(15 * 1000)
	p.producer.Close()
	p.closed = true
	log.Println("KafkaProducer closed")
}

