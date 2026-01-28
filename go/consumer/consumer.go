package consumer

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/example/custom-kafka-lib/config"
	"github.com/example/custom-kafka-lib/errors"
	"github.com/example/custom-kafka-lib/registry"
)

// Consumer is a high-level Kafka consumer with schema support.
type Consumer struct {
	config          *config.Config
	consumer        *kafka.Consumer
	registryManager *registry.Manager
	messageHandler  MessageHandler
	errorHandler    ErrorHandler
	fatalHandler    FatalErrorHandler
	httpForwarder   *HTTPForwarder
	running         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	mu              sync.Mutex
}

// NewConsumer creates a new Kafka consumer with the given configuration.
func NewConsumer(cfg *config.Config) (*Consumer, error) {
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":       cfg.BootstrapServers,
		"group.id":                cfg.GroupID,
		"auto.offset.reset":       cfg.AutoOffsetReset,
		"enable.auto.commit":      cfg.EnableAutoCommit,
		"auto.commit.interval.ms": cfg.AutoCommitIntervalMs,
		"max.poll.interval.ms":    300000,
	}

	// Add additional properties
	for k, v := range cfg.AdditionalProperties {
		if err := kafkaConfig.SetKey(k, v); err != nil {
			return nil, errors.NewConsumerError("failed to set kafka config "+k, "", err)
		}
	}

	c, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, errors.NewConsumerError("failed to create Kafka consumer", "", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	consumer := &Consumer{
		config:          cfg,
		consumer:        c,
		registryManager: registry.NewManager(cfg),
		errorHandler:    DefaultErrorHandler,
		fatalHandler:    DefaultFatalErrorHandler,
		ctx:             ctx,
		cancel:          cancel,
	}

	log.Printf("KafkaConsumer initialized with group ID: %s", cfg.GroupID)
	return consumer, nil
}

// Subscribe subscribes to topics with a message handler.
func (c *Consumer) Subscribe(topics []string, handler MessageHandler) error {
	c.messageHandler = handler
	err := c.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return errors.NewConsumerError("failed to subscribe to topics", "", err)
	}
	log.Printf("Subscribed to topics: %v", topics)
	return nil
}

// SubscribeSingle subscribes to a single topic with a message handler.
func (c *Consumer) SubscribeSingle(topic string, handler MessageHandler) error {
	return c.Subscribe([]string{topic}, handler)
}

// ForwardTo configures the consumer to forward messages to an HTTP endpoint.
func (c *Consumer) ForwardTo(forwarder *HTTPForwarder) *Consumer {
	c.httpForwarder = forwarder
	return c
}

// WithErrorHandler sets a custom error handler.
func (c *Consumer) WithErrorHandler(handler ErrorHandler) *Consumer {
	c.errorHandler = handler
	return c
}

// WithFatalErrorHandler sets a custom fatal error handler.
func (c *Consumer) WithFatalErrorHandler(handler FatalErrorHandler) *Consumer {
	c.fatalHandler = handler
	return c
}

// StartAsync starts consuming messages in a background goroutine.
func (c *Consumer) StartAsync() error {
	c.mu.Lock()
	if c.messageHandler == nil && c.httpForwarder == nil {
		c.mu.Unlock()
		return errors.NewConsumerError("no message handler configured", "", nil)
	}
	c.running = true
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeLoop()
	}()

	log.Println("Consumer started in async mode")
	return nil
}

// Start starts consuming messages (blocking).
func (c *Consumer) Start() error {
	c.mu.Lock()
	if c.messageHandler == nil && c.httpForwarder == nil {
		c.mu.Unlock()
		return errors.NewConsumerError("no message handler configured", "", nil)
	}
	c.running = true
	c.mu.Unlock()

	c.consumeLoop()
	return nil
}

func (c *Consumer) consumeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Println("Consumer loop ended (context cancelled)")
			return
		default:
			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Timeout is not an error
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error reading message: %v", err)
				continue
			}

			c.processMessage(msg)

			// Manual commit if auto-commit is disabled
			if !c.config.EnableAutoCommit {
				_, err := c.consumer.CommitMessage(msg)
				if err != nil {
					log.Printf("Error committing message: %v", err)
				}
			}
		}
	}
}

func (c *Consumer) processMessage(kafkaMsg *kafka.Message) {
	// Convert headers
	headers := make(map[string]string)
	for _, h := range kafkaMsg.Headers {
		headers[h.Key] = string(h.Value)
	}

	// Parse value as JSON
	var value interface{}
	if err := json.Unmarshal(kafkaMsg.Value, &value); err != nil {
		// If not JSON, use raw string
		value = string(kafkaMsg.Value)
	}

	msg := &Message{
		Topic:     *kafkaMsg.TopicPartition.Topic,
		Partition: kafkaMsg.TopicPartition.Partition,
		Offset:    int64(kafkaMsg.TopicPartition.Offset),
		Timestamp: kafkaMsg.Timestamp,
		Key:       string(kafkaMsg.Key),
		Value:     value,
		Headers:   headers,
	}

	// Call message handler if configured
	if c.messageHandler != nil {
		if err := c.messageHandler(msg); err != nil {
			c.errorHandler(msg, err)
		}
	}

	// Forward to HTTP endpoint if configured
	if c.httpForwarder != nil {
		if err := c.httpForwarder.Forward(msg); err != nil {
			c.errorHandler(msg, err)
		}
	}

	log.Printf("Processed message from topic '%s', partition %d, offset %d",
		msg.Topic, msg.Partition, msg.Offset)
}

// Poll polls for messages once (for manual control).
func (c *Consumer) Poll(timeout time.Duration) ([]*Message, error) {
	var messages []*Message

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			return messages, err
		}

		// Convert headers
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}

		// Parse value as JSON
		var value interface{}
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			value = string(msg.Value)
		}

		messages = append(messages, &Message{
			Topic:     *msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
			Timestamp: msg.Timestamp,
			Key:       string(msg.Key),
			Value:     value,
			Headers:   headers,
		})
	}

	return messages, nil
}

// Commit commits the current offsets.
func (c *Consumer) Commit() error {
	_, err := c.consumer.Commit()
	return err
}

// Stop stops the consumer gracefully.
func (c *Consumer) Stop() {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()
	log.Println("Consumer stopped")
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	c.Stop()

	if c.httpForwarder != nil {
		c.httpForwarder.Close()
	}

	err := c.consumer.Close()
	if err != nil {
		return errors.NewConsumerError("failed to close consumer", "", err)
	}

	log.Println("KafkaConsumer closed")
	return nil
}

// IsRunning returns whether the consumer is running.
func (c *Consumer) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

