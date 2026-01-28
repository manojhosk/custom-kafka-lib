// Example demonstrating HTTP forwarding functionality.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/custom-kafka-lib/config"
	"github.com/example/custom-kafka-lib/consumer"
)

func main() {
	// Create configuration
	cfg := config.NewBuilder().
		BootstrapServers("localhost:9092").
		SchemaRegistryURL("http://localhost:8081").
		GroupID("http-forwarder-group").
		AutoOffsetReset("earliest").
		EnableAutoCommit(true).
		Build()

	// Create consumer
	c, err := consumer.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Create HTTP forwarder (e.g., to Elasticsearch)
	forwarder := consumer.NewHTTPForwarder("http://localhost:9200/users/_doc").
		MaxRetries(3).
		RetryDelay(1000).
		Header("Content-Type", "application/json").
		ConnectTimeout(30 * time.Second).
		Build()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down consumer...")
		c.Stop()
	}()

	// Subscribe with both handler and HTTP forwarding
	err = c.SubscribeSingle("users", func(msg *consumer.Message) error {
		fmt.Printf("Processing message: %v\n", msg.Value)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Configure HTTP forwarding
	c.ForwardTo(forwarder)

	// Start consuming
	log.Println("Consumer started with HTTP forwarding...")
	if err := c.Start(); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	// Close consumer
	if err := c.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
}

