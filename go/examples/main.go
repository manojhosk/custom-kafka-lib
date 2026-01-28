// Example application demonstrating the Custom Kafka library usage.
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
	"github.com/example/custom-kafka-lib/producer"
)

// User schema (Avro)
const userSchema = `{
	"type": "record",
	"name": "User",
	"namespace": "com.example.kafka.examples",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "email", "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "createdAt", "type": "long"}
	]
}`

func main() {
	// Choose which example to run
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [producer|consumer|both]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	case "both":
		go runConsumer()
		time.Sleep(2 * time.Second) // Wait for consumer to start
		runProducer()
		// Keep running to let consumer process messages
		time.Sleep(5 * time.Second)
	default:
		fmt.Println("Unknown command:", os.Args[1])
		os.Exit(1)
	}
}

func runProducer() {
	// Create configuration
	cfg := config.NewBuilder().
		BootstrapServers("localhost:9092").
		SchemaRegistryURL("http://localhost:8081").
		WithSchemaType(config.SchemaTypeAvro).
		Build()

	// Create producer
	p, err := producer.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	// Register schema
	schemaID, err := p.RegisterSchema("users", userSchema)
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	log.Printf("Schema registered with ID: %d", schemaID)

	// Send messages
	for i := 1; i <= 5; i++ {
		user := map[string]interface{}{
			"id":        fmt.Sprintf("user-%d", i),
			"email":     fmt.Sprintf("user%d@example.com", i),
			"name":      fmt.Sprintf("User %d", i),
			"createdAt": time.Now().UnixMilli(),
		}

		result, err := p.Send("users", user["id"].(string), user)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}

		log.Printf("Message sent to topic '%s', partition %d, offset %d",
			result.Topic, result.Partition, result.Offset)
	}

	log.Println("Producer finished sending messages")
}

func runConsumer() {
	// Create configuration
	cfg := config.NewBuilder().
		BootstrapServers("localhost:9092").
		SchemaRegistryURL("http://localhost:8081").
		GroupID("example-consumer-group").
		AutoOffsetReset("earliest").
		EnableAutoCommit(true).
		Build()

	// Create consumer
	c, err := consumer.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down consumer...")
		c.Stop()
	}()

	// Subscribe with message handler
	err = c.SubscribeSingle("users", func(msg *consumer.Message) error {
		log.Printf("Received message: key=%s, value=%v", msg.Key, msg.Value)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Start consuming (blocking)
	log.Println("Consumer started, waiting for messages...")
	if err := c.Start(); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	// Close consumer
	if err := c.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
}

