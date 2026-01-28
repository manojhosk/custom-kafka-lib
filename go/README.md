# Custom Kafka Library - Go Version

A standardized Go library for producing and consuming Kafka messages with schema validation, designed to provide a seamless developer experience for teams without deep Kafka expertise.

## Features

- **Schema Validation**: Supports both Avro and JSON Schema
- **Schema Registry Integration**: Register and retrieve schemas from Confluent Schema Registry
- **High-Level Producer API**: Simple API for sending schema-validated messages
- **High-Level Consumer API**: Easy message consumption with handlers
- **HTTP Forwarding**: Forward consumed messages to REST APIs (e.g., Elasticsearch)
- **Fluent Configuration**: Builder pattern for easy configuration

## Installation

```bash
go get github.com/example/custom-kafka-lib
```

## Quick Start

### Start Infrastructure

```bash
cd ../docker && docker-compose up -d
```

This starts Kafka (port 9092) and Schema Registry (port 8081).

### Producer Example

```go
package main

import (
    "log"
    "time"

    "github.com/example/custom-kafka-lib/config"
    "github.com/example/custom-kafka-lib/producer"
)

const userSchema = `{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "createdAt", "type": "long"}
    ]
}`

func main() {
    // Create configuration
    cfg := config.NewBuilder().
        BootstrapServers("localhost:9092").
        SchemaRegistryURL("http://localhost:8081").
        WithSchemaType(config.SchemaTypeAvro).
        Build()

    // Create producer
    p, err := producer.NewProducer(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    // Register schema (enforces validation)
    p.RegisterSchema("users", userSchema)

    // Send message - automatically validated against schema
    user := map[string]interface{}{
        "id":        "123",
        "email":     "user@example.com",
        "name":      "John Doe",
        "createdAt": time.Now().UnixMilli(),
    }

    result, err := p.Send("users", "123", user)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Message sent: partition=%d, offset=%d", result.Partition, result.Offset)
}
```

### Consumer Example

```go
package main

import (
    "log"

    "github.com/example/custom-kafka-lib/config"
    "github.com/example/custom-kafka-lib/consumer"
)

func main() {
    // Create configuration
    cfg := config.NewBuilder().
        BootstrapServers("localhost:9092").
        SchemaRegistryURL("http://localhost:8081").
        GroupID("my-consumer-group").
        AutoOffsetReset("earliest").
        Build()

    // Create consumer
    c, err := consumer.NewConsumer(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Subscribe with message handler
    c.SubscribeSingle("users", func(msg *consumer.Message) error {
        log.Printf("Received: key=%s, value=%v", msg.Key, msg.Value)
        return nil
    })

    // Start consuming (blocking)
    c.Start()
}
```

### HTTP Forwarding Example

```go
package main

import (
    "time"

    "github.com/example/custom-kafka-lib/config"
    "github.com/example/custom-kafka-lib/consumer"
)

func main() {
    cfg := config.NewBuilder().
        BootstrapServers("localhost:9092").
        SchemaRegistryURL("http://localhost:8081").
        GroupID("forwarder-group").
        Build()

    c, _ := consumer.NewConsumer(cfg)
    defer c.Close()

    // Configure HTTP forwarding (e.g., to Elasticsearch)
    forwarder := consumer.NewHTTPForwarder("http://localhost:9200/users/_doc").
        MaxRetries(3).
        RetryDelay(1000).
        ConnectTimeout(30 * time.Second).
        Build()

    c.SubscribeSingle("users", nil)
    c.ForwardTo(forwarder)
    c.StartAsync()

    // Keep running...
    select {}
}
```

## API Reference

### Configuration

```go
cfg := config.NewBuilder().
    BootstrapServers("localhost:9092").      // Kafka bootstrap servers
    SchemaRegistryURL("http://localhost:8081"). // Schema Registry URL
    WithSchemaType(config.SchemaTypeAvro).   // AVRO or JSON
    GroupID("my-group").                      // Consumer group ID
    Acks("all").                              // Producer acks
    Retries(3).                               // Producer retries
    EnableIdempotence(true).                  // Idempotent producer
    AutoOffsetReset("earliest").              // Consumer offset reset
    EnableAutoCommit(true).                   // Auto commit
    Build()
```

### Producer

| Method | Description |
|--------|-------------|
| `NewProducer(cfg)` | Create a new producer |
| `RegisterSchema(topic, schema)` | Register a schema for a topic |
| `Send(topic, key, value)` | Send a message synchronously |
| `SendWithHeaders(topic, key, value, headers)` | Send with headers |
| `SendAsync(topic, key, value, callback)` | Send asynchronously |
| `Flush(timeoutMs)` | Flush pending messages |
| `Close()` | Close the producer |

### Consumer

| Method | Description |
|--------|-------------|
| `NewConsumer(cfg)` | Create a new consumer |
| `Subscribe(topics, handler)` | Subscribe to topics |
| `SubscribeSingle(topic, handler)` | Subscribe to a single topic |
| `ForwardTo(forwarder)` | Configure HTTP forwarding |
| `Start()` | Start consuming (blocking) |
| `StartAsync()` | Start consuming in background |
| `Poll(timeout)` | Poll for messages manually |
| `Stop()` | Stop the consumer |
| `Close()` | Close the consumer |

### Schema Validators

| Validator | Description |
|-----------|-------------|
| `AvroValidator` | Validates against Avro schemas |
| `JSONValidator` | Validates against JSON Schema (Draft 7) |

## Project Structure

```
go/
├── config/
│   └── config.go         # Configuration and builder
├── consumer/
│   ├── consumer.go       # High-level consumer
│   ├── http_forwarder.go # HTTP forwarding
│   └── message.go        # Message types
├── errors/
│   └── errors.go         # Custom error types
├── producer/
│   └── producer.go       # High-level producer
├── registry/
│   └── manager.go        # Schema Registry client
├── schema/
│   ├── avro_validator.go # Avro validation
│   ├── json_validator.go # JSON Schema validation
│   └── validator.go      # Validator interface
├── examples/
│   ├── main.go           # Basic example
│   └── http_forwarder/
│       └── main.go       # HTTP forwarding example
├── go.mod
└── README.md
```

## Dependencies

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - Kafka client
- [goavro](https://github.com/linkedin/goavro) - Avro encoding/decoding
- [jsonschema](https://github.com/santhosh-tekuri/jsonschema) - JSON Schema validation

## Building and Running Examples

```bash
# From repo root, start Kafka + Schema Registry
cd docker
docker-compose up -d

# In a separate terminal, run the Go examples
cd ../go
go mod tidy

# Terminal A: start the consumer (blocks; Ctrl+C to stop)
go run ./examples/main.go consumer

# Terminal B: run the producer (registers schema + produces 5 messages)
go run ./examples/main.go producer

# Or run both in one process (starts consumer in a goroutine, then produces)
go run ./examples/main.go both
```

### HTTP forwarding example (optional)

This example forwards consumed message payloads via HTTP POST (e.g., to Elasticsearch).

```bash
# Terminal A: run the HTTP forwarder consumer
cd go
go run ./examples/http_forwarder/main.go

# Terminal B: produce messages (same as above)
go run ./examples/main.go producer
```

## Further Documentation

- **Architecture overview**: see `go/docs/ARCHITECTURE.md`
- **Code walkthrough and API details**: see `go/docs/CODE_WALKTHROUGH.md`

## License

MIT License

